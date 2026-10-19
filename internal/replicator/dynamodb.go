package replicator

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	ddbBatchSize     = 25
	ddbWaitMax       = 30 * time.Second
	ddbUnprocessedMx = 10
)

func (r *run) replicateDynamoDB(ctx context.Context) {
	pager := dynamodb.NewListTablesPaginator(r.src.ddb, &dynamodb.ListTablesInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeDynamoDB, err)

			return
		}

		for _, name := range page.TableNames {
			if r.match(name) {
				r.copyTable(ctx, name)
			}
		}
	}
}

func (r *run) copyTable(ctx context.Context, name string) {
	desc, err := r.src.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name})
	if err != nil {
		r.fail(TypeDynamoDB, name, err)

		return
	}

	_, err = r.dst.ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeDynamoDB, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeDynamoDB, name, upsert(exists), "")

		return
	}

	action, reason, err := r.applyTable(ctx, desc.Table, exists)
	if err != nil {
		r.fail(TypeDynamoDB, name, err)

		return
	}

	r.add(TypeDynamoDB, name, action, reason)
}

func (r *run) applyTable(ctx context.Context, tbl *ddbtypes.TableDescription, exists bool) (string, string, error) {
	action, reason := ActionCreated, ""

	if exists {
		action, reason = ActionSkipped, "table exists"
	} else if err := r.createTable(ctx, tbl); err != nil {
		return "", "", err
	}

	changed, err := r.syncTableMeta(ctx, tbl)
	if err != nil {
		return "", "", err
	}

	if r.cfg.IncludeData {
		n, cerr := r.copyItems(ctx, aws.ToString(tbl.TableName))
		if cerr != nil {
			return "", "", cerr
		}

		changed, reason = true, fmt.Sprintf("%d items copied", n)
	}

	if changed && action == ActionSkipped {
		action = ActionUpdated
		if !r.cfg.IncludeData {
			reason = ""
		}
	}

	return action, reason, nil
}

func (r *run) createTable(ctx context.Context, t *ddbtypes.TableDescription) error {
	in := r.tableInput(t)

	tags, err := r.src.ddb.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: t.TableArn})
	if err != nil {
		return fmt.Errorf("list tags: %w", err)
	}

	in.Tags = tags.Tags

	if _, err = r.dst.ddb.CreateTable(ctx, in); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	waiter := dynamodb.NewTableExistsWaiter(r.dst.ddb)
	if err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: t.TableName}, ddbWaitMax); err != nil {
		return fmt.Errorf("wait active: %w", err)
	}

	return nil
}

func (r *run) tableInput(t *ddbtypes.TableDescription) *dynamodb.CreateTableInput {
	in := &dynamodb.CreateTableInput{
		TableName:                 t.TableName,
		AttributeDefinitions:      t.AttributeDefinitions,
		KeySchema:                 t.KeySchema,
		BillingMode:               ddbtypes.BillingModePayPerRequest,
		DeletionProtectionEnabled: t.DeletionProtectionEnabled,
		SSESpecification:          r.sseSpec(t.SSEDescription),
	}

	if s := t.BillingModeSummary; s != nil {
		in.BillingMode = s.BillingMode
	} else if t.ProvisionedThroughput != nil && aws.ToInt64(t.ProvisionedThroughput.ReadCapacityUnits) > 0 {
		in.BillingMode = ddbtypes.BillingModeProvisioned
	}

	provisioned := in.BillingMode == ddbtypes.BillingModeProvisioned
	if provisioned {
		in.ProvisionedThroughput = throughput(t.ProvisionedThroughput)
	}

	for _, g := range t.GlobalSecondaryIndexes {
		gi := ddbtypes.GlobalSecondaryIndex{IndexName: g.IndexName, KeySchema: g.KeySchema, Projection: g.Projection}
		if provisioned {
			gi.ProvisionedThroughput = throughput(g.ProvisionedThroughput)
		}

		in.GlobalSecondaryIndexes = append(in.GlobalSecondaryIndexes, gi)
	}

	for _, l := range t.LocalSecondaryIndexes {
		in.LocalSecondaryIndexes = append(in.LocalSecondaryIndexes, ddbtypes.LocalSecondaryIndex{
			IndexName: l.IndexName, KeySchema: l.KeySchema, Projection: l.Projection,
		})
	}

	if t.StreamSpecification != nil && aws.ToBool(t.StreamSpecification.StreamEnabled) {
		in.StreamSpecification = t.StreamSpecification
	}

	if t.TableClassSummary != nil {
		in.TableClass = t.TableClassSummary.TableClass
	}

	return in
}

func (r *run) sseSpec(d *ddbtypes.SSEDescription) *ddbtypes.SSESpecification {
	if d == nil || d.Status != ddbtypes.SSEStatusEnabled {
		return nil
	}

	spec := &ddbtypes.SSESpecification{Enabled: aws.Bool(true), SSEType: d.SSEType}
	if k := aws.ToString(d.KMSMasterKeyArn); k != "" {
		spec.KMSMasterKeyId = aws.String(r.rm.ARN(k))
	}

	return spec
}

func throughput(p *ddbtypes.ProvisionedThroughputDescription) *ddbtypes.ProvisionedThroughput {
	if p == nil {
		return &ddbtypes.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1)}
	}

	return &ddbtypes.ProvisionedThroughput{
		ReadCapacityUnits: p.ReadCapacityUnits, WriteCapacityUnits: p.WriteCapacityUnits,
	}
}

// syncTableMeta enables TTL on the target to match the source and reports whether it changed.
func (r *run) syncTableMeta(ctx context.Context, t *ddbtypes.TableDescription) (bool, error) {
	srcTTL, err := r.src.ddb.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: t.TableName})
	if err != nil {
		return false, fmt.Errorf("describe ttl: %w", err)
	}

	d := srcTTL.TimeToLiveDescription
	if d == nil || d.TimeToLiveStatus != ddbtypes.TimeToLiveStatusEnabled {
		return false, nil
	}

	dstTTL, err := r.dst.ddb.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: t.TableName})
	if err == nil && dstTTL.TimeToLiveDescription != nil &&
		dstTTL.TimeToLiveDescription.TimeToLiveStatus == ddbtypes.TimeToLiveStatusEnabled {
		return false, nil
	}

	_, err = r.dst.ddb.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: t.TableName,
		TimeToLiveSpecification: &ddbtypes.TimeToLiveSpecification{
			Enabled: aws.Bool(true), AttributeName: d.AttributeName,
		},
	})
	if err != nil {
		return false, fmt.Errorf("update ttl: %w", err)
	}

	return true, nil
}

func (r *run) copyItems(ctx context.Context, table string) (int, error) {
	pager := dynamodb.NewScanPaginator(r.src.ddb, &dynamodb.ScanInput{TableName: &table})
	total := 0

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return total, fmt.Errorf("scan: %w", err)
		}

		for start := 0; start < len(page.Items); start += ddbBatchSize {
			end := min(start+ddbBatchSize, len(page.Items))

			reqs := make([]ddbtypes.WriteRequest, 0, end-start)
			for _, it := range page.Items[start:end] {
				reqs = append(reqs, ddbtypes.WriteRequest{PutRequest: &ddbtypes.PutRequest{Item: it}})
			}

			if err = r.writeBatch(ctx, table, reqs); err != nil {
				return total, err
			}

			total += len(reqs)
		}
	}

	return total, nil
}

func (r *run) writeBatch(ctx context.Context, table string, reqs []ddbtypes.WriteRequest) error {
	pending := map[string][]ddbtypes.WriteRequest{table: reqs}

	for range ddbUnprocessedMx {
		out, err := r.dst.ddb.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
		if err != nil {
			return fmt.Errorf("batch write: %w", err)
		}

		if len(out.UnprocessedItems) == 0 {
			return nil
		}

		pending = out.UnprocessedItems
	}

	return fmt.Errorf("batch write: %w", errUnprocessed)
}
