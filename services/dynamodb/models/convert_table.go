package models

import (
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// --- Table Adapters ---

func ToSDKCreateTableInput(input *CreateTableInput) *dynamodb.CreateTableInput {
	// ProvisionedThroughput input.ProvisionedThroughput is 'any' in types.go, handled loosely
	// We'll approximate for now or assume map structure.

	var pt *types.ProvisionedThroughput
	if m, ok := input.ProvisionedThroughput.(map[string]any); ok {
		pt = &types.ProvisionedThroughput{
			ReadCapacityUnits:  ptrconv.Int64FromAny(m["ReadCapacityUnits"]),
			WriteCapacityUnits: ptrconv.Int64FromAny(m["WriteCapacityUnits"]),
		}
	}

	var ss *types.StreamSpecification
	if m, ok := input.StreamSpecification.(map[string]any); ok {
		var streamEnabled *bool
		if enabled, enabledOk := m["StreamEnabled"].(bool); enabledOk {
			streamEnabled = &enabled
		}

		var streamViewType types.StreamViewType
		if viewType, viewTypeOk := m["StreamViewType"].(string); viewTypeOk {
			streamViewType = types.StreamViewType(viewType)
		}

		ss = &types.StreamSpecification{
			StreamEnabled:  streamEnabled,
			StreamViewType: streamViewType,
		}
	}

	sdkTags := make([]types.Tag, 0, len(input.Tags))
	for _, t := range input.Tags {
		sdkTags = append(sdkTags, types.Tag{Key: aws.String(t.Key), Value: aws.String(t.Value)})
	}

	return &dynamodb.CreateTableInput{
		TableName:                 &input.TableName,
		KeySchema:                 ToSDKKeySchema(input.KeySchema),
		AttributeDefinitions:      ToSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes:    ToSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:     ToSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		ProvisionedThroughput:     pt,
		StreamSpecification:       ss,
		SSESpecification:          ToSDKSSESpecification(input.SSESpecification),
		OnDemandThroughput:        ToSDKOnDemandThroughput(input.OnDemandThroughput),
		DeletionProtectionEnabled: input.DeletionProtectionEnabled,
		BillingMode:               types.BillingMode(input.BillingMode),
		TableClass:                types.TableClass(input.TableClass),
		Tags:                      sdkTags,
	}
}

// ToSDKSSESpecification converts the wire-format SSESpecification to an AWS SDK type.
func ToSDKSSESpecification(input *SSESpecification) *types.SSESpecification {
	if input == nil {
		return nil
	}

	return &types.SSESpecification{
		Enabled:        input.Enabled,
		KMSMasterKeyId: ptrconv.NilIfEmpty(input.KMSMasterKeyID),
		SSEType:        types.SSEType(input.SSEType),
	}
}

// ToSDKOnDemandThroughput converts the wire-format OnDemandThroughput to an AWS SDK type.
func ToSDKOnDemandThroughput(input *OnDemandThroughput) *types.OnDemandThroughput {
	if input == nil {
		return nil
	}

	return &types.OnDemandThroughput{
		MaxReadRequestUnits:  input.MaxReadRequestUnits,
		MaxWriteRequestUnits: input.MaxWriteRequestUnits,
	}
}

func FromSDKCreateTableOutput(output *dynamodb.CreateTableOutput) *CreateTableOutput {
	return &CreateTableOutput{
		TableDescription: FromSDKTableDescription(output.TableDescription),
	}
}

func ToSDKDeleteTableInput(input *DeleteTableInput) *dynamodb.DeleteTableInput {
	return &dynamodb.DeleteTableInput{
		TableName: &input.TableName,
	}
}

func FromSDKDeleteTableOutput(output *dynamodb.DeleteTableOutput) *DeleteTableOutput {
	return &DeleteTableOutput{
		TableDescription: FromSDKTableDescription(output.TableDescription),
	}
}

func ToSDKDescribeTableInput(input *DescribeTableInput) *dynamodb.DescribeTableInput {
	return &dynamodb.DescribeTableInput{
		TableName: &input.TableName,
	}
}

func FromSDKDescribeTableOutput(output *dynamodb.DescribeTableOutput) *DescribeTableOutput {
	return &DescribeTableOutput{
		Table: FromSDKTableDescription(output.Table),
	}
}

func ToSDKListTablesInput(input *ListTablesInput) *dynamodb.ListTablesInput {
	const maxInt32Value = 2147483647
	var l *int32

	if input.Limit > 0 {
		if input.Limit > maxInt32Value {
			val := int32(maxInt32Value)
			l = &val
		} else {
			val := int32(input.Limit) // #nosec G115
			l = &val
		}
	}

	out := &dynamodb.ListTablesInput{Limit: l}
	if input.ExclusiveStartTableName != "" {
		out.ExclusiveStartTableName = &input.ExclusiveStartTableName
	}

	return out
}

func FromSDKListTablesOutput(output *dynamodb.ListTablesOutput) *ListTablesOutput {
	result := &ListTablesOutput{TableNames: output.TableNames}
	if output.LastEvaluatedTableName != nil {
		result.LastEvaluatedTableName = *output.LastEvaluatedTableName
	}

	return result
}

// ToSDKUpdateTableInput converts the wire-format UpdateTableInput to an AWS SDK input.
func ToSDKUpdateTableInput(input *UpdateTableInput) (*dynamodb.UpdateTableInput, error) {
	out := &dynamodb.UpdateTableInput{
		TableName: &input.TableName,
	}

	if len(input.AttributeDefinitions) > 0 {
		out.AttributeDefinitions = ToSDKAttributeDefinitions(input.AttributeDefinitions)
	}

	if input.ProvisionedThroughput != nil {
		out.ProvisionedThroughput = &types.ProvisionedThroughput{
			ReadCapacityUnits:  input.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: input.ProvisionedThroughput.WriteCapacityUnits,
		}
	}

	if input.StreamSpecification != nil {
		out.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  &input.StreamSpecification.StreamEnabled,
			StreamViewType: types.StreamViewType(input.StreamSpecification.StreamViewType),
		}
	}

	out.SSESpecification = ToSDKSSESpecification(input.SSESpecification)
	out.DeletionProtectionEnabled = input.DeletionProtectionEnabled
	out.TableClass = types.TableClass(input.TableClass)
	out.BillingMode = types.BillingMode(input.BillingMode)
	out.GlobalSecondaryIndexUpdates = toSDKGSIUpdates(input.GlobalSecondaryIndexUpdates)
	out.ReplicaUpdates = toSDKReplicationGroupUpdates(input.ReplicaUpdates)

	return out, nil
}

// toSDKGSIUpdates converts UpdateTable's GlobalSecondaryIndexUpdates list.
func toSDKGSIUpdates(updates []GlobalSecondaryIndexUpdate) []types.GlobalSecondaryIndexUpdate {
	out := make([]types.GlobalSecondaryIndexUpdate, 0, len(updates))

	for _, u := range updates {
		update := types.GlobalSecondaryIndexUpdate{}

		switch {
		case u.Create != nil:
			sdkCreate := &types.CreateGlobalSecondaryIndexAction{
				IndexName:  &u.Create.IndexName,
				KeySchema:  ToSDKKeySchema(u.Create.KeySchema),
				Projection: ToSDKProjection(u.Create.Projection),
			}

			if u.Create.ProvisionedThroughput != nil {
				sdkCreate.ProvisionedThroughput = &types.ProvisionedThroughput{
					ReadCapacityUnits:  u.Create.ProvisionedThroughput.ReadCapacityUnits,
					WriteCapacityUnits: u.Create.ProvisionedThroughput.WriteCapacityUnits,
				}
			}

			update.Create = sdkCreate

		case u.Update != nil:
			update.Update = &types.UpdateGlobalSecondaryIndexAction{
				IndexName: &u.Update.IndexName,
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  u.Update.ProvisionedThroughput.ReadCapacityUnits,
					WriteCapacityUnits: u.Update.ProvisionedThroughput.WriteCapacityUnits,
				},
			}

		case u.Delete != nil:
			update.Delete = &types.DeleteGlobalSecondaryIndexAction{
				IndexName: &u.Delete.IndexName,
			}
		}

		out = append(out, update)
	}

	return out
}

// toSDKReplicationGroupUpdates converts UpdateTable's ReplicaUpdates list (Global Tables v2).
func toSDKReplicationGroupUpdates(updates []ReplicaUpdate) []types.ReplicationGroupUpdate {
	out := make([]types.ReplicationGroupUpdate, 0, len(updates))

	for _, ru := range updates {
		sdkRU := types.ReplicationGroupUpdate{}
		if ru.Create != nil {
			sdkRU.Create = &types.CreateReplicationGroupMemberAction{
				RegionName: &ru.Create.RegionName,
			}
		}
		if ru.Update != nil {
			update := &types.UpdateReplicationGroupMemberAction{
				RegionName:         &ru.Update.RegionName,
				TableClassOverride: types.TableClass(ru.Update.TableClassOverride),
			}
			if ru.Update.ProvisionedReadCapacityUnits != nil {
				rcu := *ru.Update.ProvisionedReadCapacityUnits
				update.ProvisionedThroughputOverride = &types.ProvisionedThroughputOverride{
					ReadCapacityUnits: &rcu,
				}
			}
			sdkRU.Update = update
		}
		if ru.Delete != nil {
			sdkRU.Delete = &types.DeleteReplicationGroupMemberAction{
				RegionName: &ru.Delete.RegionName,
			}
		}
		out = append(out, sdkRU)
	}

	return out
}

// FromSDKUpdateTableOutput converts the AWS SDK UpdateTableOutput to wire format.
func FromSDKUpdateTableOutput(output *dynamodb.UpdateTableOutput) *UpdateTableOutput {
	return &UpdateTableOutput{
		TableDescription: FromSDKTableDescription(output.TableDescription),
	}
}

func ToSDKUpdateTimeToLiveInput(input *UpdateTimeToLiveInput) *dynamodb.UpdateTimeToLiveInput {
	return &dynamodb.UpdateTimeToLiveInput{
		TableName: &input.TableName,
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: &input.TimeToLiveSpecification.AttributeName,
			Enabled:       &input.TimeToLiveSpecification.Enabled,
		},
	}
}

func FromSDKUpdateTimeToLiveOutput(
	output *dynamodb.UpdateTimeToLiveOutput,
) *UpdateTimeToLiveOutput {
	return &UpdateTimeToLiveOutput{
		TimeToLiveSpecification: TimeToLiveSpecification{
			AttributeName: ptrconv.String(output.TimeToLiveSpecification.AttributeName),
			Enabled:       ptrconv.Bool(output.TimeToLiveSpecification.Enabled),
		},
	}
}

func ToSDKDescribeTimeToLiveInput(
	input *DescribeTimeToLiveInput,
) *dynamodb.DescribeTimeToLiveInput {
	return &dynamodb.DescribeTimeToLiveInput{
		TableName: &input.TableName,
	}
}

func FromSDKDescribeTimeToLiveOutput(
	output *dynamodb.DescribeTimeToLiveOutput,
) *DescribeTimeToLiveOutput {
	if output == nil {
		return &DescribeTimeToLiveOutput{}
	}
	status := ""
	if output.TimeToLiveDescription != nil {
		status = string(output.TimeToLiveDescription.TimeToLiveStatus)
	}
	attr := ""
	if output.TimeToLiveDescription != nil {
		attr = ptrconv.String(output.TimeToLiveDescription.AttributeName)
	}

	return &DescribeTimeToLiveOutput{
		TimeToLiveDescription: TimeToLiveDescription{
			AttributeName:    attr,
			TimeToLiveStatus: status,
		},
	}
}

// Helpers 2

func FromSDKTableDescription(td *types.TableDescription) TableDescription {
	if td == nil {
		return TableDescription{}
	}

	cnt := 0
	if td.ItemCount != nil {
		cnt = int(*td.ItemCount)
	}

	replicas := fromSDKReplicaDescriptions(td.Replicas)

	out := TableDescription{
		TableName:            ptrconv.String(td.TableName),
		TableStatus:          string(td.TableStatus),
		TableArn:             ptrconv.String(td.TableArn),
		TableID:              ptrconv.String(td.TableId),
		ItemCount:            cnt,
		KeySchema:            FromSDKKeySchema(td.KeySchema),
		AttributeDefinitions: FromSDKAttributeDefinitions(td.AttributeDefinitions),
		GlobalSecondaryIndexes: FromSDKGlobalSecondaryIndexDescriptions(
			td.GlobalSecondaryIndexes,
		),
		LocalSecondaryIndexes: FromSDKLocalSecondaryIndexDescriptions(td.LocalSecondaryIndexes),
		ProvisionedThroughput: FromSDKProvisionedThroughputDescription(
			td.ProvisionedThroughput,
		),
		Replicas:                  replicas,
		LatestStreamArn:           ptrconv.String(td.LatestStreamArn),
		LatestStreamLabel:         ptrconv.String(td.LatestStreamLabel),
		GlobalTableVersion:        ptrconv.String(td.GlobalTableVersion),
		DeletionProtectionEnabled: aws.ToBool(td.DeletionProtectionEnabled),
	}

	if td.BillingModeSummary != nil {
		out.BillingModeSummary = &BillingModeSummaryDescription{
			BillingMode: string(td.BillingModeSummary.BillingMode),
		}
	}

	if td.StreamSpecification != nil {
		out.StreamSpecification = &StreamSpecificationInput{
			StreamEnabled:  aws.ToBool(td.StreamSpecification.StreamEnabled),
			StreamViewType: string(td.StreamSpecification.StreamViewType),
		}
	}

	if td.SSEDescription != nil {
		out.SSEDescription = &SSEDescription{
			Status:          string(td.SSEDescription.Status),
			SSEType:         string(td.SSEDescription.SSEType),
			KMSMasterKeyArn: ptrconv.String(td.SSEDescription.KMSMasterKeyArn),
		}
	}

	if td.OnDemandThroughput != nil {
		out.OnDemandThroughput = &OnDemandThroughput{
			MaxReadRequestUnits:  td.OnDemandThroughput.MaxReadRequestUnits,
			MaxWriteRequestUnits: td.OnDemandThroughput.MaxWriteRequestUnits,
		}
	}

	if td.TableClassSummary != nil {
		out.TableClassSummary = &TableClassSummaryDescription{
			TableClass: string(td.TableClassSummary.TableClass),
		}
	}

	if td.TableSizeBytes != nil {
		out.TableSizeBytes = *td.TableSizeBytes
	}

	if td.CreationDateTime != nil {
		out.CreationDateTime = awstime.Epoch(*td.CreationDateTime)
	}

	return out
}

func fromSDKReplicaDescriptions(sdkReplicas []types.ReplicaDescription) []ReplicaDescription {
	if len(sdkReplicas) == 0 {
		return nil
	}

	out := make([]ReplicaDescription, len(sdkReplicas))
	for i, r := range sdkReplicas {
		rep := ReplicaDescription{
			RegionName:    ptrconv.String(r.RegionName),
			ReplicaStatus: string(r.ReplicaStatus),
		}
		if r.ReplicaTableClassSummary != nil && r.ReplicaTableClassSummary.TableClass != "" {
			rep.TableClassOverride = string(r.ReplicaTableClassSummary.TableClass)
		}
		if r.ProvisionedThroughputOverride != nil &&
			r.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
			rcu := *r.ProvisionedThroughputOverride.ReadCapacityUnits
			rep.ProvisionedReadCapacityUnits = &rcu
		}
		if len(r.GlobalSecondaryIndexes) > 0 {
			gsis := make([]ReplicaGSIOverride, 0, len(r.GlobalSecondaryIndexes))
			for _, g := range r.GlobalSecondaryIndexes {
				ov := ReplicaGSIOverride{IndexName: ptrconv.String(g.IndexName)}
				if g.ProvisionedThroughputOverride != nil &&
					g.ProvisionedThroughputOverride.ReadCapacityUnits != nil {
					rcu := *g.ProvisionedThroughputOverride.ReadCapacityUnits
					ov.ProvisionedReadCapacity = &rcu
				}
				gsis = append(gsis, ov)
			}
			rep.GlobalSecondaryIndexes = gsis
		}
		out[i] = rep
	}

	return out
}

func FromSDKGlobalSecondaryIndexDescriptions(
	gsis []types.GlobalSecondaryIndexDescription,
) []GlobalSecondaryIndexDescription {
	if len(gsis) == 0 {
		return nil
	}
	out := make([]GlobalSecondaryIndexDescription, len(gsis))
	for i, gsi := range gsis {
		out[i] = GlobalSecondaryIndexDescription{
			IndexName:   ptrconv.String(gsi.IndexName),
			IndexStatus: string(gsi.IndexStatus),
			KeySchema:   FromSDKKeySchema(gsi.KeySchema),
			Projection:  FromSDKProjection(gsi.Projection),
			ProvisionedThroughput: ProvisionedThroughputDescription{
				ReadCapacityUnits: int(ptrconv.Int64(gsi.ProvisionedThroughput.ReadCapacityUnits)),
				WriteCapacityUnits: int(
					ptrconv.Int64(gsi.ProvisionedThroughput.WriteCapacityUnits),
				),
			},
			ItemCount: int(ptrconv.Int64(gsi.ItemCount)),
		}
	}

	return out
}

func FromSDKLocalSecondaryIndexDescriptions(
	lsis []types.LocalSecondaryIndexDescription,
) []LocalSecondaryIndexDescription {
	if len(lsis) == 0 {
		return nil
	}
	out := make([]LocalSecondaryIndexDescription, len(lsis))
	for i, lsi := range lsis {
		out[i] = LocalSecondaryIndexDescription{
			IndexName:      ptrconv.String(lsi.IndexName),
			KeySchema:      FromSDKKeySchema(lsi.KeySchema),
			Projection:     FromSDKProjection(lsi.Projection),
			IndexSizeBytes: ptrconv.Int64(lsi.IndexSizeBytes),
			ItemCount:      int(ptrconv.Int64(lsi.ItemCount)),
		}
	}

	return out
}

func FromSDKProvisionedThroughputDescription(
	ptd *types.ProvisionedThroughputDescription,
) *ProvisionedThroughputDescription {
	if ptd == nil {
		return nil
	}

	return &ProvisionedThroughputDescription{
		ReadCapacityUnits:  int(ptrconv.Int64(ptd.ReadCapacityUnits)),
		WriteCapacityUnits: int(ptrconv.Int64(ptd.WriteCapacityUnits)),
	}
}

func FromSDKConsumedCapacity(cc *types.ConsumedCapacity) *ConsumedCapacity {
	if cc == nil {
		return nil
	}

	return &ConsumedCapacity{
		TableName:          ptrconv.String(cc.TableName),
		CapacityUnits:      ptrconv.Float64(cc.CapacityUnits),
		ReadCapacityUnits:  ptrconv.Float64(cc.ReadCapacityUnits),
		WriteCapacityUnits: ptrconv.Float64(cc.WriteCapacityUnits),
	}
}

func FromSDKItemCollectionMetrics(icm *types.ItemCollectionMetrics) *ItemCollectionMetrics {
	if icm == nil {
		return nil
	}

	return &ItemCollectionMetrics{
		ItemCollectionKey:   FromSDKItem(icm.ItemCollectionKey),
		SizeEstimateRangeGB: icm.SizeEstimateRangeGB,
	}
}

// ToSDKTagResourceInput converts the wire-format TagResourceInput to an AWS SDK input.
func ToSDKTagResourceInput(input *TagResourceInput) (*dynamodb.TagResourceInput, error) {
	sdkTags := make([]types.Tag, len(input.Tags))
	for i, t := range input.Tags {
		tag := t // capture loop var
		sdkTags[i] = types.Tag{Key: &tag.Key, Value: &tag.Value}
	}

	return &dynamodb.TagResourceInput{ResourceArn: &input.ResourceArn, Tags: sdkTags}, nil
}

// FromSDKTagResourceOutput converts the AWS SDK TagResourceOutput to wire format.
func FromSDKTagResourceOutput(_ *dynamodb.TagResourceOutput) *TagResourceOutput {
	return &TagResourceOutput{}
}

// ToSDKUntagResourceInput converts the wire-format UntagResourceInput to an AWS SDK input.
func ToSDKUntagResourceInput(input *UntagResourceInput) (*dynamodb.UntagResourceInput, error) {
	return &dynamodb.UntagResourceInput{
		ResourceArn: &input.ResourceArn,
		TagKeys:     input.TagKeys,
	}, nil
}

// FromSDKUntagResourceOutput converts the AWS SDK UntagResourceOutput to wire format.
func FromSDKUntagResourceOutput(_ *dynamodb.UntagResourceOutput) *UntagResourceOutput {
	return &UntagResourceOutput{}
}

// ToSDKListTagsOfResourceInput converts the wire-format input to an AWS SDK input.
func ToSDKListTagsOfResourceInput(
	input *ListTagsOfResourceInput,
) (*dynamodb.ListTagsOfResourceInput, error) {
	out := &dynamodb.ListTagsOfResourceInput{ResourceArn: &input.ResourceArn}
	if input.NextToken != "" {
		out.NextToken = &input.NextToken
	}

	return out, nil
}

// FromSDKListTagsOfResourceOutput converts the AWS SDK output to wire format.
func FromSDKListTagsOfResourceOutput(
	output *dynamodb.ListTagsOfResourceOutput,
) *ListTagsOfResourceOutput {
	tags := make([]Tag, len(output.Tags))
	for i, t := range output.Tags {
		tags[i] = Tag{
			Key:   ptrconv.String(t.Key),
			Value: ptrconv.String(t.Value),
		}
	}

	result := &ListTagsOfResourceOutput{Tags: tags}
	if output.NextToken != nil {
		result.NextToken = *output.NextToken
	}

	return result
}
