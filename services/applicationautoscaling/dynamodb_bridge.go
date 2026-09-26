package applicationautoscaling

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// dynamoDBOnDemandMessage is AAS's own on-demand rejection text, from a real-account
// transcript: github.com/hashicorp/terraform-provider-aws/issues/22784.
const dynamoDBOnDemandMessage = "Validation failed for scalable target. Reason: " +
	"PAY_PER_REQUEST table mode is not scalable."

// dynamoDBServiceNamespace routes a scalable target/scaling policy through this bridge.
const dynamoDBServiceNamespace = "dynamodb"

// The four ScalableDimension values AWS models for DynamoDB.
const (
	dimTableRead  = "dynamodb:table:ReadCapacityUnits"
	dimTableWrite = "dynamodb:table:WriteCapacityUnits"
	dimIndexRead  = "dynamodb:index:ReadCapacityUnits"
	dimIndexWrite = "dynamodb:index:WriteCapacityUnits"
)

// ResourceId path segments: "table/<name>" or "table/<name>/index/<indexName>".
const (
	resourceIDTableSegment = "table"
	resourceIDIndexSegment = "index"
)

// dynamoDBTarget is the table/index/direction a ResourceId+ScalableDimension
// pair addresses -- the same selector dynamodb's own autoscaling.go uses internally.
type dynamoDBTarget struct {
	tableName string
	indexName string // empty for a table-level dimension
	read      bool   // true for *ReadCapacityUnits, false for *WriteCapacityUnits
}

// matched=false means scalableDimension isn't a DynamoDB dimension; callers
// skip the bridge rather than reject the request.
func parseDynamoDBTarget(resourceID, scalableDimension string) (dynamoDBTarget, bool, error) {
	wantIndex, read, matched := classifyDynamoDBDimension(scalableDimension)
	if !matched {
		return dynamoDBTarget{}, false, nil
	}

	tableName, indexName, ok := splitDynamoDBResourceID(resourceID)
	if !ok || (indexName != "") != wantIndex {
		return dynamoDBTarget{}, true, invalidDynamoDBResourceIDError(scalableDimension, wantIndex)
	}

	return dynamoDBTarget{tableName: tableName, indexName: indexName, read: read}, true, nil
}

// ok=false means id matches neither ResourceId shape.
func splitDynamoDBResourceID(id string) (string, string, bool) {
	const tablePartCount = 2

	const indexPartCount = 4

	parts := strings.Split(id, "/")

	switch len(parts) {
	case tablePartCount:
		if parts[0] == resourceIDTableSegment && parts[1] != "" {
			return parts[1], "", true
		}
	case indexPartCount:
		if parts[0] == resourceIDTableSegment && parts[1] != "" &&
			parts[2] == resourceIDIndexSegment && parts[3] != "" {
			return parts[1], parts[3], true
		}
	}

	return "", "", false
}

func invalidDynamoDBResourceIDError(scalableDimension string, wantIndex bool) error {
	if wantIndex {
		return fmt.Errorf(
			"%w: ResourceId must be table/<table-name>/index/<index-name> for ScalableDimension %s",
			ErrValidation, scalableDimension,
		)
	}

	return fmt.Errorf(
		"%w: ResourceId must be table/<table-name> for ScalableDimension %s",
		ErrValidation, scalableDimension,
	)
}

// Returns (wantIndex, read, matched).
func classifyDynamoDBDimension(dimension string) (bool, bool, bool) {
	switch dimension {
	case dimTableRead:
		return false, true, true
	case dimTableWrite:
		return false, false, true
	case dimIndexRead:
		return true, true, true
	case dimIndexWrite:
		return true, false, true
	default:
		return false, false, false
	}
}

// dynamoDBResourceID is the inverse of parseDynamoDBTarget.
func dynamoDBResourceID(target dynamoDBTarget) string {
	if target.indexName == "" {
		return "table/" + target.tableName
	}

	return "table/" + target.tableName + "/index/" + target.indexName
}

// dynamoDBDimension rebuilds the canonical ScalableDimension for target.
func dynamoDBDimension(target dynamoDBTarget) string {
	switch {
	case target.indexName == "" && target.read:
		return dimTableRead
	case target.indexName == "" && !target.read:
		return dimTableWrite
	case target.indexName != "" && target.read:
		return dimIndexRead
	default:
		return dimIndexWrite
	}
}

// dynamoDBRequestContext carries this backend's own account/region so the
// sibling call resolves the same table/replica a same-region request would.
func (b *InMemoryBackend) dynamoDBRequestContext() context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Account: b.accountID, Region: b.region})
}

// registerDynamoDBScalableTarget validates the table exists, then pushes
// MinCapacity/MaxCapacity/RoleARN into DynamoDB's own autoscaling state.
func (b *InMemoryBackend) registerDynamoDBScalableTarget(
	resourceID, scalableDimension string, minCapacity, maxCapacity *int32, roleARN string,
) error {
	target, matched, parseErr := parseDynamoDBTarget(resourceID, scalableDimension)
	if parseErr != nil {
		return parseErr
	}

	if !matched {
		return nil
	}

	if err := b.validateDynamoDBTargetExists(target, resourceID); err != nil {
		return err
	}

	if err := b.pushDynamoDBCapacity(target, minCapacity, maxCapacity, roleARN); err != nil {
		return fmt.Errorf("%w: %s", ErrValidation, err.Error())
	}

	return nil
}

// Existence verified against a real account: terraform-aws-modules/terraform-aws-dynamodb-table#15.
func (b *InMemoryBackend) validateDynamoDBTargetExists(target dynamoDBTarget, resourceID string) error {
	ddb, ok := b.dynamoDBBackend()
	if !ok {
		return nil
	}

	onDemand, exists := b.dynamoDBTableIsOnDemand(ddb, target.tableName)
	if !exists {
		return fmt.Errorf("%w: DynamoDB table does not exist: %s", ErrValidation, resourceID)
	}

	if onDemand {
		return fmt.Errorf("%w: %s", ErrValidation, dynamoDBOnDemandMessage)
	}

	return nil
}

// dynamoDBTableIsOnDemand reports PAY_PER_REQUEST billing; ok is false if the
// table can't be described.
func (b *InMemoryBackend) dynamoDBTableIsOnDemand(ddb ddbbackend.StorageBackend, tableName string) (bool, bool) {
	out, err := ddb.DescribeTable(b.dynamoDBRequestContext(), &sdkddb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return false, false
	}

	return isPayPerRequestTable(out), true
}

// isPayPerRequestTable reports on-demand billing; nil BillingModeSummary means PROVISIONED.
func isPayPerRequestTable(out *sdkddb.DescribeTableOutput) bool {
	return out != nil && out.Table != nil && out.Table.BillingModeSummary != nil &&
		out.Table.BillingModeSummary.BillingMode == ddbtypes.BillingModePayPerRequest
}

// ok=false means the sibling isn't wired or the table/replica/index can't be resolved.
func dynamoDBAutoScalingSettings(
	ctx context.Context, ddb ddbbackend.StorageBackend, target dynamoDBTarget, region string,
) (*ddbtypes.AutoScalingSettingsDescription, bool) {
	out, err := ddb.DescribeTableReplicaAutoScaling(ctx, &sdkddb.DescribeTableReplicaAutoScalingInput{
		TableName: aws.String(target.tableName),
	})
	if err != nil || out.TableAutoScalingDescription == nil {
		return nil, false
	}

	for _, r := range out.TableAutoScalingDescription.Replicas {
		if aws.ToString(r.RegionName) != region {
			continue
		}

		return replicaAutoScalingSettingsForTarget(r, target), true
	}

	return nil, false
}

func replicaAutoScalingSettingsForTarget(
	r ddbtypes.ReplicaAutoScalingDescription, target dynamoDBTarget,
) *ddbtypes.AutoScalingSettingsDescription {
	if target.indexName == "" {
		if target.read {
			return r.ReplicaProvisionedReadCapacityAutoScalingSettings
		}

		return r.ReplicaProvisionedWriteCapacityAutoScalingSettings
	}

	for _, g := range r.GlobalSecondaryIndexes {
		if aws.ToString(g.IndexName) != target.indexName {
			continue
		}

		if target.read {
			return g.ProvisionedReadCapacityAutoScalingSettings
		}

		return g.ProvisionedWriteCapacityAutoScalingSettings
	}

	return nil
}

// dynamoDBAutoScalingUpdateInput wraps upd into the input shape matching
// target: table-level write, per-GSI write, per-replica read, or per-replica-per-GSI read.
func dynamoDBAutoScalingUpdateInput(
	target dynamoDBTarget, region string, upd *ddbtypes.AutoScalingSettingsUpdate,
) *sdkddb.UpdateTableReplicaAutoScalingInput {
	input := &sdkddb.UpdateTableReplicaAutoScalingInput{TableName: aws.String(target.tableName)}

	switch {
	case target.indexName == "" && !target.read:
		input.ProvisionedWriteCapacityAutoScalingUpdate = upd
	case target.indexName != "" && !target.read:
		input.GlobalSecondaryIndexUpdates = []ddbtypes.GlobalSecondaryIndexAutoScalingUpdate{
			{IndexName: aws.String(target.indexName), ProvisionedWriteCapacityAutoScalingUpdate: upd},
		}
	case target.indexName == "" && target.read:
		input.ReplicaUpdates = []ddbtypes.ReplicaAutoScalingUpdate{
			{RegionName: aws.String(region), ReplicaProvisionedReadCapacityAutoScalingUpdate: upd},
		}
	default: // index + read
		input.ReplicaUpdates = []ddbtypes.ReplicaAutoScalingUpdate{
			{
				RegionName: aws.String(region),
				ReplicaGlobalSecondaryIndexUpdates: []ddbtypes.ReplicaGlobalSecondaryIndexAutoScalingUpdate{
					{IndexName: aws.String(target.indexName), ProvisionedReadCapacityAutoScalingUpdate: upd},
				},
			},
		}
	}

	return input
}

// nil fields mean "leave unchanged"; an AutoScalingSettingsUpdate otherwise
// replaces the whole stored throughput, wiping omitted fields.
func carryForwardAutoScalingSettingsUpdate(
	existing *ddbtypes.AutoScalingSettingsDescription,
	minCapacity, maxCapacity *int64,
	roleARN *string,
) *ddbtypes.AutoScalingSettingsUpdate {
	upd := &ddbtypes.AutoScalingSettingsUpdate{
		MinimumUnits: minCapacity,
		MaximumUnits: maxCapacity,
	}

	if existing != nil {
		if minCapacity == nil {
			upd.MinimumUnits = existing.MinimumUnits
		}

		if maxCapacity == nil {
			upd.MaximumUnits = existing.MaximumUnits
		}

		upd.AutoScalingRoleArn = existing.AutoScalingRoleArn
	}

	if roleARN != nil && *roleARN != "" {
		upd.AutoScalingRoleArn = roleARN
	}

	if existing != nil {
		upd.ScalingPolicyUpdate = carryForwardScalingPolicyUpdate(existing)
	}

	return upd
}

func carryForwardScalingPolicyUpdate(
	existing *ddbtypes.AutoScalingSettingsDescription,
) *ddbtypes.AutoScalingPolicyUpdate {
	if len(existing.ScalingPolicies) == 0 {
		return nil
	}

	p := existing.ScalingPolicies[0]
	if p.TargetTrackingScalingPolicyConfiguration == nil {
		return &ddbtypes.AutoScalingPolicyUpdate{PolicyName: p.PolicyName}
	}

	t := p.TargetTrackingScalingPolicyConfiguration

	return &ddbtypes.AutoScalingPolicyUpdate{
		PolicyName: p.PolicyName,
		TargetTrackingScalingPolicyConfiguration: &ddbtypes.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
			TargetValue:      t.TargetValue,
			DisableScaleIn:   t.DisableScaleIn,
			ScaleInCooldown:  t.ScaleInCooldown,
			ScaleOutCooldown: t.ScaleOutCooldown,
		},
	}
}

func (b *InMemoryBackend) pushDynamoDBCapacity(
	target dynamoDBTarget, minCapacity, maxCapacity *int32, roleARN string,
) error {
	ddb, ok := b.dynamoDBBackend()
	if !ok {
		return nil
	}

	ctx := b.dynamoDBRequestContext()

	existing, _ := dynamoDBAutoScalingSettings(ctx, ddb, target, b.region)

	var roleARNPtr *string
	if roleARN != "" {
		roleARNPtr = &roleARN
	}

	upd := carryForwardAutoScalingSettingsUpdate(existing, toInt64Ptr(minCapacity), toInt64Ptr(maxCapacity), roleARNPtr)

	_, err := ddb.UpdateTableReplicaAutoScaling(ctx, dynamoDBAutoScalingUpdateInput(target, b.region, upd))

	return err
}

// DynamoDB models a single ScalingPolicyUpdate per dimension; StepScaling/
// PredictiveScaling have no DynamoDB-side representation (see PARITY.md).
func (b *InMemoryBackend) pushDynamoDBTargetTrackingPolicy(
	target dynamoDBTarget, policyName string, cfg map[string]any,
) error {
	ddb, ok := b.dynamoDBBackend()
	if !ok {
		return nil
	}

	// Billing mode can change after registration; re-check so DynamoDB's error never leaks.
	if onDemand, exists := b.dynamoDBTableIsOnDemand(ddb, target.tableName); exists && onDemand {
		return fmt.Errorf("%w: %s", ErrValidation, dynamoDBOnDemandMessage)
	}

	ctx := b.dynamoDBRequestContext()

	existing, _ := dynamoDBAutoScalingSettings(ctx, ddb, target, b.region)

	upd := carryForwardAutoScalingSettingsUpdate(existing, nil, nil, nil)
	upd.ScalingPolicyUpdate = &ddbtypes.AutoScalingPolicyUpdate{
		PolicyName: aws.String(policyName),
		TargetTrackingScalingPolicyConfiguration: &ddbtypes.AutoScalingTargetTrackingScalingPolicyConfigurationUpdate{
			TargetValue:      targetTrackingFloat(cfg, "TargetValue"),
			DisableScaleIn:   targetTrackingBool(cfg, "DisableScaleIn"),
			ScaleInCooldown:  targetTrackingInt32(cfg, "ScaleInCooldown"),
			ScaleOutCooldown: targetTrackingInt32(cfg, "ScaleOutCooldown"),
		},
	}

	_, err := ddb.UpdateTableReplicaAutoScaling(ctx, dynamoDBAutoScalingUpdateInput(target, b.region, upd))

	return err
}

// Only TargetTrackingScaling policies push into DynamoDB; see pushDynamoDBTargetTrackingPolicy.
func (b *InMemoryBackend) pushDynamoDBPolicyIfApplicable(
	serviceNamespace, resourceID, scalableDimension, policyType, policyName string,
	targetTrackingConfig map[string]any,
) error {
	if serviceNamespace != dynamoDBServiceNamespace || policyType != policyTypeTargetTrackingScaling {
		return nil
	}

	if targetTrackingConfig == nil {
		return nil
	}

	target, matched, parseErr := parseDynamoDBTarget(resourceID, scalableDimension)
	if parseErr != nil {
		return parseErr
	}

	if !matched {
		return nil
	}

	if err := b.pushDynamoDBTargetTrackingPolicy(target, policyName, targetTrackingConfig); err != nil {
		if errors.Is(err, ErrValidation) {
			return err
		}

		return fmt.Errorf("%w: %s", ErrValidation, err.Error())
	}

	return nil
}

func toInt64Ptr(v *int32) *int64 {
	if v == nil {
		return nil
	}

	out := int64(*v)

	return &out
}

// targetTrackingFloat/Bool/Int32 read a field from the passthrough config
// map; values may be float64 (decoded JSON) or a Go literal (tests).
func targetTrackingFloat(cfg map[string]any, key string) *float64 {
	switch v := cfg[key].(type) {
	case float64:
		return &v
	case float32:
		f := float64(v)

		return &f
	case int:
		f := float64(v)

		return &f
	default:
		return nil
	}
}

func targetTrackingBool(cfg map[string]any, key string) *bool {
	v, ok := cfg[key].(bool)
	if !ok {
		return nil
	}

	return &v
}

func targetTrackingInt32(cfg map[string]any, key string) *int32 {
	switch v := cfg[key].(type) {
	case float64:
		i := int32(v)

		return &i
	case int:
		i := int32(v) //nolint:gosec // G115: cooldown seconds, never near int32 range

		return &i
	case int32:
		return &v
	default:
		return nil
	}
}
