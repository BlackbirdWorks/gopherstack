package applicationautoscaling

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// dimensionsPerResource is the read+write dimension pair every DynamoDB
// resourceId (table or index) is probed for.
const dimensionsPerResource = 2

func capacityToInt32(v int64) int32 {
	return int32(v) //nolint:gosec // G115: DynamoDB capacity units, never near int32 range
}

// Synthesizes rows for settings configured directly via DynamoDB (known avoids
// duplicates); an empty f.ResourceIDs only probes table-level dimensions (PARITY.md).
func (b *InMemoryBackend) dynamodbSiblingScalableTargets(
	f DescribeScalableTargetsFilter, known map[string]bool,
) []*ScalableTarget {
	if f.ServiceNamespace != dynamoDBServiceNamespace {
		return nil
	}

	ddb, wired := b.dynamoDBBackend()
	if !wired {
		return nil
	}

	ctx := b.dynamoDBRequestContext()

	out := make([]*ScalableTarget, 0)

	for _, target := range b.candidateDynamoDBTargets(ctx, ddb, f.ResourceIDs) {
		dimension := dynamoDBDimension(target)
		if f.ScalableDimension != "" && dimension != f.ScalableDimension {
			continue
		}

		resourceID := dynamoDBResourceID(target)
		if known[scalableTargetKey(dynamoDBServiceNamespace, resourceID, dimension)] {
			continue
		}

		desc, ok := dynamoDBAutoScalingSettings(ctx, ddb, target, b.region)
		if !ok || desc == nil || desc.MinimumUnits == nil || desc.MaximumUnits == nil {
			continue
		}

		out = append(out, b.synthesizeScalableTarget(target, desc))
	}

	return out
}

// dynamodbSiblingScalingPolicies is the DescribeScalingPolicies analog of
// dynamodbSiblingScalableTargets; same scope/disclosure.
func (b *InMemoryBackend) dynamodbSiblingScalingPolicies(
	f DescribeScalingPoliciesFilter, known map[string]bool,
) []*ScalingPolicy {
	if f.ServiceNamespace != dynamoDBServiceNamespace {
		return nil
	}

	ddb, wired := b.dynamoDBBackend()
	if !wired {
		return nil
	}

	ctx := b.dynamoDBRequestContext()

	var resourceIDs []string
	if f.ResourceID != "" {
		resourceIDs = []string{f.ResourceID}
	}

	out := make([]*ScalingPolicy, 0)

	for _, target := range b.candidateDynamoDBTargets(ctx, ddb, resourceIDs) {
		dimension := dynamoDBDimension(target)
		if f.ScalableDimension != "" && dimension != f.ScalableDimension {
			continue
		}

		resourceID := dynamoDBResourceID(target)

		desc, ok := dynamoDBAutoScalingSettings(ctx, ddb, target, b.region)
		if !ok || desc == nil || len(desc.ScalingPolicies) == 0 {
			continue
		}

		p := desc.ScalingPolicies[0]
		policyName := aws.ToString(p.PolicyName)

		if known[policyNameKey(dynamoDBServiceNamespace, resourceID, dimension, policyName)] {
			continue
		}

		out = append(out, b.synthesizeScalingPolicy(target, policyName, p))
	}

	return out
}

// Parses resourceIDs directly when given, else derives from the account's first ListTables page.
func (b *InMemoryBackend) candidateDynamoDBTargets(
	ctx context.Context, ddb ddbbackend.StorageBackend, resourceIDs []string,
) []dynamoDBTarget {
	if len(resourceIDs) > 0 {
		return candidateTargetsForResourceIDs(resourceIDs)
	}

	out, err := ddb.ListTables(ctx, &sdkddb.ListTablesInput{})
	if err != nil || out == nil {
		return nil
	}

	targets := make([]dynamoDBTarget, 0, len(out.TableNames)*dimensionsPerResource)
	for _, name := range out.TableNames {
		targets = append(targets,
			dynamoDBTarget{tableName: name, read: true},
			dynamoDBTarget{tableName: name, read: false},
		)
	}

	return targets
}

// A ResourceId matching neither shape is skipped, not rejected.
func candidateTargetsForResourceIDs(resourceIDs []string) []dynamoDBTarget {
	targets := make([]dynamoDBTarget, 0, len(resourceIDs)*dimensionsPerResource)

	for _, id := range resourceIDs {
		tableName, indexName, ok := splitDynamoDBResourceID(id)
		if !ok {
			continue
		}

		targets = append(targets,
			dynamoDBTarget{tableName: tableName, indexName: indexName, read: true},
			dynamoDBTarget{tableName: tableName, indexName: indexName, read: false},
		)
	}

	return targets
}

// Stable (not random): there's no registration event here to mint a UUID
// from, and repeated Describe calls must return the same ARN.
func syntheticTargetARNSuffix(resourceID, dimension string) string {
	r := strings.NewReplacer("/", "-", ":", "-")

	return r.Replace(resourceID) + "-" + r.Replace(dimension)
}

func (b *InMemoryBackend) synthesizeScalableTarget(
	target dynamoDBTarget, desc *ddbtypes.AutoScalingSettingsDescription,
) *ScalableTarget {
	resourceID := dynamoDBResourceID(target)
	dimension := dynamoDBDimension(target)

	return &ScalableTarget{
		ServiceNamespace:  dynamoDBServiceNamespace,
		ResourceID:        resourceID,
		ScalableDimension: dimension,
		MinCapacity:       capacityToInt32(aws.ToInt64(desc.MinimumUnits)),
		MaxCapacity:       capacityToInt32(aws.ToInt64(desc.MaximumUnits)),
		RoleARN:           aws.ToString(desc.AutoScalingRoleArn),
		AccountID:         b.accountID,
		Region:            b.region,
		Tags:              map[string]string{},
		ARN: arn.Build(
			"application-autoscaling", b.region, b.accountID,
			"scalable-target/"+syntheticTargetARNSuffix(resourceID, dimension),
		),
	}
}

// DynamoDB's own API has no metric-type field; the metric is implied by the dimension.
func dynamoDBPredefinedMetricType(target dynamoDBTarget) string {
	if target.read {
		return "DynamoDBReadCapacityUtilization"
	}

	return "DynamoDBWriteCapacityUtilization"
}

func (b *InMemoryBackend) synthesizeScalingPolicy(
	target dynamoDBTarget, policyName string, p ddbtypes.AutoScalingPolicyDescription,
) *ScalingPolicy {
	resourceID := dynamoDBResourceID(target)
	dimension := dynamoDBDimension(target)

	cfg := map[string]any{
		"PredefinedMetricSpecification": map[string]any{
			"PredefinedMetricType": dynamoDBPredefinedMetricType(target),
		},
	}

	if t := p.TargetTrackingScalingPolicyConfiguration; t != nil {
		cfg["TargetValue"] = aws.ToFloat64(t.TargetValue)
		cfg["DisableScaleIn"] = aws.ToBool(t.DisableScaleIn)
		cfg["ScaleInCooldown"] = aws.ToInt32(t.ScaleInCooldown)
		cfg["ScaleOutCooldown"] = aws.ToInt32(t.ScaleOutCooldown)
	}

	return &ScalingPolicy{
		ServiceNamespace:     dynamoDBServiceNamespace,
		ResourceID:           resourceID,
		ScalableDimension:    dimension,
		PolicyName:           policyName,
		PolicyType:           policyTypeTargetTrackingScaling,
		TargetTrackingConfig: cfg,
		ARN: arn.Build(
			"autoscaling", b.region, b.accountID,
			"scalingPolicy:"+syntheticTargetARNSuffix(resourceID, dimension)+":policyName/"+policyName,
		),
	}
}
