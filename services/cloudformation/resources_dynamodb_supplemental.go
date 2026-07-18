package cloudformation

import (
	"context"
	"fmt"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (rc *ResourceCreator) createDynamoDBSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::DynamoDB::GlobalTable":
		id, err := rc.createDynamoDBGlobalTable(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) createDynamoDBGlobalTable(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DynamoDB == nil {
		return logicalID + "-stub", nil
	}

	tableName := strProp(props, "TableName", params, physicalIDs)
	if tableName == "" {
		tableName = logicalID
	}

	// Build replication group from Replicas prop.
	var replicas []ddbtypes.Replica
	if replicaList, hasReplicas := props["Replicas"].([]any); hasReplicas {
		for _, r := range replicaList {
			if rm, isMap := r.(map[string]any); isMap {
				if region, hasRegion := rm["Region"].(string); hasRegion && region != "" {
					regionCopy := region
					replicas = append(replicas, ddbtypes.Replica{RegionName: &regionCopy})
				}
			}
		}
	}

	if len(replicas) == 0 {
		region := "us-east-1"
		replicas = []ddbtypes.Replica{{RegionName: &region}}
	}

	out, err := rc.backends.DynamoDB.Backend.CreateGlobalTable(ctx, &awsddb.CreateGlobalTableInput{
		GlobalTableName:  &tableName,
		ReplicationGroup: replicas,
	})
	if err != nil {
		return "", fmt.Errorf("create DynamoDB global table %s: %w", tableName, err)
	}

	if out.GlobalTableDescription != nil && out.GlobalTableDescription.GlobalTableArn != nil {
		return *out.GlobalTableDescription.GlobalTableArn, nil
	}

	return tableName, nil
}

// deleteDynamoDBSupplementalResource handles deletion for DynamoDB supplemental resource
// types that have no real backend operation to reverse.
func (rc *ResourceCreator) deleteDynamoDBSupplementalResource(resourceType, _ string) bool {
	return resourceType == "AWS::DynamoDB::GlobalTable"
}
