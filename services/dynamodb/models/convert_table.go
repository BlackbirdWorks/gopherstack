package models

import (
	"github.com/aws/aws-sdk-go-v2/aws"
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

	return &dynamodb.CreateTableInput{
		TableName:              &input.TableName,
		KeySchema:              ToSDKKeySchema(input.KeySchema),
		AttributeDefinitions:   ToSDKAttributeDefinitions(input.AttributeDefinitions),
		GlobalSecondaryIndexes: ToSDKGlobalSecondaryIndexes(input.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  ToSDKLocalSecondaryIndexes(input.LocalSecondaryIndexes),
		ProvisionedThroughput:  pt,
		StreamSpecification:    ss,
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

	gsiUpdates := make([]types.GlobalSecondaryIndexUpdate, 0, len(input.GlobalSecondaryIndexUpdates))

	for _, u := range input.GlobalSecondaryIndexUpdates {
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

		gsiUpdates = append(gsiUpdates, update)
	}

	out.GlobalSecondaryIndexUpdates = gsiUpdates

	// Convert replica updates (Global Tables v2).
	replicaUpdates := make([]types.ReplicationGroupUpdate, 0, len(input.ReplicaUpdates))
	for _, ru := range input.ReplicaUpdates {
		sdkRU := types.ReplicationGroupUpdate{}
		if ru.Create != nil {
			sdkRU.Create = &types.CreateReplicationGroupMemberAction{
				RegionName: &ru.Create.RegionName,
			}
		}
		if ru.Delete != nil {
			sdkRU.Delete = &types.DeleteReplicationGroupMemberAction{
				RegionName: &ru.Delete.RegionName,
			}
		}
		replicaUpdates = append(replicaUpdates, sdkRU)
	}
	out.ReplicaUpdates = replicaUpdates

	return out, nil
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

	replicas := make([]ReplicaDescription, len(td.Replicas))
	for i, r := range td.Replicas {
		replicas[i] = ReplicaDescription{
			RegionName:    ptrconv.String(r.RegionName),
			ReplicaStatus: string(r.ReplicaStatus),
		}
	}
	if len(replicas) == 0 {
		replicas = nil
	}

	out := TableDescription{
		TableName:              ptrconv.String(td.TableName),
		TableStatus:            string(td.TableStatus),
		TableArn:               ptrconv.String(td.TableArn),
		TableID:                ptrconv.String(td.TableId),
		ItemCount:              cnt,
		KeySchema:              FromSDKKeySchema(td.KeySchema),
		AttributeDefinitions:   FromSDKAttributeDefinitions(td.AttributeDefinitions),
		GlobalSecondaryIndexes: FromSDKGlobalSecondaryIndexDescriptions(td.GlobalSecondaryIndexes),
		LocalSecondaryIndexes:  FromSDKLocalSecondaryIndexDescriptions(td.LocalSecondaryIndexes),
		ProvisionedThroughput:  FromSDKProvisionedThroughputDescription(td.ProvisionedThroughput),
		Replicas:               replicas,
		LatestStreamArn:        ptrconv.String(td.LatestStreamArn),
		LatestStreamLabel:      ptrconv.String(td.LatestStreamLabel),
	}

	if td.StreamSpecification != nil {
		out.StreamSpecification = &StreamSpecificationInput{
			StreamEnabled:  aws.ToBool(td.StreamSpecification.StreamEnabled),
			StreamViewType: string(td.StreamSpecification.StreamViewType),
		}
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
	return &dynamodb.UntagResourceInput{ResourceArn: &input.ResourceArn, TagKeys: input.TagKeys}, nil
}

// FromSDKUntagResourceOutput converts the AWS SDK UntagResourceOutput to wire format.
func FromSDKUntagResourceOutput(_ *dynamodb.UntagResourceOutput) *UntagResourceOutput {
	return &UntagResourceOutput{}
}

// ToSDKListTagsOfResourceInput converts the wire-format input to an AWS SDK input.
func ToSDKListTagsOfResourceInput(input *ListTagsOfResourceInput) (*dynamodb.ListTagsOfResourceInput, error) {
	out := &dynamodb.ListTagsOfResourceInput{ResourceArn: &input.ResourceArn}
	if input.NextToken != "" {
		out.NextToken = &input.NextToken
	}

	return out, nil
}

// FromSDKListTagsOfResourceOutput converts the AWS SDK output to wire format.
func FromSDKListTagsOfResourceOutput(output *dynamodb.ListTagsOfResourceOutput) *ListTagsOfResourceOutput {
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
