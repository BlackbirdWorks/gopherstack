package dynamodb_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestCreateTable_ResourcePolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name    string
		policy  string
		wantErr bool
	}{
		{name: "attached", policy: policy},
		{
			name:    "oversize_rejected",
			policy:  `{"Pad":"` + strings.Repeat("x", 20*1024) + `"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

			out, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName:            aws.String("rp-table"),
				BillingMode:          types.BillingModePayPerRequest,
				ResourcePolicy:       aws.String(tt.policy),
				AttributeDefinitions: pkAttrDefs(),
				KeySchema:            pkKeySchema(),
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetResourcePolicy(t.Context(), &sdk.GetResourcePolicyInput{
				ResourceArn: out.TableDescription.TableArn,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.policy, aws.ToString(got.Policy))
			assert.NotEmpty(t, aws.ToString(got.RevisionId))
		})
	}
}

func TestUpdateTable_ThroughputChangeTimestamps(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName:             aws.String("tp-table"),
		ProvisionedThroughput: provisioned(5),
		AttributeDefinitions:  pkAttrDefs(),
		KeySchema:             pkKeySchema(),
	})
	require.NoError(t, err)

	describe := func() *types.ProvisionedThroughputDescription {
		out, derr := client.DescribeTable(t.Context(), &sdk.DescribeTableInput{TableName: aws.String("tp-table")})
		require.NoError(t, derr)

		return out.Table.ProvisionedThroughput
	}

	update := func(rcu int64) {
		_, uerr := client.UpdateTable(t.Context(), &sdk.UpdateTableInput{
			TableName:             aws.String("tp-table"),
			ProvisionedThroughput: provisioned(rcu),
		})
		require.NoError(t, uerr)
	}

	pt := describe()
	assert.Nil(t, pt.LastIncreaseDateTime)
	assert.Nil(t, pt.LastDecreaseDateTime)
	require.NotNil(t, pt.NumberOfDecreasesToday)
	assert.Equal(t, int64(0), *pt.NumberOfDecreasesToday)

	update(10)

	pt = describe()
	require.NotNil(t, pt.LastIncreaseDateTime)
	assert.Nil(t, pt.LastDecreaseDateTime)
	assert.Equal(t, int64(0), aws.ToInt64(pt.NumberOfDecreasesToday))

	update(4)
	update(2)

	pt = describe()
	require.NotNil(t, pt.LastDecreaseDateTime)
	assert.Equal(t, int64(2), aws.ToInt64(pt.NumberOfDecreasesToday))
}

func testLSI(name string) types.LocalSecondaryIndex {
	return types.LocalSecondaryIndex{
		IndexName: aws.String(name),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String(name + "sk"), KeyType: types.KeyTypeRange},
		},
		Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
	}
}

func TestRestoreTable_LocalSecondaryIndexOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		override []types.LocalSecondaryIndex
		want     []string
	}{
		{name: "omitted_keeps_all", override: nil, want: []string{"l1", "l2"}},
		{name: "subset", override: []types.LocalSecondaryIndex{testLSI("l2")}, want: []string{"l2"}},
		{name: "empty_excludes_all", override: []types.LocalSecondaryIndex{}, want: []string{}},
	}

	for _, mode := range []string{"backup", "pitr"} {
		for _, tt := range tests {
			t.Run(mode+"_"+tt.name, func(t *testing.T) {
				t.Parallel()
				runLSIOverrideRestore(t, mode == "pitr", tt.override, tt.want)
			})
		}
	}
}

func runLSIOverrideRestore(t *testing.T, pitr bool, override []types.LocalSecondaryIndex, want []string) {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	_, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName:   aws.String("lsi-src"),
		BillingMode: types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("l1sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("l2sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{testLSI("l1"), testLSI("l2")},
	})
	require.NoError(t, err)

	if pitr {
		_, err = client.UpdateContinuousBackups(t.Context(), &sdk.UpdateContinuousBackupsInput{
			TableName: aws.String("lsi-src"),
			PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
				PointInTimeRecoveryEnabled: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		_, err = client.RestoreTableToPointInTime(t.Context(), &sdk.RestoreTableToPointInTimeInput{
			SourceTableName:             aws.String("lsi-src"),
			TargetTableName:             aws.String("lsi-dst"),
			UseLatestRestorableTime:     aws.Bool(true),
			LocalSecondaryIndexOverride: override,
		})
		require.NoError(t, err)
	} else {
		backup, berr := client.CreateBackup(t.Context(), &sdk.CreateBackupInput{
			TableName:  aws.String("lsi-src"),
			BackupName: aws.String("b1"),
		})
		require.NoError(t, berr)

		_, err = client.RestoreTableFromBackup(t.Context(), &sdk.RestoreTableFromBackupInput{
			BackupArn:                   backup.BackupDetails.BackupArn,
			TargetTableName:             aws.String("lsi-dst"),
			LocalSecondaryIndexOverride: override,
		})
		require.NoError(t, err)
	}

	desc, err := client.DescribeTable(t.Context(), &sdk.DescribeTableInput{TableName: aws.String("lsi-dst")})
	require.NoError(t, err)

	got := make([]string, 0, len(desc.Table.LocalSecondaryIndexes))
	for _, l := range desc.Table.LocalSecondaryIndexes {
		got = append(got, aws.ToString(l.IndexName))
	}

	assert.ElementsMatch(t, want, got)
}

func TestDescribeTable_ReplicaArn(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))

	created, err := client.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName:            aws.String("replica-arn-table"),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: pkAttrDefs(),
		KeySchema:            pkKeySchema(),
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(t.Context(), &sdk.UpdateTableInput{
		TableName: aws.String("replica-arn-table"),
		ReplicaUpdates: []types.ReplicationGroupUpdate{
			{Create: &types.CreateReplicationGroupMemberAction{RegionName: aws.String("eu-west-1")}},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeTable(t.Context(), &sdk.DescribeTableInput{TableName: aws.String("replica-arn-table")})
	require.NoError(t, err)

	want := strings.Replace(aws.ToString(created.TableDescription.TableArn), ":"+ddbTagsRTRegion+":", ":eu-west-1:", 1)

	var got string

	for _, r := range desc.Table.Replicas {
		if aws.ToString(r.RegionName) == "eu-west-1" {
			got = aws.ToString(r.ReplicaArn)
		}
	}

	assert.Equal(t, want, got)
}

func pkAttrDefs() []types.AttributeDefinition {
	return []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}}
}

func pkKeySchema() []types.KeySchemaElement {
	return []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
}

func provisioned(units int64) *types.ProvisionedThroughput {
	return &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(units), WriteCapacityUnits: aws.Int64(units)}
}
