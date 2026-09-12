package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// TestLFTagsOnResource_RoundTrip drives AddLFTagsToResource,
// RemoveLFTagsFromResource and GetResourceLFTags through a real SDK client.
func TestLFTagsOnResource_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	backend.AddLFTagInternal("", "env", []string{"prod", "dev"})
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	dbResource := &types.Resource{Database: &types.DatabaseResource{Name: aws.String("salesdb")}}

	addOut, err := client.AddLFTagsToResource(t.Context(), &lakeformationsdk.AddLFTagsToResourceInput{
		Resource: dbResource,
		LFTags:   []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, addOut.Failures)

	getOut, err := client.GetResourceLFTags(t.Context(), &lakeformationsdk.GetResourceLFTagsInput{
		Resource: dbResource,
	})
	require.NoError(t, err)
	require.Len(t, getOut.LFTagOnDatabase, 1)
	assert.Equal(t, "env", aws.ToString(getOut.LFTagOnDatabase[0].TagKey))
	assert.Equal(t, []string{"prod"}, getOut.LFTagOnDatabase[0].TagValues)
	assert.Empty(t, getOut.LFTagsOnTable)
	assert.Empty(t, getOut.LFTagsOnColumns)

	removeOut, err := client.RemoveLFTagsFromResource(t.Context(), &lakeformationsdk.RemoveLFTagsFromResourceInput{
		Resource: dbResource,
		LFTags:   []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, removeOut.Failures)

	getOut2, err := client.GetResourceLFTags(t.Context(), &lakeformationsdk.GetResourceLFTagsInput{
		Resource: dbResource,
	})
	require.NoError(t, err)
	assert.Empty(t, getOut2.LFTagOnDatabase)
}

// TestBatchGrantRevokePermissions_RoundTrip drives BatchGrantPermissions and
// BatchRevokePermissions through a real SDK client.
func TestBatchGrantRevokePermissions_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	entry := types.BatchPermissionsRequestEntry{
		Id: aws.String("entry-1"),
		Principal: &types.DataLakePrincipal{
			DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:user/alice"),
		},
		Resource:    &types.Resource{Database: &types.DatabaseResource{Name: aws.String("salesdb")}},
		Permissions: []types.Permission{types.PermissionDescribe},
	}

	grantOut, err := client.BatchGrantPermissions(t.Context(), &lakeformationsdk.BatchGrantPermissionsInput{
		Entries: []types.BatchPermissionsRequestEntry{entry},
	})
	require.NoError(t, err)
	assert.Empty(t, grantOut.Failures)

	listOut, err := client.ListPermissions(t.Context(), &lakeformationsdk.ListPermissionsInput{
		Principal: entry.Principal,
	})
	require.NoError(t, err)
	require.Len(t, listOut.PrincipalResourcePermissions, 1)
	assert.Equal(t, []types.Permission{types.PermissionDescribe}, listOut.PrincipalResourcePermissions[0].Permissions)

	revokeOut, err := client.BatchRevokePermissions(t.Context(), &lakeformationsdk.BatchRevokePermissionsInput{
		Entries: []types.BatchPermissionsRequestEntry{entry},
	})
	require.NoError(t, err)
	assert.Empty(t, revokeOut.Failures)

	listOut2, err := client.ListPermissions(t.Context(), &lakeformationsdk.ListPermissionsInput{
		Principal: entry.Principal,
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.PrincipalResourcePermissions)
}

// TestTransactionLifecycle_RoundTrip drives StartTransaction,
// DescribeTransaction, ListTransactions, ExtendTransaction,
// CancelTransaction, CommitTransaction and DeleteObjectsOnCancel through a
// real SDK client.
func TestTransactionLifecycle_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	startOut, err := client.StartTransaction(t.Context(), &lakeformationsdk.StartTransactionInput{
		TransactionType: types.TransactionTypeReadAndWrite,
	})
	require.NoError(t, err)
	txnID := aws.ToString(startOut.TransactionId)
	require.NotEmpty(t, txnID)

	descOut, err := client.DescribeTransaction(t.Context(), &lakeformationsdk.DescribeTransactionInput{
		TransactionId: aws.String(txnID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.TransactionDescription)
	assert.Equal(t, types.TransactionStatusActive, descOut.TransactionDescription.TransactionStatus)
	assert.Equal(t, txnID, aws.ToString(descOut.TransactionDescription.TransactionId))
	assert.NotNil(t, descOut.TransactionDescription.TransactionStartTime)

	extendOut, err := client.ExtendTransaction(t.Context(), &lakeformationsdk.ExtendTransactionInput{
		TransactionId: aws.String(txnID),
	})
	require.NoError(t, err)
	require.NotNil(t, extendOut)

	listOut, err := client.ListTransactions(t.Context(), &lakeformationsdk.ListTransactionsInput{
		StatusFilter: types.TransactionStatusFilterActive,
	})
	require.NoError(t, err)
	var found bool
	for _, txn := range listOut.Transactions {
		if aws.ToString(txn.TransactionId) == txnID {
			found = true
		}
	}
	assert.True(t, found, "started transaction should appear in ListTransactions ACTIVE filter")

	cancelOut, err := client.CancelTransaction(t.Context(), &lakeformationsdk.CancelTransactionInput{
		TransactionId: aws.String(txnID),
	})
	require.NoError(t, err)
	require.NotNil(t, cancelOut)

	descOut2, err := client.DescribeTransaction(t.Context(), &lakeformationsdk.DescribeTransactionInput{
		TransactionId: aws.String(txnID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TransactionStatusAborted, descOut2.TransactionDescription.TransactionStatus)

	deleteOut, err := client.DeleteObjectsOnCancel(t.Context(), &lakeformationsdk.DeleteObjectsOnCancelInput{
		DatabaseName:  aws.String("salesdb"),
		TableName:     aws.String("orders"),
		TransactionId: aws.String(txnID),
		Objects: []types.VirtualObject{
			{Uri: aws.String("s3://bucket/orders/part-0000")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, deleteOut)

	startOut2, err := client.StartTransaction(t.Context(), &lakeformationsdk.StartTransactionInput{})
	require.NoError(t, err)
	txnID2 := aws.ToString(startOut2.TransactionId)

	commitOut, err := client.CommitTransaction(t.Context(), &lakeformationsdk.CommitTransactionInput{
		TransactionId: aws.String(txnID2),
	})
	require.NoError(t, err)
	require.NotNil(t, commitOut)

	descOut3, err := client.DescribeTransaction(t.Context(), &lakeformationsdk.DescribeTransactionInput{
		TransactionId: aws.String(txnID2),
	})
	require.NoError(t, err)
	assert.Equal(t, types.TransactionStatusCommitted, descOut3.TransactionDescription.TransactionStatus)
}

// TestDataCellsFilter_UpdateRoundTrip drives CreateDataCellsFilter and
// UpdateDataCellsFilter through a real SDK client.
func TestDataCellsFilter_UpdateRoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	tableData := &types.DataCellsFilter{
		TableCatalogId: aws.String("123456789012"),
		DatabaseName:   aws.String("salesdb"),
		TableName:      aws.String("orders"),
		Name:           aws.String("region-filter"),
		RowFilter:      &types.RowFilter{FilterExpression: aws.String("region='us-east-1'")},
	}

	_, err := client.CreateDataCellsFilter(t.Context(), &lakeformationsdk.CreateDataCellsFilterInput{
		TableData: tableData,
	})
	require.NoError(t, err)

	tableData.RowFilter = &types.RowFilter{FilterExpression: aws.String("region='us-west-2'")}

	updateOut, err := client.UpdateDataCellsFilter(t.Context(), &lakeformationsdk.UpdateDataCellsFilterInput{
		TableData: tableData,
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut)

	getOut, err := client.GetDataCellsFilter(t.Context(), &lakeformationsdk.GetDataCellsFilterInput{
		TableCatalogId: aws.String("123456789012"),
		DatabaseName:   aws.String("salesdb"),
		TableName:      aws.String("orders"),
		Name:           aws.String("region-filter"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.DataCellsFilter)
	require.NotNil(t, getOut.DataCellsFilter.RowFilter)
	assert.Equal(t, "region='us-west-2'", aws.ToString(getOut.DataCellsFilter.RowFilter.FilterExpression))
}

// TestLFTagExpression_RoundTrip drives CreateLFTagExpression,
// GetLFTagExpression, UpdateLFTagExpression, ListLFTagExpressions and
// DeleteLFTagExpression through a real SDK client.
func TestLFTagExpression_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	_, err := client.CreateLFTagExpression(t.Context(), &lakeformationsdk.CreateLFTagExpressionInput{
		Name:        aws.String("expr-1"),
		Description: aws.String("initial"),
		Expression:  []types.LFTag{{TagKey: aws.String("env"), TagValues: []string{"prod"}}},
	})
	require.NoError(t, err)

	getOut, err := client.GetLFTagExpression(t.Context(), &lakeformationsdk.GetLFTagExpressionInput{
		Name: aws.String("expr-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "initial", aws.ToString(getOut.Description))
	require.Len(t, getOut.Expression, 1)
	assert.Equal(t, "env", aws.ToString(getOut.Expression[0].TagKey))

	_, err = client.UpdateLFTagExpression(t.Context(), &lakeformationsdk.UpdateLFTagExpressionInput{
		Name:        aws.String("expr-1"),
		Description: aws.String("updated"),
		Expression:  []types.LFTag{{TagKey: aws.String("env"), TagValues: []string{"dev"}}},
	})
	require.NoError(t, err)

	getOut2, err := client.GetLFTagExpression(t.Context(), &lakeformationsdk.GetLFTagExpressionInput{
		Name: aws.String("expr-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(getOut2.Description))
	require.Len(t, getOut2.Expression, 1)
	assert.Equal(t, []string{"dev"}, getOut2.Expression[0].TagValues)

	listOut, err := client.ListLFTagExpressions(t.Context(), &lakeformationsdk.ListLFTagExpressionsInput{})
	require.NoError(t, err)
	var found bool
	for _, e := range listOut.LFTagExpressions {
		if aws.ToString(e.Name) == "expr-1" {
			found = true
		}
	}
	assert.True(t, found)

	_, err = client.DeleteLFTagExpression(t.Context(), &lakeformationsdk.DeleteLFTagExpressionInput{
		Name: aws.String("expr-1"),
	})
	require.NoError(t, err)

	_, err = client.GetLFTagExpression(t.Context(), &lakeformationsdk.GetLFTagExpressionInput{
		Name: aws.String("expr-1"),
	})
	require.Error(t, err)
}

// TestLakeFormationOptIn_RoundTrip drives CreateLakeFormationOptIn,
// ListLakeFormationOptIns and DeleteLakeFormationOptIn through a real SDK
// client.
func TestLakeFormationOptIn_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	principal := &types.DataLakePrincipal{
		DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:role/analyst"),
	}
	resource := &types.Resource{Database: &types.DatabaseResource{Name: aws.String("salesdb")}}

	_, err := client.CreateLakeFormationOptIn(t.Context(), &lakeformationsdk.CreateLakeFormationOptInInput{
		Principal: principal,
		Resource:  resource,
	})
	require.NoError(t, err)

	listOut, err := client.ListLakeFormationOptIns(t.Context(), &lakeformationsdk.ListLakeFormationOptInsInput{
		Principal: principal,
	})
	require.NoError(t, err)
	require.Len(t, listOut.LakeFormationOptInsInfoList, 1)
	assert.Equal(
		t,
		"arn:aws:iam::123456789012:role/analyst",
		aws.ToString(listOut.LakeFormationOptInsInfoList[0].Principal.DataLakePrincipalIdentifier),
	)
	assert.NotNil(t, listOut.LakeFormationOptInsInfoList[0].LastModified)

	_, err = client.DeleteLakeFormationOptIn(t.Context(), &lakeformationsdk.DeleteLakeFormationOptInInput{
		Principal: principal,
		Resource:  resource,
	})
	require.NoError(t, err)

	listOut2, err := client.ListLakeFormationOptIns(t.Context(), &lakeformationsdk.ListLakeFormationOptInsInput{
		Principal: principal,
	})
	require.NoError(t, err)
	assert.Empty(t, listOut2.LakeFormationOptInsInfoList)
}

// TestDeleteLakeFormationIdentityCenterConfiguration_RoundTrip drives
// CreateLakeFormationIdentityCenterConfiguration then
// DeleteLakeFormationIdentityCenterConfiguration through a real SDK client.
func TestDeleteLakeFormationIdentityCenterConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(backend)
	h.AccountID = "123456789012"
	client := newTestLakeFormationClient(t, h)

	_, err := client.CreateLakeFormationIdentityCenterConfiguration(
		t.Context(), &lakeformationsdk.CreateLakeFormationIdentityCenterConfigurationInput{
			InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-0000000000000000"),
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeLakeFormationIdentityCenterConfiguration(
		t.Context(), &lakeformationsdk.DescribeLakeFormationIdentityCenterConfigurationInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(descOut.ApplicationArn))

	_, err = client.DeleteLakeFormationIdentityCenterConfiguration(
		t.Context(), &lakeformationsdk.DeleteLakeFormationIdentityCenterConfigurationInput{},
	)
	require.NoError(t, err)

	_, err = client.DescribeLakeFormationIdentityCenterConfiguration(
		t.Context(), &lakeformationsdk.DescribeLakeFormationIdentityCenterConfigurationInput{},
	)
	require.Error(t, err)
}

// TestGetDataLakePrincipal_RoundTrip drives GetDataLakePrincipal through a
// real SDK client.
func TestGetDataLakePrincipal_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	out, err := client.GetDataLakePrincipal(t.Context(), &lakeformationsdk.GetDataLakePrincipalInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.Identity))
}

// TestTableObjectsAndStorageOptimizer_RoundTrip drives UpdateTableObjects
// (Add + Delete), GetTableObjects, UpdateTableStorageOptimizer and
// ListTableStorageOptimizers through a real SDK client.
func TestTableObjectsAndStorageOptimizer_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	startOut, err := client.StartTransaction(t.Context(), &lakeformationsdk.StartTransactionInput{})
	require.NoError(t, err)
	txnID := aws.ToString(startOut.TransactionId)

	_, err = client.UpdateTableObjects(t.Context(), &lakeformationsdk.UpdateTableObjectsInput{
		DatabaseName:  aws.String("salesdb"),
		TableName:     aws.String("orders"),
		TransactionId: aws.String(txnID),
		WriteOperations: []types.WriteOperation{
			{AddObject: &types.AddObjectInput{
				Uri:  aws.String("s3://bucket/orders/part-0000"),
				ETag: aws.String("etag-1"),
				Size: 1024,
			}},
		},
	})
	require.NoError(t, err)

	getOut, err := client.GetTableObjects(t.Context(), &lakeformationsdk.GetTableObjectsInput{
		DatabaseName: aws.String("salesdb"),
		TableName:    aws.String("orders"),
	})
	require.NoError(t, err)
	require.Len(t, getOut.Objects, 1)
	require.Len(t, getOut.Objects[0].Objects, 1)
	assert.Equal(t, "s3://bucket/orders/part-0000", aws.ToString(getOut.Objects[0].Objects[0].Uri))
	assert.Equal(t, int64(1024), getOut.Objects[0].Objects[0].Size)

	_, err = client.UpdateTableObjects(t.Context(), &lakeformationsdk.UpdateTableObjectsInput{
		DatabaseName:  aws.String("salesdb"),
		TableName:     aws.String("orders"),
		TransactionId: aws.String(txnID),
		WriteOperations: []types.WriteOperation{
			{DeleteObject: &types.DeleteObjectInput{
				Uri: aws.String("s3://bucket/orders/part-0000"),
			}},
		},
	})
	require.NoError(t, err)

	getOut2, err := client.GetTableObjects(t.Context(), &lakeformationsdk.GetTableObjectsInput{
		DatabaseName: aws.String("salesdb"),
		TableName:    aws.String("orders"),
	})
	require.NoError(t, err)
	for _, partition := range getOut2.Objects {
		assert.Empty(t, partition.Objects, "DeleteObject write should remove the added object")
	}

	updateOptOut, err := client.UpdateTableStorageOptimizer(
		t.Context(),
		&lakeformationsdk.UpdateTableStorageOptimizerInput{
			DatabaseName: aws.String("salesdb"),
			TableName:    aws.String("orders"),
			StorageOptimizerConfig: map[string]map[string]string{
				"COMPACTION": {"is_enabled": "true"},
			},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(updateOptOut.Result))

	listOptOut, err := client.ListTableStorageOptimizers(t.Context(), &lakeformationsdk.ListTableStorageOptimizersInput{
		DatabaseName: aws.String("salesdb"),
		TableName:    aws.String("orders"),
	})
	require.NoError(t, err)
	require.Len(t, listOptOut.StorageOptimizerList, 1)
	assert.Equal(t, types.OptimizerType("COMPACTION"), listOptOut.StorageOptimizerList[0].StorageOptimizerType)
	assert.Equal(t, "true", listOptOut.StorageOptimizerList[0].Config["is_enabled"])
}

// TestGetTemporaryGluePartitionCredentials_RoundTrip drives
// GetTemporaryGluePartitionCredentials through a real SDK client.
func TestGetTemporaryGluePartitionCredentials_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	out, err := client.GetTemporaryGluePartitionCredentials(
		t.Context(), &lakeformationsdk.GetTemporaryGluePartitionCredentialsInput{
			TableArn: aws.String("arn:aws:glue:us-east-1:123456789012:table/salesdb/orders"),
			Partition: &types.PartitionValueList{
				Values: []string{"2024", "01"},
			},
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.AccessKeyId))
	assert.NotEmpty(t, aws.ToString(out.SecretAccessKey))
	assert.NotEmpty(t, aws.ToString(out.SessionToken))
	assert.NotNil(t, out.Expiration)
}

// TestGetQueryState_RoundTrip drives StartQueryPlanning and GetQueryState
// through a real SDK client.
func TestGetQueryState_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	startOut, err := client.StartQueryPlanning(t.Context(), &lakeformationsdk.StartQueryPlanningInput{
		QueryPlanningContext: &types.QueryPlanningContext{
			DatabaseName: aws.String("salesdb"),
		},
		QueryString: aws.String("SELECT * FROM orders"),
	})
	require.NoError(t, err)
	queryID := aws.ToString(startOut.QueryId)
	require.NotEmpty(t, queryID)

	stateOut, err := client.GetQueryState(t.Context(), &lakeformationsdk.GetQueryStateInput{
		QueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.QueryStateStringWorkunitsAvailable, stateOut.State)
}

// TestSearchDatabasesTablesByLFTags_RoundTrip drives SearchDatabasesByLFTags
// and SearchTablesByLFTags through a real SDK client.
func TestSearchDatabasesTablesByLFTags_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	backend.AddLFTagInternal("", "env", []string{"prod", "dev"})
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	_, err := client.AddLFTagsToResource(t.Context(), &lakeformationsdk.AddLFTagsToResourceInput{
		Resource: &types.Resource{Database: &types.DatabaseResource{Name: aws.String("salesdb")}},
		LFTags:   []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}},
	})
	require.NoError(t, err)

	_, err = client.AddLFTagsToResource(t.Context(), &lakeformationsdk.AddLFTagsToResourceInput{
		Resource: &types.Resource{
			Table: &types.TableResource{DatabaseName: aws.String("salesdb"), Name: aws.String("orders")},
		},
		LFTags: []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}},
	})
	require.NoError(t, err)

	expr := []types.LFTag{{TagKey: aws.String("env"), TagValues: []string{"prod"}}}

	dbOut, err := client.SearchDatabasesByLFTags(t.Context(), &lakeformationsdk.SearchDatabasesByLFTagsInput{
		Expression: expr,
	})
	require.NoError(t, err)
	require.Len(t, dbOut.DatabaseList, 1)
	assert.Equal(t, "salesdb", aws.ToString(dbOut.DatabaseList[0].Database.Name))
	require.Len(t, dbOut.DatabaseList[0].LFTags, 1)

	tblOut, err := client.SearchTablesByLFTags(t.Context(), &lakeformationsdk.SearchTablesByLFTagsInput{
		Expression: expr,
	})
	require.NoError(t, err)
	require.Len(t, tblOut.TableList, 1)
	assert.Equal(t, "orders", aws.ToString(tblOut.TableList[0].Table.Name))
	assert.Equal(t, "salesdb", aws.ToString(tblOut.TableList[0].Table.DatabaseName))
}

// TestAssumeDecoratedRoleWithSAML_RoundTrip drives
// AssumeDecoratedRoleWithSAML through a real SDK client.
func TestAssumeDecoratedRoleWithSAML_TypedRoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	out, err := client.AssumeDecoratedRoleWithSAML(t.Context(), &lakeformationsdk.AssumeDecoratedRoleWithSAMLInput{
		PrincipalArn:  aws.String("arn:aws:iam::123456789012:saml-provider/idp"),
		RoleArn:       aws.String("arn:aws:iam::123456789012:role/analyst"),
		SAMLAssertion: aws.String("dGVzdC1hc3NlcnRpb24="),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.AccessKeyId))
	assert.NotEmpty(t, aws.ToString(out.SecretAccessKey))
	assert.NotEmpty(t, aws.ToString(out.SessionToken))
	assert.NotNil(t, out.Expiration)
}

// TestUpdateResource_RoundTrip drives RegisterResource, UpdateResource and
// DescribeResource through a real SDK client.
func TestUpdateResource_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := lakeformation.NewInMemoryBackend()
	client := newTestLakeFormationClient(t, lakeformation.NewHandler(backend))

	resourceArn := "arn:aws:s3:::my-governed-bucket"

	_, err := client.RegisterResource(t.Context(), &lakeformationsdk.RegisterResourceInput{
		ResourceArn: aws.String(resourceArn),
		RoleArn:     aws.String("arn:aws:iam::123456789012:role/lf-service-role"),
	})
	require.NoError(t, err)

	_, err = client.UpdateResource(t.Context(), &lakeformationsdk.UpdateResourceInput{
		ResourceArn:    aws.String(resourceArn),
		RoleArn:        aws.String("arn:aws:iam::123456789012:role/lf-updated-role"),
		WithFederation: aws.Bool(true),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeResource(t.Context(), &lakeformationsdk.DescribeResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ResourceInfo)
	assert.Equal(t, "arn:aws:iam::123456789012:role/lf-updated-role", aws.ToString(descOut.ResourceInfo.RoleArn))
	assert.True(t, aws.ToBool(descOut.ResourceInfo.WithFederation))
}
