package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_GlossaryLifecycle drives Create/Get/UpdateGlossary,
// ListGlossaries, CreateGlossaryTerm, GetGlossaryTerm, and
// Associate/DisassociateGlossaryTerms.
func TestRealClient_GlossaryLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	created, err := client.CreateGlossary(ctx, &gluesdk.CreateGlossaryInput{
		Name:        aws.String("business-terms"),
		Description: aws.String("initial"),
	})
	require.NoError(t, err)
	glossaryID := created.Id

	got, err := client.GetGlossary(ctx, &gluesdk.GetGlossaryInput{Identifier: glossaryID})
	require.NoError(t, err)
	assert.Equal(t, "business-terms", aws.ToString(got.Name))

	updated, err := client.UpdateGlossary(ctx, &gluesdk.UpdateGlossaryInput{
		Identifier:  glossaryID,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(updated.Description))

	listed, err := client.ListGlossaries(ctx, &gluesdk.ListGlossariesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Items, 1)
	assert.Equal(t, aws.ToString(glossaryID), aws.ToString(listed.Items[0].Id))

	term, err := client.CreateGlossaryTerm(ctx, &gluesdk.CreateGlossaryTermInput{
		GlossaryIdentifier: glossaryID,
		Name:               aws.String("PII"),
		ShortDescription:   aws.String("personally identifiable information"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(glossaryID), aws.ToString(term.GlossaryId))

	gotTerm, err := client.GetGlossaryTerm(ctx, &gluesdk.GetGlossaryTermInput{Identifier: term.Id})
	require.NoError(t, err)
	assert.Equal(t, "PII", aws.ToString(gotTerm.Name))

	updatedTerm, err := client.UpdateGlossaryTerm(ctx, &gluesdk.UpdateGlossaryTermInput{
		Identifier:       term.Id,
		ShortDescription: aws.String("updated short description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated short description", aws.ToString(updatedTerm.ShortDescription))

	_, err = client.PutFormType(ctx, &gluesdk.PutFormTypeInput{
		Name:   aws.String("SchemaForm"),
		Schema: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)

	_, err = client.PutAssetType(ctx, &gluesdk.PutAssetTypeInput{
		Name: aws.String("TableAsset"),
		Forms: map[string]types.AssetTypeFormReference{
			"schema": {FormTypeIdentifier: aws.String("SchemaForm")},
		},
	})
	require.NoError(t, err)

	asset, err := client.PutAsset(ctx, &gluesdk.PutAssetInput{
		Identifier:  aws.String("asset-1"),
		Name:        aws.String("customers table"),
		AssetTypeId: aws.String("TableAsset"),
		Forms:       map[string]types.AssetFormEntry{},
	})
	require.NoError(t, err)

	assoc, err := client.AssociateGlossaryTerms(ctx, &gluesdk.AssociateGlossaryTermsInput{
		AssetIdentifier:         asset.Id,
		GlossaryTermIdentifiers: []string{aws.ToString(term.Id)},
	})
	require.NoError(t, err)
	assert.Contains(t, assoc.GlossaryTerms, aws.ToString(term.Id))

	disassoc, err := client.DisassociateGlossaryTerms(ctx, &gluesdk.DisassociateGlossaryTermsInput{
		AssetIdentifier:         asset.Id,
		GlossaryTermIdentifiers: []string{aws.ToString(term.Id)},
	})
	require.NoError(t, err)
	assert.NotContains(t, disassoc.GlossaryTerms, aws.ToString(term.Id))
}

// TestRealClient_AssetTypeAndFormTypeLifecycle drives PutFormType/GetFormType/
// ListFormTypes, PutAssetType/GetAssetType/ListAssetTypes.
func TestRealClient_AssetTypeAndFormTypeLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	ft, err := client.PutFormType(ctx, &gluesdk.PutFormTypeInput{
		Name:   aws.String("SchemaForm"),
		Schema: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, "SchemaForm", aws.ToString(ft.Name))

	gotForm, err := client.GetFormType(ctx, &gluesdk.GetFormTypeInput{Identifier: aws.String("SchemaForm")})
	require.NoError(t, err)
	assert.JSONEq(t, `{"type":"object"}`, aws.ToString(gotForm.Schema))

	formsList, err := client.ListFormTypes(ctx, &gluesdk.ListFormTypesInput{})
	require.NoError(t, err)
	require.Len(t, formsList.Items, 1)
	assert.Equal(t, "SchemaForm", aws.ToString(formsList.Items[0].Name))

	at, err := client.PutAssetType(ctx, &gluesdk.PutAssetTypeInput{
		Name: aws.String("TableAsset"),
		Forms: map[string]types.AssetTypeFormReference{
			"schema": {FormTypeIdentifier: aws.String("SchemaForm")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "TableAsset", aws.ToString(at.Name))

	gotAT, err := client.GetAssetType(ctx, &gluesdk.GetAssetTypeInput{Identifier: aws.String("TableAsset")})
	require.NoError(t, err)
	require.Contains(t, gotAT.Forms, "schema")
	assert.Equal(t, "SchemaForm", aws.ToString(gotAT.Forms["schema"].FormTypeIdentifier))

	atList, err := client.ListAssetTypes(ctx, &gluesdk.ListAssetTypesInput{})
	require.NoError(t, err)
	require.Len(t, atList.Items, 1)
	assert.Equal(t, "TableAsset", aws.ToString(atList.Items[0].Name))
}

// TestRealClient_AssetAndAttachmentLifecycle drives GetAsset, UpdateAsset,
// SearchAssets, PutAttachment, DeleteAttachment, BatchGetIterableForms and
// ListIterableForms.
func TestRealClient_AssetAndAttachmentLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.PutFormType(ctx, &gluesdk.PutFormTypeInput{
		Name:   aws.String("ColumnForm"),
		Schema: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)

	_, err = client.PutAssetType(ctx, &gluesdk.PutAssetTypeInput{
		Name: aws.String("TableAsset2"),
		Forms: map[string]types.AssetTypeFormReference{
			"columns": {FormTypeIdentifier: aws.String("ColumnForm")},
		},
	})
	require.NoError(t, err)

	created, err := client.PutAsset(ctx, &gluesdk.PutAssetInput{
		Identifier:  aws.String("asset-2"),
		Name:        aws.String("orders table"),
		Description: aws.String("orders"),
		AssetTypeId: aws.String("TableAsset2"),
		Forms:       map[string]types.AssetFormEntry{},
	})
	require.NoError(t, err)

	got, err := client.GetAsset(ctx, &gluesdk.GetAssetInput{Identifier: aws.String("asset-2")})
	require.NoError(t, err)
	assert.Equal(t, "orders table", aws.ToString(got.Name))
	assert.Equal(t, aws.ToTime(created.CreatedAt), aws.ToTime(got.CreatedAt))

	updated, err := client.UpdateAsset(ctx, &gluesdk.UpdateAssetInput{
		Identifier:  aws.String("asset-2"),
		Description: aws.String("orders table (updated)"),
	})
	require.NoError(t, err)
	assert.Equal(t, "orders table (updated)", aws.ToString(updated.Description))

	searched, err := client.SearchAssets(ctx, &gluesdk.SearchAssetsInput{SearchText: aws.String("orders")})
	require.NoError(t, err)
	require.Len(t, searched.Items, 1)
	assert.Equal(t, "asset-2", aws.ToString(searched.Items[0].Id))

	_, err = client.PutAttachment(ctx, &gluesdk.PutAttachmentInput{
		AssetIdentifier: aws.String("asset-2"),
		AttachmentName:  aws.String("readme"),
		FormTypeId:      aws.String("ColumnForm"),
		Content:         aws.String(`{"note":"see docs"}`),
	})
	require.NoError(t, err)

	gotWithAttachment, err := client.GetAsset(ctx, &gluesdk.GetAssetInput{Identifier: aws.String("asset-2")})
	require.NoError(t, err)
	require.Contains(t, gotWithAttachment.Attachments, "readme")

	_, err = client.DeleteAttachment(ctx, &gluesdk.DeleteAttachmentInput{
		AssetIdentifier: aws.String("asset-2"),
		AttachmentName:  aws.String("readme"),
	})
	require.NoError(t, err)

	gotAfterDelete, err := client.GetAsset(ctx, &gluesdk.GetAssetInput{Identifier: aws.String("asset-2")})
	require.NoError(t, err)
	assert.NotContains(t, gotAfterDelete.Attachments, "readme")

	_, err = client.PutAttachment(ctx, &gluesdk.PutAttachmentInput{
		AssetIdentifier:  aws.String("asset-2"),
		AttachmentName:   aws.String("col-note"),
		FormTypeId:       aws.String("ColumnForm"),
		Content:          aws.String(`{"type":"varchar"}`),
		IterableFormName: aws.String("columns"),
		ItemIdentifier:   aws.String("customer_id"),
	})
	require.NoError(t, err)

	batchOut, err := client.BatchGetIterableForms(ctx, &gluesdk.BatchGetIterableFormsInput{
		AssetIdentifier:  aws.String("asset-2"),
		IterableFormName: aws.String("columns"),
		ItemIdentifiers:  []string{"customer_id", "no-such-item"},
	})
	require.NoError(t, err)
	require.Len(t, batchOut.Items, 1)
	assert.Equal(t, "customer_id", aws.ToString(batchOut.Items[0].ItemId))
	require.Len(t, batchOut.Errors, 1)
	assert.Equal(t, "no-such-item", aws.ToString(batchOut.Errors[0].ItemIdentifier))

	listItems, err := client.ListIterableForms(ctx, &gluesdk.ListIterableFormsInput{
		AssetIdentifier:  aws.String("asset-2"),
		IterableFormName: aws.String("columns"),
	})
	require.NoError(t, err)
	require.Len(t, listItems.Items, 1)
	assert.Equal(t, "customer_id", aws.ToString(listItems.Items[0].ItemId))
}

// TestRealClient_CustomEntityTypeLifecycle drives Create/Get/
// DeleteCustomEntityType and BatchGetCustomEntityTypes.
func TestRealClient_CustomEntityTypeLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateCustomEntityType(ctx, &gluesdk.CreateCustomEntityTypeInput{
		Name:         aws.String("SSN"),
		RegexString:  aws.String(`\d{3}-\d{2}-\d{4}`),
		ContextWords: []string{"social security"},
	})
	require.NoError(t, err)

	got, err := client.GetCustomEntityType(ctx, &gluesdk.GetCustomEntityTypeInput{Name: aws.String("SSN")})
	require.NoError(t, err)
	assert.Equal(t, `\d{3}-\d{2}-\d{4}`, aws.ToString(got.RegexString))
	assert.Contains(t, got.ContextWords, "social security")

	batch, err := client.BatchGetCustomEntityTypes(ctx, &gluesdk.BatchGetCustomEntityTypesInput{
		Names: []string{"SSN", "no-such-type"},
	})
	require.NoError(t, err)
	require.Len(t, batch.CustomEntityTypes, 1)
	assert.Equal(t, "SSN", aws.ToString(batch.CustomEntityTypes[0].Name))
	require.Len(t, batch.CustomEntityTypesNotFound, 1)
	assert.Equal(t, "no-such-type", batch.CustomEntityTypesNotFound[0])

	_, err = client.DeleteCustomEntityType(ctx, &gluesdk.DeleteCustomEntityTypeInput{Name: aws.String("SSN")})
	require.NoError(t, err)

	_, err = client.GetCustomEntityType(ctx, &gluesdk.GetCustomEntityTypeInput{Name: aws.String("SSN")})
	require.Error(t, err)
}

// TestRealClient_TableOptimizerLifecycle drives UpdateTableOptimizer,
// ListTableOptimizerRuns and DeleteTableOptimizer.
func TestRealClient_TableOptimizerLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("optdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("optdb"),
		TableInput:   &types.TableInput{Name: aws.String("opttbl")},
	})
	require.NoError(t, err)

	_, err = client.CreateTableOptimizer(ctx, &gluesdk.CreateTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		TableOptimizerConfiguration: &types.TableOptimizerConfiguration{
			Enabled: aws.Bool(true),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/optimizer"),
		},
		Type: types.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err)

	runs, err := client.ListTableOptimizerRuns(ctx, &gluesdk.ListTableOptimizerRunsInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		Type:         types.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err)
	require.Len(t, runs.TableOptimizerRuns, 1)
	assert.Equal(t, types.TableOptimizerEventTypeCompleted, runs.TableOptimizerRuns[0].EventType)

	_, err = client.UpdateTableOptimizer(ctx, &gluesdk.UpdateTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		Type:         types.TableOptimizerTypeCompaction,
		TableOptimizerConfiguration: &types.TableOptimizerConfiguration{
			Enabled: aws.Bool(false),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/optimizer"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetTableOptimizer(ctx, &gluesdk.GetTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		Type:         types.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(got.TableOptimizer.Configuration.Enabled))

	_, err = client.DeleteTableOptimizer(ctx, &gluesdk.DeleteTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		Type:         types.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err)

	_, err = client.GetTableOptimizer(ctx, &gluesdk.GetTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("optdb"),
		TableName:    aws.String("opttbl"),
		Type:         types.TableOptimizerTypeCompaction,
	})
	require.Error(t, err)
}

// TestRealClient_UsageProfileDelete drives DeleteUsageProfile.
func TestRealClient_UsageProfileDelete(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateUsageProfile(ctx, &gluesdk.CreateUsageProfileInput{
		Name:          aws.String("profile-1"),
		Description:   aws.String("initial"),
		Configuration: &types.ProfileConfiguration{},
	})
	require.NoError(t, err)

	_, err = client.DeleteUsageProfile(ctx, &gluesdk.DeleteUsageProfileInput{Name: aws.String("profile-1")})
	require.NoError(t, err)

	_, err = client.GetUsageProfile(ctx, &gluesdk.GetUsageProfileInput{Name: aws.String("profile-1")})
	require.Error(t, err)
}

// TestRealClient_IdentityCenterConfigDelete drives
// DeleteGlueIdentityCenterConfiguration.
func TestRealClient_IdentityCenterConfigDelete(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateGlueIdentityCenterConfiguration(
		ctx, &gluesdk.CreateGlueIdentityCenterConfigurationInput{
			InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
		},
	)
	require.NoError(t, err)

	_, err = client.DeleteGlueIdentityCenterConfiguration(
		ctx, &gluesdk.DeleteGlueIdentityCenterConfigurationInput{},
	)
	require.NoError(t, err)

	got, err := client.GetGlueIdentityCenterConfiguration(ctx, &gluesdk.GetGlueIdentityCenterConfigurationInput{})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.InstanceArn), "configuration must be cleared after delete")
}

// TestRealClient_DashboardUrl drives GetDashboardUrl.
func TestRealClient_DashboardUrl(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name: aws.String("job-1"),
		Role: aws.String("arn:aws:iam::000000000000:role/glue"),
		Command: &types.JobCommand{
			Name:           aws.String("glueetl"),
			ScriptLocation: aws.String("s3://bucket/script.py"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetDashboardUrl(ctx, &gluesdk.GetDashboardUrlInput{
		ResourceId:   aws.String("job-1"),
		ResourceType: types.GlueResourceTypeJob,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.Url), "job-1")

	_, err = client.GetDashboardUrl(ctx, &gluesdk.GetDashboardUrlInput{
		ResourceId:   aws.String("no-such-job"),
		ResourceType: types.GlueResourceTypeJob,
	})
	require.Error(t, err)
}

// TestRealClient_ETLScriptGeneration drives CreateScript, GetDataflowGraph,
// GetMapping and GetPlan.
func TestRealClient_ETLScriptGeneration(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("etldb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("etldb"),
		TableInput: &types.TableInput{
			Name: aws.String("srctbl"),
			StorageDescriptor: &types.StorageDescriptor{
				Columns: []types.Column{{Name: aws.String("id"), Type: aws.String("int")}},
			},
		},
	})
	require.NoError(t, err)

	created, err := client.CreateScript(ctx, &gluesdk.CreateScriptInput{
		Language: types.LanguagePython,
		DagNodes: []types.CodeGenNode{
			{
				Id:       aws.String("ds1"),
				NodeType: aws.String("DataSource"),
				Args: []types.CodeGenNodeArg{
					{Name: aws.String("database"), Value: aws.String("etldb")},
					{Name: aws.String("table_name"), Value: aws.String("srctbl")},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.PythonScript))

	graph, err := client.GetDataflowGraph(ctx, &gluesdk.GetDataflowGraphInput{
		PythonScript: created.PythonScript,
	})
	require.NoError(t, err)
	require.Len(t, graph.DagNodes, 1)
	assert.Equal(t, "ds1", aws.ToString(graph.DagNodes[0].Id))

	mapping, err := client.GetMapping(ctx, &gluesdk.GetMappingInput{
		Source: &types.CatalogEntry{DatabaseName: aws.String("etldb"), TableName: aws.String("srctbl")},
	})
	require.NoError(t, err)
	require.Len(t, mapping.Mapping, 1)
	assert.Equal(t, "id", aws.ToString(mapping.Mapping[0].SourcePath))

	plan, err := client.GetPlan(ctx, &gluesdk.GetPlanInput{
		Language: types.LanguagePython,
		Mapping:  []types.MappingEntry{},
		Source:   &types.CatalogEntry{DatabaseName: aws.String("etldb"), TableName: aws.String("srctbl")},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(plan.PythonScript))
}

// TestRealClient_DescribeEntity drives DescribeEntity (through the
// connection-scoped path -- ListEntities/GetEntityRecords also accept the
// connectionless native-catalog path, but DescribeEntity requires a real
// connection) and GetEntityRecords' native-catalog path.
func TestRealClient_DescribeEntity(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
		ConnectionInput: &types.ConnectionInput{
			Name:                 aws.String("crm-conn"),
			ConnectionType:       types.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://host/db"},
		},
	})
	require.NoError(t, err)

	fields, err := client.DescribeEntity(ctx, &gluesdk.DescribeEntityInput{
		ConnectionName: aws.String("crm-conn"),
		EntityName:     aws.String("Account"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, fields.Fields)

	found := false

	for _, f := range fields.Fields {
		if aws.ToString(f.FieldName) == "Id" {
			found = true
		}
	}

	assert.True(t, found, "Account entity must expose an Id field")

	_, err = client.DescribeEntity(ctx, &gluesdk.DescribeEntityInput{
		ConnectionName: aws.String("crm-conn"),
		EntityName:     aws.String("NoSuchEntity"),
	})
	require.Error(t, err)

	_, err = client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("entitydb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("entitydb"),
		TableInput: &types.TableInput{
			Name: aws.String("entitytbl"),
			StorageDescriptor: &types.StorageDescriptor{
				Columns: []types.Column{{Name: aws.String("id"), Type: aws.String("int")}},
			},
		},
	})
	require.NoError(t, err)

	records, err := client.GetEntityRecords(ctx, &gluesdk.GetEntityRecordsInput{
		EntityName: aws.String("entitydb.entitytbl"),
		Limit:      aws.Int64(10),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, records.Records)
}

// TestRealClient_DeleteTableVersion drives DeleteTableVersion.
func TestRealClient_DeleteTableVersion(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("vdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("vdb"),
		TableInput:   &types.TableInput{Name: aws.String("vtbl")},
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &gluesdk.UpdateTableInput{
		DatabaseName: aws.String("vdb"),
		TableInput: &types.TableInput{
			Name:        aws.String("vtbl"),
			Description: aws.String("v2"),
		},
	})
	require.NoError(t, err)

	versions, err := client.GetTableVersions(ctx, &gluesdk.GetTableVersionsInput{
		DatabaseName: aws.String("vdb"),
		TableName:    aws.String("vtbl"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, versions.TableVersions)

	oldestVersionID := versions.TableVersions[0].VersionId

	_, err = client.DeleteTableVersion(ctx, &gluesdk.DeleteTableVersionInput{
		DatabaseName: aws.String("vdb"),
		TableName:    aws.String("vtbl"),
		VersionId:    oldestVersionID,
	})
	require.NoError(t, err)

	_, err = client.GetTableVersion(ctx, &gluesdk.GetTableVersionInput{
		DatabaseName: aws.String("vdb"),
		TableName:    aws.String("vtbl"),
		VersionId:    oldestVersionID,
	})
	require.Error(t, err)
}
