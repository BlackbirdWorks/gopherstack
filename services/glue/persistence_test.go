package glue_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestInMemoryBackend_SnapshotRestore_FullState seeds one instance of every
// resource collection on InMemoryBackend -- both the store.Table-backed
// collections converted in Phase 3.3 and the collections left as plain maps
// (see the comment above registerAllTables in store_setup.go) -- then
// verifies a Snapshot/Restore round trip into a fresh backend reproduces the
// exact same state. This is the persistence-safety regression test required
// by the conversion: every map that was persisted before the refactor must
// still be persisted identically afterward.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	orig := glue.NewInMemoryBackend("123456789012", "us-east-1")
	seedFullState(t, orig)

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	verifyFullState(t, restored)
}

func seedFullState(t *testing.T, b *glue.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateDatabase(glue.DatabaseInput{Name: "db1"}, map[string]string{"k": "v"})
	require.NoError(t, err)
	_, err = b.CreateTable("db1", glue.TableInput{
		Name:          "tbl1",
		PartitionKeys: []glue.Column{{Name: "year"}},
	})
	require.NoError(t, err)
	_, errs := b.BatchCreatePartition("db1", "tbl1", []glue.PartitionInput{{Values: []string{"2024"}}})
	require.Empty(t, errs)
	b.AddTableVersionInternal("db1", "tbl1", &glue.TableVersion{VersionID: "1"})
	_, err = b.CreateCrawler("crawler1", "role1", "db1", glue.CrawlerTarget{}, nil)
	require.NoError(t, err)
	require.NoError(t, b.StartCrawler("crawler1")) // populates raw crawlHistory
	_, err = b.CreateJob(glue.Job{Name: "job1", Role: "role1", Command: glue.JobCommand{Name: "glueetl"}})
	require.NoError(t, err)
	_, err = b.StartJobRun("job1", nil) // populates raw jobRuns + jobBookmarks
	require.NoError(t, err)
	_, err = b.CreateConnection("conn1", "JDBC", nil, nil)
	require.NoError(t, err)
	_, err = b.CreateBlueprint("bp1", "s3://bucket/bp1", "", nil)
	require.NoError(t, err)
	_, err = b.CreateCustomEntityType("cet1", "regex", nil)
	require.NoError(t, err)
	b.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "dqr1"})
	_, err = b.CreateDevEndpoint("dep1", glue.DevEndpointInput{}, "arn:aws:iam::123456789012:role/dep-role", nil)
	require.NoError(t, err)
	_, err = b.CreateDataQualityRuleset("ruleset1", "rules", nil)
	require.NoError(t, err)
	_, err = b.StartDataQualityRulesetEvaluationRun([]string{"ruleset1"})
	require.NoError(t, err)
	_, err = b.CreateTrigger(glue.Trigger{Name: "trig1"}, nil)
	require.NoError(t, err)
	_, err = b.CreateWorkflow(glue.Workflow{Name: "wf1"}, nil)
	require.NoError(t, err)
	_, err = b.StartWorkflowRun("wf1") // populates raw workflowRuns
	require.NoError(t, err)
	require.NoError(t, b.CreateClassifier(glue.Classifier{GrokClassifier: &glue.GrokClassifier{Name: "clf1"}}))
	_, err = b.CreateRegistry("reg1", "desc", nil)
	require.NoError(t, err)
	_, _, err = b.CreateSchema("reg1", "schema1", "AVRO", "NONE", "desc", "", nil)
	require.NoError(t, err)
	_, err = b.RegisterSchemaVersion(
		"reg1", "schema1", `{"type":"record","name":"v1","fields":[]}`,
	) // populates raw schemaVersions
	require.NoError(t, err)
	_, err = b.CreateUserDefinedFunction("db1", glue.UserDefinedFunction{FunctionName: "udf1"}, nil)
	require.NoError(t, err)
	_, err = b.CreateSecurityConfiguration("sc1", glue.EncryptionConfiguration{})
	require.NoError(t, err)
	_, err = b.CreateSession("sess1", "role1", glue.SessionCommand{Name: "glueetl"}, glue.Session{})
	require.NoError(t, err)
	_, err = b.RunStatement("sess1", "print(1)") // populates raw sessionStatements
	require.NoError(t, err)
	require.NoError(t, b.CreateTableOptimizer("cat1", "db1", "tbl1", "compaction", glue.TableOptimizerConfiguration{}))
	require.NoError(t, b.UpdateColumnStatisticsForTable("db1", "tbl1", []*glue.ColumnStatistics{ // raw tableColumnStats
		{ColumnName: "col1"},
	}))
	require.NoError(t, b.UpdateColumnStatisticsForPartition("db1", "tbl1", []string{"2024"}, // raw partitionColumnStats
		[]*glue.ColumnStatistics{{ColumnName: "col1"}}))
	_, err = b.PutResourcePolicy( // raw resourcePolicies
		"policy-doc", "arn:aws:glue:us-east-1:123456789012:catalog", "", "", "",
	)
	require.NoError(t, err)
	mlTransform, err := b.CreateMLTransform("mlt1", "desc", "role1", nil, glue.MLTransformParameter{}, nil)
	require.NoError(t, err)
	_, err = b.StartMLEvaluationTaskRun(mlTransform.TransformID)
	require.NoError(t, err)
	require.NoError(t, b.CreateCatalog("cat1", "name1", "desc", nil))
	// raw catalogEncryptionSettings
	require.NoError(t, b.PutDataCatalogEncryptionSettings("cat1", glue.DataCatalogEncryptionSettings{}))
	require.NoError(t, b.ImportCatalogToGlue("cat1")) // raw catalogImports
	// raw schemaVersionMetadata
	require.NoError(t, b.PutSchemaVersionMetadata("sv1", "key1", "val1"))
	// raw partitionIndexes
	require.NoError(t, b.CreatePartitionIndex("db1", "tbl1", glue.PartitionIndex{
		IndexName: "idx1",
		Keys:      []string{"year"},
	}))
	_, err = b.CreateUsageProfile("up1", "desc", nil)
	require.NoError(t, err)
	_, err = b.StartBlueprintRun("bp1")
	require.NoError(t, err)
	_, err = b.StartDataQualityRuleRecommendationRun("s3://bucket/path")
	require.NoError(t, err)
	_, err = b.CreateColumnStatisticsTaskSettings("db1", "tbl1", "role1", nil)
	require.NoError(t, err)
	_, err = b.StartColumnStatisticsTaskRun("db1", "tbl1")
	require.NoError(t, err)
	_, err = b.StartMaterializedViewRefreshTaskRun("db1", "view1")
	require.NoError(t, err)
	_, err = b.CreateIntegration(
		"int1",
		"arn:aws:s3:::source-bucket",
		"arn:aws:redshift:us-east-1:123456789012:cluster/target",
		nil,
	)
	require.NoError(t, err)
	_, err = b.CreateIntegrationResourceProperty("arn:aws:glue:resource1", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.CreateIntegrationTableProperties("arn:aws:glue:resource1", "tbl1", nil, nil))
	b.PutDataQualityStatisticAnnotation("profile1", "stat1", "INCLUDE")
	_, err = b.CreateGlueIdentityCenterConfiguration("instance1")
	require.NoError(t, err)
	_, err = b.RegisterConnectionType("custom1", "a custom connector", fullRegisterConnectionTypeSpec())
	require.NoError(t, err)

	// Business glossary / asset catalog (parity-4).
	glossary, err := b.CreateGlossary("Finance", "money terms")
	require.NoError(t, err)
	term, err := b.CreateGlossaryTerm(glossary.ID, "Revenue", "money in", "")
	require.NoError(t, err)
	_, err = b.PutFormType("TableSchema", `{"type":"object"}`)
	require.NoError(t, err)
	_, err = b.PutAssetType("Table", map[string]glue.AssetTypeFormReference{
		"main": {FormTypeIdentifier: "TableSchema"},
	})
	require.NoError(t, err)
	_, err = b.PutAsset("asset1", "orders", "", "Table", nil)
	require.NoError(t, err)
	_, err = b.AssociateGlossaryTerms("asset1", []string{term.ID})
	require.NoError(t, err)
	require.NoError(t, b.PutAttachment( // raw iterableFormItems
		"asset1", "col-notes", "TableSchema", `{"pii":true}`, "columns", "order_id",
	))
}

func verifyFullState(t *testing.T, b *glue.InMemoryBackend) {
	t.Helper()

	assert.Equal(t, 1, glue.DatabaseCount(b))
	assert.Equal(t, 1, glue.TableCount(b))
	assert.Equal(t, 1, glue.PartitionCount(b))
	assert.Equal(t, 1, glue.TableVersionCount(b))
	assert.Equal(t, 1, glue.CrawlerCount(b))
	assert.Equal(t, 1, glue.JobCount(b))
	assert.Equal(t, 1, glue.JobRunCount(b))
	assert.Equal(t, 1, glue.ConnectionCount(b))
	assert.Equal(t, 1, glue.BlueprintCount(b))
	assert.Equal(t, 1, glue.CustomEntityTypeCount(b))
	assert.Equal(t, 1, glue.DataQualityResultCount(b))
	assert.Equal(t, 1, glue.DevEndpointCount(b))
	assert.Equal(t, 1, glue.DataQualityRulesetCount(b))
	assert.Equal(t, 1, glue.DataQualityEvalRunCount(b))
	assert.Equal(t, 1, glue.MLTaskRunCount(b))
	assert.Equal(t, 1, glue.CustomConnectionTypeCountForTest(b))

	trig, err := b.GetTrigger("trig1")
	require.NoError(t, err)
	assert.Equal(t, "trig1", trig.Name)

	wf, err := b.GetWorkflow("wf1", false)
	require.NoError(t, err)
	assert.Equal(t, "wf1", wf.Name)
	wfRuns, err := b.GetWorkflowRuns("wf1")
	require.NoError(t, err)
	assert.Len(t, wfRuns, 1)

	clf, err := b.GetClassifier("clf1")
	require.NoError(t, err)
	require.NotNil(t, clf.GrokClassifier)
	assert.Equal(t, "clf1", clf.GrokClassifier.Name)

	reg, err := b.DescribeRegistry("reg1")
	require.NoError(t, err)
	assert.Equal(t, "reg1", reg.Name)

	sch, err := b.DescribeSchema("reg1", "schema1")
	require.NoError(t, err)
	assert.Equal(t, "schema1", sch.SchemaName)
	schVersions := b.ListSchemaVersions("reg1", "schema1")
	require.Len(t, schVersions, 1)
	assert.JSONEq(t, `{"type":"record","name":"v1","fields":[]}`, schVersions[0].SchemaDefinition)

	udf, err := b.GetUserDefinedFunction("db1", "udf1")
	require.NoError(t, err)
	assert.Equal(t, "udf1", udf.FunctionName)

	_, err = b.GetSecurityConfiguration("sc1")
	require.NoError(t, err)

	sess, err := b.GetSession("sess1")
	require.NoError(t, err)
	assert.Equal(t, "sess1", sess.SessionID)
	stmts, err := b.GetStatements("sess1")
	require.NoError(t, err)
	assert.Len(t, stmts, 1)

	to, err := b.GetTableOptimizer("db1", "tbl1", "compaction")
	require.NoError(t, err)
	assert.Equal(t, "compaction", to.Type)

	colStats, err := b.GetColumnStatisticsForTable("db1", "tbl1", nil)
	require.NoError(t, err)
	require.Len(t, colStats, 1)
	assert.Equal(t, "col1", colStats[0].ColumnName)

	partColStats, err := b.GetColumnStatisticsForPartition("db1", "tbl1", []string{"2024"}, nil)
	require.NoError(t, err)
	require.Len(t, partColStats, 1)

	policy, hash, err := b.GetResourcePolicy("arn:aws:glue:us-east-1:123456789012:catalog")
	require.NoError(t, err)
	assert.Equal(t, "policy-doc", policy)
	assert.NotEmpty(t, hash)

	mlTransforms := b.GetMLTransforms()
	require.Len(t, mlTransforms, 1)
	assert.Equal(t, "mlt1", mlTransforms[0].Name)
	mlTaskRuns, err := b.GetMLTaskRuns(mlTransforms[0].TransformID)
	require.NoError(t, err)
	assert.Len(t, mlTaskRuns, 1)

	cat, err := b.GetCatalog("cat1")
	require.NoError(t, err)
	assert.Equal(t, "cat1", cat.CatalogID)
	encSettings, err := b.GetDataCatalogEncryptionSettings("cat1")
	require.NoError(t, err)
	assert.NotNil(t, encSettings)
	assert.True(t, b.GetCatalogImportStatus("cat1").ImportCompleted)
	assert.Equal(t, map[string]string{"key1": "val1"}, b.QuerySchemaVersionMetadata("sv1"))

	idxs, err := b.GetPartitionIndexes("db1", "tbl1")
	require.NoError(t, err)
	require.Len(t, idxs, 1)
	assert.Equal(t, "idx1", idxs[0].IndexName)

	_, err = b.GetUsageProfile("up1")
	require.NoError(t, err)

	bpRuns := b.GetBlueprintRuns("bp1")
	assert.Len(t, bpRuns, 1)

	assert.Len(t, b.ListDataQualityRuleRecommendationRuns(), 1)

	_, err = b.GetColumnStatisticsTaskSettings("db1", "tbl1")
	require.NoError(t, err)
	assert.Len(t, b.ListColumnStatisticsTaskRuns(), 1)
	assert.Len(t, b.ListMaterializedViewRefreshTaskRuns(), 1)
	assert.Len(t, b.ListIntegrations(), 1)

	_, err = b.GetIntegrationResourceProperty("arn:aws:glue:resource1")
	require.NoError(t, err)
	_, err = b.GetIntegrationTableProperties("arn:aws:glue:resource1", "tbl1")
	require.NoError(t, err)

	annotations := b.ListDataQualityStatisticAnnotations("profile1", "stat1")
	require.Len(t, annotations, 1)
	assert.Equal(t, "INCLUDE", annotations[0].Inclusion)

	idConfig, err := b.GetGlueIdentityCenterConfiguration()
	require.NoError(t, err)
	assert.Equal(t, "instance1", idConfig.InstanceARN)

	connType, err := b.DescribeConnectionType("custom1")
	require.NoError(t, err)
	assert.Equal(t, "CUSTOM1", connType.ConnectionType)

	// Cascade-delete sanity check: the AddPartitionInternal/AddTableVersionInternal
	// identity-fix (dbName/tableName stamped onto the stored value so store.Table's
	// key is a pure function of the value, matching the pre-conversion raw-map
	// behavior) must still let BatchDeleteTable cascade to the restored partition
	// and table version exactly as it did before persistence.
	tableErrs := b.BatchDeleteTable("db1", []string{"tbl1"})
	assert.Empty(t, tableErrs)
	assert.Equal(t, 0, glue.PartitionCount(b))
	assert.Equal(t, 0, glue.TableVersionCount(b))

	// Business glossary / asset catalog (parity-4): verify the restored
	// state includes the glossary/term/asset-type/form-type/asset chain AND
	// the raw (non-store.Table) iterableFormItems map survived the round
	// trip, since PutAttachment's IterableFormName/ItemIdentifier path is
	// the one write this family routes through a plain map instead of a
	// store.Table (see InMemoryBackend.iterableFormItems in store.go).
	glossaries := b.ListGlossaries()
	require.Len(t, glossaries, 1)
	assert.Equal(t, "Finance", glossaries[0].Name)

	terms, err := b.ListGlossaryTerms(glossaries[0].ID)
	require.NoError(t, err)
	require.Len(t, terms, 1)
	assert.Equal(t, "Revenue", terms[0].Name)

	_, err = b.GetAssetType("Table")
	require.NoError(t, err)
	_, err = b.GetFormType("TableSchema")
	require.NoError(t, err)

	asset, err := b.GetAsset("asset1")
	require.NoError(t, err)
	assert.Equal(t, "orders", asset.Name)
	assert.Equal(t, "Table", asset.AssetTypeID)
	assert.Contains(t, asset.GlossaryTerms, terms[0].ID)

	items, err := b.ListIterableForms("asset1", "columns")
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, "order_id", items[0].ItemID)

	batchItems, batchErrs, err := b.BatchGetIterableForms("asset1", "columns", []string{"order_id"})
	require.NoError(t, err)
	assert.Empty(t, batchErrs)
	require.Len(t, batchItems, 1)
	assert.Equal(t, `{"pii":true}`, batchItems[0].Attachments["col-notes"].Content)
}

// TestInMemoryBackend_Restore_VersionMismatch verifies that Restore discards
// (rather than partially decodes) a snapshot whose Version does not match the
// backend's current glueSnapshotVersion, leaving the backend in its empty
// starting state instead of erroring or corrupting state.
func TestInMemoryBackend_Restore_VersionMismatch(t *testing.T) {
	t.Parallel()

	orig := glue.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := orig.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
	require.NoError(t, err)

	snap := orig.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Corrupt the version field so Restore hits the mismatch branch, without
	// hardcoding the rest of the snapshot's shape.
	var generic map[string]any
	require.NoError(t, json.Unmarshal(snap, &generic))
	require.Contains(t, generic, "version")
	generic["version"] = 999

	mutated, err := json.Marshal(generic)
	require.NoError(t, err)

	restored := glue.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), mutated))

	assert.Equal(t, 0, glue.DatabaseCount(restored))
}
