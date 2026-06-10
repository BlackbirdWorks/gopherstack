package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ============================================================
// VPC config v2 — Ipv6AllowedForDualStack
// ============================================================

func TestBatch3_VpcConfig_Ipv6AllowedForDualStack_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"],
			"SecurityGroupIds":["sg-1"],
			"Ipv6AllowedForDualStack":true
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

func TestBatch3_VpcConfig_Ipv6AllowedForDualStack_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "vpc6-update-fn")

	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/vpc6-update-fn/configuration",
		`{"VpcConfig":{"SubnetIds":["subnet-2"],"Ipv6AllowedForDualStack":true}}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

func TestBatch3_VpcConfig_Ipv6AllowedForDualStack_NotSet_Omitted(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-omit-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"]
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	// omitempty: nil pointer → field absent in JSON
	_, hasField := vpc["Ipv6AllowedForDualStack"]
	assert.False(t, hasField)
}

func TestBatch3_VpcConfig_Ipv6AllowedForDualStack_GetConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-get-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"],
			"SecurityGroupIds":["sg-1"],
			"Ipv6AllowedForDualStack":true
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/vpc6-get-fn/configuration", "")
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

// ============================================================
// Amazon MSK event source
// ============================================================

func TestBatch3_ESM_MSK_Topics_And_ConsumerGroup(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "msk-fn")

	body := `{
		"FunctionName":"msk-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
		"Topics":["my-topic"],
		"StartingPosition":"TRIM_HORIZON",
		"AmazonManagedKafkaEventSourceConfig":{"ConsumerGroupId":"my-group"}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	topics, _ := out["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "my-topic", topics[0])

	cfg, _ := out["AmazonManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, cfg)
	assert.Equal(t, "my-group", cfg["ConsumerGroupId"])
}

func TestBatch3_ESM_MSK_Get_PreservesConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "msk-get-fn")

	body := `{
		"FunctionName":"msk-get-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
		"Topics":["topic-a"],
		"AmazonManagedKafkaEventSourceConfig":{"ConsumerGroupId":"group-1"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id, _ := created["UUID"].(string)
	require.NotEmpty(t, id)

	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))

	topics, _ := got["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "topic-a", topics[0])

	cfg, _ := got["AmazonManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, cfg)
	assert.Equal(t, "group-1", cfg["ConsumerGroupId"])
}

// ============================================================
// Self-managed Apache Kafka event source
// ============================================================

func TestBatch3_ESM_SelfManagedKafka_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "smk-fn")

	// SelfManaged Kafka has no EventSourceArn in real AWS; use a placeholder to satisfy backend validation.
	body := `{
		"FunctionName":"smk-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/smk/xyz",
		"Topics":["smk-topic"],
		"SelfManagedEventSource":{
			"Endpoints":{"KAFKA_BOOTSTRAP_SERVERS":["broker1:9092","broker2:9092"]}
		},
		"SelfManagedKafkaEventSourceConfig":{"ConsumerGroupId":"smk-group"},
		"SourceAccessConfigurations":[
			{"Type":"SASL_SCRAM_512_AUTH","URI":"arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-secret"}
		]
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	topics, _ := out["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "smk-topic", topics[0])

	sme, _ := out["SelfManagedEventSource"].(map[string]any)
	require.NotNil(t, sme)
	endpoints, _ := sme["Endpoints"].(map[string]any)
	brokers, _ := endpoints["KAFKA_BOOTSTRAP_SERVERS"].([]any)
	assert.Len(t, brokers, 2)

	smk, _ := out["SelfManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, smk)
	assert.Equal(t, "smk-group", smk["ConsumerGroupId"])

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	require.Len(t, sacs, 1)
	sac, _ := sacs[0].(map[string]any)
	assert.Equal(t, "SASL_SCRAM_512_AUTH", sac["Type"])
	assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-secret", sac["URI"])
}

func TestBatch3_ESM_SelfManagedKafka_UpdateSourceAccessConfigs(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "smk-upd-fn")

	createBody := `{
		"FunctionName":"smk-upd-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/smk/upd",
		"Topics":["t1"],
		"SelfManagedKafkaEventSourceConfig":{"ConsumerGroupId":"g1"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id := created["UUID"].(string)

	updateBody := `{
		"SourceAccessConfigurations":[
			{"Type":"VPC_SUBNET","URI":"subnet-abc"},
			{"Type":"VPC_SECURITY_GROUP","URI":"sg-abc"}
		]
	}`
	updateRec := callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/event-source-mappings/"+id, updateBody)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&out))

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	assert.Len(t, sacs, 2)
}

// ============================================================
// Amazon MQ / Queues
// ============================================================

func TestBatch3_ESM_AmazonMQ_Queues(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "mq-fn")

	body := `{
		"FunctionName":"mq-fn",
		"EventSourceArn":"arn:aws:mq:us-east-1:000000000000:broker:my-broker:b-abc",
		"Queues":["TestQueue"],
		"SourceAccessConfigurations":[
			{"Type":"BASIC_AUTH","URI":"arn:aws:secretsmanager:us-east-1:000000000000:secret:mq-creds"}
		]
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	queues, _ := out["Queues"].([]any)
	require.Len(t, queues, 1)
	assert.Equal(t, "TestQueue", queues[0])

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	require.Len(t, sacs, 1)
}

// ============================================================
// Amazon DocumentDB event source
// ============================================================

func TestBatch3_ESM_DocumentDB_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-fn")

	body := `{
		"FunctionName":"docdb-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:my-docdb-cluster",
		"StartingPosition":"LATEST",
		"DocumentDBEventSourceConfig":{
			"DatabaseName":"mydb",
			"CollectionName":"mycoll",
			"FullDocument":"UpdateLookup"
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	docdb, _ := out["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "mydb", docdb["DatabaseName"])
	assert.Equal(t, "mycoll", docdb["CollectionName"])
	assert.Equal(t, "UpdateLookup", docdb["FullDocument"])
}

func TestBatch3_ESM_DocumentDB_Get_PreservesConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-get-fn")

	body := `{
		"FunctionName":"docdb-get-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"DocumentDBEventSourceConfig":{
			"DatabaseName":"testdb",
			"FullDocument":"Default"
		}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id := created["UUID"].(string)

	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))

	docdb, _ := got["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "testdb", docdb["DatabaseName"])
	assert.Equal(t, "Default", docdb["FullDocument"])
}

func TestBatch3_ESM_DocumentDB_NoCollectionName_Omitted(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-nocoll-fn")

	body := `{
		"FunctionName":"docdb-nocoll-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:c",
		"DocumentDBEventSourceConfig":{"DatabaseName":"db1"}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	docdb, _ := out["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "db1", docdb["DatabaseName"])
	_, hasCollection := docdb["CollectionName"]
	assert.False(t, hasCollection)
}

// ============================================================
// AccountUsage — FunctionCount and TotalCodeSize
// ============================================================

func TestBatch3_AccountUsage_FunctionCount_Increments(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec0 := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec0.Code)

	var s0 lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec0.Body).Decode(&s0))
	require.NotNil(t, s0.AccountUsage)
	assert.Equal(t, 0, s0.AccountUsage.FunctionCount)

	createFunctionForTest(t, h, "usage-fn-1")
	createFunctionForTest(t, h, "usage-fn-2")

	rec2 := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec2.Code)

	var s2 lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&s2))
	require.NotNil(t, s2.AccountUsage)
	assert.Equal(t, 2, s2.AccountUsage.FunctionCount)
}

func TestBatch3_AccountUsage_AccountLimit_Fields(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2016-08-19/account-settings", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var s lambda.AccountSettingsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&s))
	require.NotNil(t, s.AccountLimit)
	assert.Positive(t, s.AccountLimit.TotalCodeSize)
	assert.Positive(t, s.AccountLimit.ConcurrentExecutions)
	assert.Positive(t, s.AccountLimit.UnreservedConcurrentExecutions)
	assert.Positive(t, s.AccountLimit.CodeSizeUnzipped)
	assert.Positive(t, s.AccountLimit.CodeSizeZipped)
}

// ============================================================
// Provisioned concurrency — List and status accuracy
// ============================================================

func TestBatch3_ProvisionedConcurrency_StatusReady(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-status-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}))

	_, err := bk.PublishVersion("prov-status-fn", "")
	require.NoError(t, err)

	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-status-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(5), out["RequestedProvisionedConcurrentExecutions"], 0)
	assert.InDelta(t, float64(5), out["AllocatedProvisionedConcurrentExecutions"], 0)
	assert.InDelta(t, float64(5), out["AvailableProvisionedConcurrentExecutions"], 0)
	assert.Equal(t, "READY", out["Status"])
}

func TestBatch3_ProvisionedConcurrency_List_IncludesAllQualifiers(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-list-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
	}))

	_, err := bk.PublishVersion("prov-list-fn", "")
	require.NoError(t, err)
	_, err = bk.PublishVersion("prov-list-fn", "")
	require.NoError(t, err)

	callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":3}`,
	)
	callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency?Qualifier=2",
		`{"ProvisionedConcurrentExecutions":7}`,
	)

	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/prov-list-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var out lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	assert.Len(t, out.ProvisionedConcurrencyConfigs, 2)
}

// ============================================================
// Function URL config — Cors round-trip
// ============================================================

func TestBatch3_FunctionURL_CorsAllowOrigins_RoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-cors-fn")

	createRec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/functions/url-cors-fn/url",
		`{"AuthType":"NONE","Cors":{"AllowOrigins":["https://example.com"],"AllowMethods":["GET","POST"]}}`,
	)
	require.Equal(t, http.StatusCreated, createRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/url-cors-fn/url", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	cors, _ := out["Cors"].(map[string]any)
	require.NotNil(t, cors)
	origins, _ := cors["AllowOrigins"].([]any)
	assert.Len(t, origins, 1)
	assert.Equal(t, "https://example.com", origins[0])
}

// ============================================================
// SnapStart — OptimizationStatus in GetFunctionConfiguration
// ============================================================

func TestBatch3_SnapStart_GetConfiguration_OptimizationStatus(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"snap-cfg-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"SnapStart":{"ApplyOn":"PublishedVersions"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	cfgRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/snap-cfg-fn/configuration", "")
	require.Equal(t, http.StatusOK, cfgRec.Code)

	var cfg map[string]any
	require.NoError(t, json.NewDecoder(cfgRec.Body).Decode(&cfg))
	snap, _ := cfg["SnapStart"].(map[string]any)
	require.NotNil(t, snap)
	assert.Equal(t, "PublishedVersions", snap["ApplyOn"])
	assert.Equal(t, "On", snap["OptimizationStatus"])
}

// ============================================================
// SourceAccessConfiguration — Type and URI shape
// ============================================================

func TestBatch3_SourceAccessConfiguration_TypeAndURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sacType string
		uri     string
		name    string
	}{
		{
			name:    "basic_auth",
			sacType: "BASIC_AUTH",
			uri:     "arn:aws:secretsmanager:us-east-1:000000000000:secret:broker-creds",
		},
		{
			name:    "sasl_scram_512",
			sacType: "SASL_SCRAM_512_AUTH",
			uri:     "arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-scram512",
		},
		{
			name:    "vpc_subnet",
			sacType: "VPC_SUBNET",
			uri:     "subnet-0123456789",
		},
		{
			name:    "vpc_security_group",
			sacType: "VPC_SECURITY_GROUP",
			uri:     "sg-0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "sac-"+tt.name+"-fn")

			body := `{
				"FunctionName":"sac-` + tt.name + `-fn",
				"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/c/x",
				"Topics":["t"],
				"SourceAccessConfigurations":[{"Type":"` + tt.sacType + `","URI":"` + tt.uri + `"}]
			}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			sacs, _ := out["SourceAccessConfigurations"].([]any)
			require.Len(t, sacs, 1)
			sac, _ := sacs[0].(map[string]any)
			assert.Equal(t, tt.sacType, sac["Type"])
			assert.Equal(t, tt.uri, sac["URI"])
		})
	}
}
