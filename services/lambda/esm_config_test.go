package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// --- EventSourceMapping edge cases ---

func TestESM_HTTPLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "esm-fn"
	createFunctionForTest(t, h, fnName)

	// Create ESM
	rec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"FunctionName":"esm-fn",`+
			`"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",`+
			`"StartingPosition":"LATEST"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	uuid := created["UUID"].(string)

	// List by function name
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-fn", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), uuid)

	// Get ESM
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update ESM
	rec = callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"BatchSize":50}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete ESM
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestESM_BatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-batch-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":"esm-batch-fn",
		"StartingPosition":"LATEST",
		"BatchSize":100,
		"MaximumBatchingWindowInSeconds":60
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(60), out["MaximumBatchingWindowInSeconds"], 0)
}

func TestESM_TumblingWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-tumble-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/tumble-stream",
		"FunctionName":"esm-tumble-fn",
		"StartingPosition":"LATEST",
		"TumblingWindowInSeconds":300
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(300), out["TumblingWindowInSeconds"], 0)
}

func TestESM_FilterCriteria(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-filter-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-queue",
		"FunctionName":"esm-filter-fn",
		"FilterCriteria":{"Filters":[{"Pattern":"{\"source\":[\"myapp\"]}"}]}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	fc, ok := out["FilterCriteria"].(map[string]any)
	require.True(t, ok)
	filters := fc["Filters"].([]any)
	require.Len(t, filters, 1)
}

func TestESM_BisectBatchOnError(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-bisect-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/bisect-stream",
		"FunctionName":"esm-bisect-fn",
		"StartingPosition":"LATEST",
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestESM_MaximumRecordAge(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-recage-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/age-stream",
		"FunctionName":"esm-recage-fn",
		"StartingPosition":"LATEST",
		"MaximumRecordAgeInSeconds":3600
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(3600), out["MaximumRecordAgeInSeconds"], 0)
}

func TestESM_ParallelizationFactor(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-parallel-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/parallel-stream",
		"FunctionName":"esm-parallel-fn",
		"StartingPosition":"LATEST",
		"ParallelizationFactor":5
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(5), out["ParallelizationFactor"], 0)
}

func TestESM_DestinationConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-dest-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/dest-stream",
		"FunctionName":"esm-dest-fn",
		"StartingPosition":"LATEST",
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:dead-letter"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	dc, ok := out["DestinationConfig"].(map[string]any)
	require.True(t, ok)
	onFail := dc["OnFailure"].(map[string]any)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:dead-letter", onFail["Destination"])
}

func TestESM_UpdateBatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-upd-window-fn")

	createBody := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/upd-stream",
		"FunctionName":"esm-upd-window-fn",
		"StartingPosition":"LATEST"
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"MaximumBatchingWindowInSeconds":30}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	assert.InDelta(t, float64(30), updated["MaximumBatchingWindowInSeconds"], 0)
}

func TestESM_SQS_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-sqs-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-sqs-queue",
		"FunctionName":"esm-sqs-fn",
		"BatchSize":10,
		"MaximumBatchingWindowInSeconds":5,
		"FilterCriteria":{"Filters":[{"Pattern":"{\"body\":{\"type\":[\"order\"]}}"}]},
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:sqs-dlq"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(10), out["BatchSize"], 0)
	assert.InDelta(t, float64(5), out["MaximumBatchingWindowInSeconds"], 0)
	assert.NotNil(t, out["FilterCriteria"])
	assert.NotNil(t, out["DestinationConfig"])
	assert.Equal(t, "Enabled", out["State"])
}

func TestESM_DDB_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-ddb-fn")

	body := `{
		"EventSourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		"FunctionName":"esm-ddb-fn",
		"StartingPosition":"LATEST",
		"BatchSize":50,
		"BisectBatchOnFunctionError":true,
		"MaximumRetryAttempts":3
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(50), out["BatchSize"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
	assert.InDelta(t, float64(3), out["MaximumRetryAttempts"], 0)
}

func TestESM_Kinesis_AllWindowOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-kin-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/full-stream",
		"FunctionName":"esm-kin-fn",
		"StartingPosition":"TRIM_HORIZON",
		"BatchSize":200,
		"MaximumBatchingWindowInSeconds":120,
		"TumblingWindowInSeconds":60,
		"MaximumRecordAgeInSeconds":7200,
		"MaximumRetryAttempts":5,
		"ParallelizationFactor":3,
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(200), out["BatchSize"], 0)
	assert.InDelta(t, float64(120), out["MaximumBatchingWindowInSeconds"], 0)
	assert.InDelta(t, float64(60), out["TumblingWindowInSeconds"], 0)
	assert.InDelta(t, float64(7200), out["MaximumRecordAgeInSeconds"], 0)
	assert.InDelta(t, float64(5), out["MaximumRetryAttempts"], 0)
	assert.InDelta(t, float64(3), out["ParallelizationFactor"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestESM_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-list-fn")

	for i := range 3 {
		body := fmt.Sprintf(`{
			"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:q-%d",
			"FunctionName":"esm-list-fn"
		}`, i)
		callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	}

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-list-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	mappings := out["EventSourceMappings"].([]any)
	assert.Len(t, mappings, 3)
}

func TestESM_EnableDisable(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-toggle-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:toggle-q","FunctionName":"esm-toggle-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	// Disable
	disabled := false
	_ = disabled
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":false}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updOut map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
	assert.Equal(t, "Disabled", updOut["State"])

	// Re-enable
	enableRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":true}`)
	require.Equal(t, http.StatusOK, enableRec.Code)

	var enableOut map[string]any
	require.NoError(t, json.NewDecoder(enableRec.Body).Decode(&enableOut))
	assert.Equal(t, "Enabled", enableOut["State"])
}

func TestESM_DeleteReturnsMapping(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-del-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:del-q","FunctionName":"esm-del-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	require.Equal(t, http.StatusOK, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(delRec.Body).Decode(&delOut))
	assert.Equal(t, uuid, delOut["UUID"])

	// Get after delete → 404
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestESM_MSK_Topics_And_ConsumerGroup(t *testing.T) {
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

func TestESM_MSK_Get_PreservesConfig(t *testing.T) {
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

func TestESM_SelfManagedKafka_Create(t *testing.T) {
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

func TestESM_SelfManagedKafka_UpdateSourceAccessConfigs(t *testing.T) {
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

func TestESM_AmazonMQ_Queues(t *testing.T) {
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

func TestESM_DocumentDB_Create(t *testing.T) {
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

func TestESM_DocumentDB_Get_PreservesConfig(t *testing.T) {
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

func TestESM_DocumentDB_NoCollectionName_Omitted(t *testing.T) {
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

func TestSourceAccessConfiguration_TypeAndURI(t *testing.T) {
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

// TestListESM_EventSourceArnFilter verifies ListEventSourceMappings
// filters by EventSourceArn query parameter.
func TestListESM_EventSourceArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    "",
			wantCount: 2,
		},
		{
			name:      "exact ARN match returns one",
			filter:    "arn:aws:sqs:us-east-1:000000000000:queue-a",
			wantCount: 1,
		},
		{
			name:      "non-matching ARN returns none",
			filter:    "arn:aws:sqs:us-east-1:000000000000:no-such-queue",
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "esm-filter-fn")

			// Seed two ESMs with different source ARNs.
			for _, queueSuffix := range []string{"queue-a", "queue-b"} {
				sourceARN := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", queueSuffix)
				_, esmErr := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
					EventSourceARN:   sourceARN,
					FunctionName:     "esm-filter-fn",
					StartingPosition: "TRIM_HORIZON",
					Enabled:          true,
				})
				require.NoError(t, esmErr)
			}

			listPath := "/2015-03-31/event-source-mappings/"
			if tc.filter != "" {
				listPath += "?EventSourceArn=" + url.QueryEscape(tc.filter)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, listPath, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			esms, _ := out["EventSourceMappings"].([]any)
			assert.Len(t, esms, tc.wantCount,
				"EventSourceMappings count mismatch for filter=%q", tc.filter)
		})
	}
}
