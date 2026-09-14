package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// assertRawObjectHasNoArn decodes body, walks decoded[path...] down to a
// JSON object and asserts it carries no "Arn" key. Returns the object so
// callers can chain further assertions on it.
func assertRawObjectHasNoArn(t *testing.T, body []byte, path ...string) map[string]any {
	t.Helper()

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))

	obj := decoded
	for i, key := range path {
		v, ok := obj[key]
		require.Truef(t, ok, "missing key %q in response", key)

		m, ok := v.(map[string]any)
		require.Truef(t, ok, "key %q is not a JSON object", key)

		if i == len(path)-1 {
			_, hasArn := m["Arn"]
			assert.Falsef(t, hasArn, "raw response leaked an Arn key at %v", path)

			return m
		}

		obj = m
	}

	return obj
}

// assertNoArnInArray decodes body, reads decoded[containerKey] as a JSON
// array and asserts no element carries an "Arn" key.
func assertNoArnInArray(t *testing.T, body []byte, containerKey string) {
	t.Helper()

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(body, &decoded))

	arr, ok := decoded[containerKey].([]any)
	require.Truef(t, ok, "missing array key %q in response", containerKey)
	require.NotEmpty(t, arr)

	for _, item := range arr {
		obj, itemOK := item.(map[string]any)
		require.True(t, itemOK)

		_, hasArn := obj["Arn"]
		assert.Falsef(t, hasArn, "array item under %q leaked an Arn key", containerKey)
	}
}

// TestWireARN_GetPathsOmitArn pins gopherstack-6vwds: Crawler, Job,
// Connection, Trigger, Workflow and Database all persist ARN
// (json:"Arn,omitempty", keyed by 865a0c12f's tag persistence) but none of
// the real Get*Output shapes carries an Arn member
// (aws-sdk-go-v2/service/glue@v1.157.0 types/types.go Crawler:2841,
// Job:6832, Connection:2117, Trigger:12584, Workflow:13103, Database:3337).
// Each subtest creates the resource, reads it back through the real SDK
// client (proving decode still works either way), then inspects the raw
// response body for a bare "Arn" key -- the assertion that fails without
// wire_arn.go's read-path twins (confirmed by reverting crawlerWire's shadow
// field in place: the crawler subtest fails with the key present).
func TestWireARN_GetPathsOmitArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, client *gluesdk.Client, h *glue.Handler)
		name string
	}{
		{name: "crawler", run: wireARNCrawlerCase},
		{name: "job", run: wireARNJobCase},
		{name: "connection", run: wireARNConnectionCase},
		{name: "trigger", run: wireARNTriggerCase},
		{name: "workflow", run: wireARNWorkflowCase},
		{name: "database", run: wireARNDatabaseCase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			h := glue.NewHandler(backend)
			client := newTestGlueClient(t, h)

			tt.run(t, client, h)
		})
	}
}

func wireARNCrawlerCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("wireleak-crawler-db")},
	})
	require.NoError(t, err)

	_, err = client.CreateCrawler(ctx, &gluesdk.CreateCrawlerInput{
		Name:         aws.String("wireleak-crawler"),
		Role:         aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		DatabaseName: aws.String("wireleak-crawler-db"),
		Targets: &types.CrawlerTargets{
			S3Targets: []types.S3Target{{Path: aws.String("s3://bucket/data/")}},
		},
	})
	require.NoError(t, err)

	got, err := client.GetCrawler(ctx, &gluesdk.GetCrawlerInput{Name: aws.String("wireleak-crawler")})
	require.NoError(t, err)
	require.NotNil(t, got.Crawler)
	assert.Equal(t, "wireleak-crawler", aws.ToString(got.Crawler.Name))

	rec := doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "wireleak-crawler"})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Crawler")
}

func wireARNJobCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name: aws.String("wireleak-job"),
		Role: aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		Command: &types.JobCommand{
			Name:           aws.String("glueetl"),
			ScriptLocation: aws.String("s3://bucket/script.py"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("wireleak-job")})
	require.NoError(t, err)
	require.NotNil(t, got.Job)
	assert.Equal(t, "wireleak-job", aws.ToString(got.Job.Name))

	rec := doGlueRequest(t, h, "GetJob", map[string]any{"JobName": "wireleak-job"})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Job")
}

func wireARNConnectionCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
		ConnectionInput: &types.ConnectionInput{
			Name:           aws.String("wireleak-conn"),
			ConnectionType: types.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{
				"JDBC_CONNECTION_URL": "jdbc:mysql://host/db",
				"USERNAME":            "u",
				"PASSWORD":            "p",
			},
		},
	})
	require.NoError(t, err)

	got, err := client.GetConnection(ctx, &gluesdk.GetConnectionInput{Name: aws.String("wireleak-conn")})
	require.NoError(t, err)
	require.NotNil(t, got.Connection)
	assert.Equal(t, "wireleak-conn", aws.ToString(got.Connection.Name))

	rec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "wireleak-conn"})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Connection")
}

func wireARNTriggerCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateTrigger(ctx, &gluesdk.CreateTriggerInput{
		Name:    aws.String("wireleak-trigger"),
		Type:    types.TriggerTypeOnDemand,
		Actions: []types.Action{{JobName: aws.String("some-job")}},
	})
	require.NoError(t, err)

	got, err := client.GetTrigger(ctx, &gluesdk.GetTriggerInput{Name: aws.String("wireleak-trigger")})
	require.NoError(t, err)
	require.NotNil(t, got.Trigger)
	assert.Equal(t, "wireleak-trigger", aws.ToString(got.Trigger.Name))

	rec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": "wireleak-trigger"})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Trigger")

	updated, err := client.UpdateTrigger(ctx, &gluesdk.UpdateTriggerInput{
		Name:          aws.String("wireleak-trigger"),
		TriggerUpdate: &types.TriggerUpdate{Description: aws.String("updated")},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.Trigger)

	rec = doGlueRequest(t, h, "UpdateTrigger", map[string]any{
		"Name":          "wireleak-trigger",
		"TriggerUpdate": map[string]any{"Description": "updated"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Trigger")
}

// wireARNWorkflowCase also covers the nested leak found beyond the six
// cited paths: workflowGraphLocked embeds a real *Trigger inside
// Graph.Nodes[].TriggerDetails.Trigger (workflow_graph.go), three levels
// below Workflow's own ARN shadow, so toWorkflowGraphWire has to rebuild the
// graph with triggerWire in place of each node's Trigger.
func wireARNWorkflowCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateWorkflow(ctx, &gluesdk.CreateWorkflowInput{Name: aws.String("wireleak-wf")})
	require.NoError(t, err)

	_, err = client.CreateTrigger(ctx, &gluesdk.CreateTriggerInput{
		Name:         aws.String("wireleak-wf-trigger"),
		Type:         types.TriggerTypeOnDemand,
		WorkflowName: aws.String("wireleak-wf"),
		Actions:      []types.Action{{JobName: aws.String("some-job")}},
	})
	require.NoError(t, err)

	got, err := client.GetWorkflow(ctx, &gluesdk.GetWorkflowInput{
		Name:         aws.String("wireleak-wf"),
		IncludeGraph: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, got.Workflow)
	assert.Equal(t, "wireleak-wf", aws.ToString(got.Workflow.Name))
	require.NotNil(t, got.Workflow.Graph)

	var typedTriggerNode *types.Node

	for i := range got.Workflow.Graph.Nodes {
		if got.Workflow.Graph.Nodes[i].Type == types.NodeTypeTrigger {
			typedTriggerNode = &got.Workflow.Graph.Nodes[i]
		}
	}

	require.NotNil(t, typedTriggerNode)
	require.NotNil(t, typedTriggerNode.TriggerDetails)
	require.NotNil(t, typedTriggerNode.TriggerDetails.Trigger)
	assert.Equal(t, "wireleak-wf-trigger", aws.ToString(typedTriggerNode.TriggerDetails.Trigger.Name))

	rec := doGlueRequest(t, h, "GetWorkflow", map[string]any{"Name": "wireleak-wf", "IncludeGraph": true})
	require.Equal(t, http.StatusOK, rec.Code)

	wf := assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Workflow")

	graph, ok := wf["Graph"].(map[string]any)
	require.True(t, ok)

	nodes, ok := graph["Nodes"].([]any)
	require.True(t, ok)

	var triggerNode map[string]any

	for _, item := range nodes {
		node, nodeOK := item.(map[string]any)
		require.True(t, nodeOK)

		if node["Type"] == "TRIGGER" {
			triggerNode = node
		}
	}

	require.NotNil(t, triggerNode, "no TRIGGER node found in raw graph")

	triggerDetails, ok := triggerNode["TriggerDetails"].(map[string]any)
	require.True(t, ok)

	nestedTrigger, ok := triggerDetails["Trigger"].(map[string]any)
	require.True(t, ok)

	_, hasNestedArn := nestedTrigger["Arn"]
	assert.False(t, hasNestedArn, "nested workflow-graph Trigger leaked an Arn key")
	assert.Equal(t, "wireleak-wf-trigger", nestedTrigger["Name"])
}

func wireARNDatabaseCase(t *testing.T, client *gluesdk.Client, h *glue.Handler) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("wireleak-db")},
	})
	require.NoError(t, err)

	got, err := client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{Name: aws.String("wireleak-db")})
	require.NoError(t, err)
	require.NotNil(t, got.Database)
	assert.Equal(t, "wireleak-db", aws.ToString(got.Database.Name))

	rec := doGlueRequest(t, h, "GetDatabase", map[string]any{"Name": "wireleak-db"})
	require.Equal(t, http.StatusOK, rec.Code)
	assertRawObjectHasNoArn(t, rec.Body.Bytes(), "Database")
}

// TestWireARN_ListBatchPathsOmitArn covers the plural/batch paths beyond
// the Get* paths the gopherstack-6vwds census cited: GetCrawlers,
// BatchGetCrawlers, GetJobs, BatchGetJobs, GetConnections, GetTriggers,
// BatchGetTriggers, BatchGetWorkflows and GetDatabases all marshal the same
// six structs directly and leak the same field if left unfixed.
func TestWireARN_ListBatchPathsOmitArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "get_crawlers", run: testWireARNGetCrawlers},
		{name: "batch_get_crawlers", run: testWireARNBatchGetCrawlers},
		{name: "get_jobs", run: testWireARNGetJobs},
		{name: "batch_get_jobs", run: testWireARNBatchGetJobs},
		{name: "get_connections", run: testWireARNGetConnections},
		{name: "get_triggers", run: testWireARNGetTriggers},
		{name: "batch_get_triggers", run: testWireARNBatchGetTriggers},
		{name: "batch_get_workflows", run: testWireARNBatchGetWorkflows},
		{name: "get_databases", run: testWireARNGetDatabases},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testWireARNGetCrawlers(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "lb-crawler", "Role": "role1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetCrawlers", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Crawlers")
}

func testWireARNBatchGetCrawlers(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateCrawler", map[string]any{"Name": "lb-bcrawler", "Role": "role1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchGetCrawlers", map[string]any{"CrawlerNames": []string{"lb-bcrawler"}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Crawlers")
}

func testWireARNGetJobs(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "lb-job", "Role": "role1",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetJobs", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Jobs")
}

func testWireARNBatchGetJobs(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "lb-bjob", "Role": "role1",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchGetJobs", map[string]any{"JobNames": []string{"lb-bjob"}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Jobs")
}

func testWireARNGetConnections(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name": "lb-conn", "ConnectionType": "JDBC",
			"ConnectionProperties": map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://host/db"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetConnections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "ConnectionList")
}

func testWireARNGetTriggers(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name": "lb-trigger", "Type": "ON_DEMAND",
		"Actions": []map[string]any{{"JobName": "some-job"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetTriggers", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Triggers")
}

func testWireARNBatchGetTriggers(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name": "lb-btrigger", "Type": "ON_DEMAND",
		"Actions": []map[string]any{{"JobName": "some-job"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchGetTriggers", map[string]any{"TriggerNames": []string{"lb-btrigger"}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Triggers")
}

func testWireARNBatchGetWorkflows(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "lb-wf"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchGetWorkflows", map[string]any{"Names": []string{"lb-wf"}})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "Workflows")
}

func testWireARNGetDatabases(t *testing.T) {
	t.Helper()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "lb-db"}})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "GetDatabases", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assertNoArnInArray(t, rec.Body.Bytes(), "DatabaseList")
}
