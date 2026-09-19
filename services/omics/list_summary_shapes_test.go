package omics_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	omicssdk "github.com/aws/aws-sdk-go-v2/service/omics"
	"github.com/aws/aws-sdk-go-v2/service/omics/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSummaries_MemberRoundTrips drives each op's Create/Start then List
// through the real aws-sdk-go-v2 client and asserts a member that was
// missing from the List summary (over-wide response class, gopherstack-dv4s)
// now round-trips.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("run workflow name", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name:      aws.String("wf-run-summary"),
			Engine:    types.WorkflowEngineWdl,
			RequestId: aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		_, err = client.StartRun(t.Context(), &omicssdk.StartRunInput{
			WorkflowId: wf.Id,
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
			OutputUri:  aws.String("s3://bucket/out/"),
			RequestId:  aws.String(uuid.NewString()),
			Name:       aws.String("run-summary-test"),
		})
		require.NoError(t, err)

		listed, err := client.ListRuns(t.Context(), &omicssdk.ListRunsInput{})
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.Equal(t, "wf-run-summary", aws.ToString(listed.Items[0].WorkflowName))
	})

	t.Run("run task uuid", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name:      aws.String("wf-task-summary"),
			Engine:    types.WorkflowEngineWdl,
			RequestId: aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		run, err := client.StartRun(t.Context(), &omicssdk.StartRunInput{
			WorkflowId: wf.Id,
			RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
			OutputUri:  aws.String("s3://bucket/out/"),
			RequestId:  aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		listed, err := client.ListRunTasks(t.Context(), &omicssdk.ListRunTasksInput{Id: run.Id})
		require.NoError(t, err)
		require.Len(t, listed.Items, 1)
		assert.NotEmpty(t, aws.ToString(listed.Items[0].Uuid))
	})

	t.Run("run in batch submission status", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		wf, err := client.CreateWorkflow(t.Context(), &omicssdk.CreateWorkflowInput{
			Name:      aws.String("wf-batch-summary"),
			Engine:    types.WorkflowEngineWdl,
			RequestId: aws.String(uuid.NewString()),
		})
		require.NoError(t, err)

		batch, err := client.StartRunBatch(t.Context(), &omicssdk.StartRunBatchInput{
			RequestId: aws.String(uuid.NewString()),
			BatchName: aws.String("batch-summary-test"),
			DefaultRunSetting: &types.DefaultRunSetting{
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/role"),
				WorkflowId: wf.Id,
			},
			BatchRunSettings: &types.BatchRunSettingsMemberInlineSettings{
				Value: []types.InlineSetting{{RunSettingId: aws.String("s1")}},
			},
		})
		require.NoError(t, err)

		listed, err := client.ListRunsInBatch(t.Context(), &omicssdk.ListRunsInBatchInput{
			BatchId: batch.Id,
		})
		require.NoError(t, err)
		require.Len(t, listed.Runs, 1)
		assert.Equal(t, types.SubmissionStatusSuccess, listed.Runs[0].SubmissionStatus)
		assert.Equal(t, "s1", aws.ToString(listed.Runs[0].RunSettingId))
	})

	t.Run("read set upload part creation time and part source", func(t *testing.T) {
		t.Parallel()

		client := newRealClient(t)

		store, err := client.CreateSequenceStore(t.Context(), &omicssdk.CreateSequenceStoreInput{
			Name: aws.String("seq-store-part-summary"),
		})
		require.NoError(t, err)

		upload, err := client.CreateMultipartReadSetUpload(
			t.Context(),
			&omicssdk.CreateMultipartReadSetUploadInput{
				SequenceStoreId: store.Id,
				Name:            aws.String("mp-rs-summary"),
				SourceFileType:  types.FileTypeFastq,
				SubjectId:       aws.String("subject-1"),
				SampleId:        aws.String("sample-1"),
			},
		)
		require.NoError(t, err)

		_, err = client.UploadReadSetPart(t.Context(), &omicssdk.UploadReadSetPartInput{
			SequenceStoreId: store.Id,
			UploadId:        upload.UploadId,
			PartSource:      types.ReadSetPartSourceSource1,
			PartNumber:      aws.Int32(1),
			Payload:         bytes.NewReader([]byte("part-data")),
		})
		require.NoError(t, err)

		listed, err := client.ListReadSetUploadParts(t.Context(), &omicssdk.ListReadSetUploadPartsInput{
			SequenceStoreId: store.Id,
			UploadId:        upload.UploadId,
			PartSource:      types.ReadSetPartSourceSource1,
		})
		require.NoError(t, err)
		require.Len(t, listed.Parts, 1)
		assert.False(t, aws.ToTime(listed.Parts[0].CreationTime).IsZero())
		assert.Equal(t, types.ReadSetPartSourceSource1, listed.Parts[0].PartSource)
	})
}

// TestListReadSetUploadParts_RawBodyUsesPartSourceKey proves the wire-key
// fix directly: real ReadSetUploadPartListItem serializes the part's source
// under "partSource" (deserializers.go's
// awsRestjson1_deserializeDocumentReadSetUploadPartListItem), a key this
// struct previously mislabeled "source" -- a key no real deserializer for
// this shape reads.
func TestListReadSetUploadParts_RawBodyUsesPartSourceKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s-part-key"})
	require.Equal(t, http.StatusCreated, storeRec.Code)

	var store map[string]any
	require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
	storeID := store["id"].(string)

	uploadRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/upload", map[string]any{
		"name":           "mp-rs-key",
		"sourceFileType": "FASTQ",
		"subjectId":      "subject-1",
		"sampleId":       "sample-1",
	})
	require.Equal(t, http.StatusCreated, uploadRec.Code)

	var upload map[string]any
	require.NoError(t, json.Unmarshal(uploadRec.Body.Bytes(), &upload))
	uploadID := upload["uploadId"].(string)

	partRec := doRequest(
		t, h, http.MethodPut,
		"/sequencestore/"+storeID+"/upload/"+uploadID+"/part?partNumber=1&partSource=SOURCE1",
		[]byte("data"),
	)
	require.Equal(t, http.StatusOK, partRec.Code)

	listRec := doRequest(
		t, h, http.MethodPost,
		"/sequencestore/"+storeID+"/upload/"+uploadID+"/parts",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Parts []map[string]any `json:"parts"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Parts, 1)

	assert.Equal(t, "SOURCE1", listResp.Parts[0]["partSource"])
	assert.NotContains(t, listResp.Parts[0], "source", "the real key is partSource, not source")
	assert.NotEmpty(t, listResp.Parts[0]["creationTime"])
}

// TestListSummaries_OmitDescribeOnlyFields proves, per flagged List op, that
// keys only the Get/Describe-shaped struct carries are absent from the real
// List response (over-wide response class, gopherstack-dv4s). Checked at the
// raw-body level since a typed client silently discards unknown keys and so
// can't see a leak.
func TestListSummaries_OmitDescribeOnlyFields(t *testing.T) {
	t.Parallel()

	t.Run("configurations omit runConfigurations tags uuid", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/configuration", map[string]any{
			"name":              "cfg-leak",
			"runConfigurations": map[string]any{},
			"tags":              map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/configuration", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "runConfigurations", "tags", "uuid")
		assert.Equal(t, "cfg-leak", items[0]["name"])
	})

	t.Run("read set activation jobs omit sources", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s-act-leak"})
		require.Equal(t, http.StatusCreated, storeRec.Code)

		var store map[string]any
		require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
		storeID := store["id"].(string)

		jobRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/activationjob", map[string]any{
			"sources": []map[string]any{{"readSetId": "rs-1"}},
		})
		require.Equal(t, http.StatusCreated, jobRec.Code)

		listRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/activationjobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "activationJobs")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "sources")
	})

	t.Run("read set import jobs omit sources", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s-imp-leak"})
		require.Equal(t, http.StatusCreated, storeRec.Code)

		var store map[string]any
		require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
		storeID := store["id"].(string)

		jobRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/importjob", map[string]any{
			"roleArn": "arn:aws:iam::000000000000:role/role",
			"sources": []map[string]any{{"sourceFileType": "BAM", "name": "read-1"}},
		})
		require.Equal(t, http.StatusCreated, jobRec.Code)

		listRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/importjobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "importJobs")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "sources")
	})

	t.Run("read sets omit files tags updateTime", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		storeRec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s-rs-leak"})
		require.Equal(t, http.StatusCreated, storeRec.Code)

		var store map[string]any
		require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
		storeID := store["id"].(string)

		jobRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/importjob", map[string]any{
			"roleArn": "arn:aws:iam::000000000000:role/role",
			"sources": []map[string]any{{"sourceFileType": "BAM", "name": "read-leak"}},
		})
		require.Equal(t, http.StatusCreated, jobRec.Code)

		listRec := doRequest(t, h, http.MethodPost, "/sequencestore/"+storeID+"/readsets", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "readSets")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "files", "tags", "updateTime")
	})

	t.Run("references omit files tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		storeRec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s-ref-leak"})
		require.Equal(t, http.StatusCreated, storeRec.Code)

		var store map[string]any
		require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
		storeID := store["id"].(string)

		jobRec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjob", map[string]any{
			"roleArn": "arn:aws:iam::000000000000:role/role",
			"sources": []map[string]any{{"sourceFile": "s3://bucket/ref.fa", "name": "ref-leak"}},
		})
		require.Equal(t, http.StatusCreated, jobRec.Code)

		listRec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/references", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "references")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "files", "tags")
	})

	t.Run("reference import jobs omit sources", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		storeRec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{"name": "s-refimp-leak"})
		require.Equal(t, http.StatusCreated, storeRec.Code)

		var store map[string]any
		require.NoError(t, json.Unmarshal(storeRec.Body.Bytes(), &store))
		storeID := store["id"].(string)

		jobRec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjob", map[string]any{
			"roleArn": "arn:aws:iam::000000000000:role/role",
			"sources": []map[string]any{{"sourceFile": "s3://bucket/ref.fa", "name": "ref-imp-leak"}},
		})
		require.Equal(t, http.StatusCreated, jobRec.Code)

		listRec := doRequest(t, h, http.MethodPost, "/referencestore/"+storeID+"/importjobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "importJobs")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "sources")
	})

	t.Run("run caches omit description tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/runCache", map[string]any{
			"name":            "cache-leak",
			"cacheS3Location": "s3://bucket/cache/",
			"description":     "a cache",
			"tags":            map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/runCache", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "description", "tags")
	})

	t.Run("run groups omit tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/runGroup", map[string]any{
			"name": "rg-leak",
			"tags": map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/runGroup", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "tags")
	})

	t.Run("run tasks omit runId", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-task-leak", "engine": "WDL"})
		require.Equal(t, http.StatusCreated, wfRec.Code)

		var wf map[string]any
		require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
		wfID := wf["id"].(string)

		runRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
			"workflowId": wfID,
			"roleArn":    "arn:aws:iam::000000000000:role/role",
			"outputUri":  "s3://bucket/out/",
			"requestId":  "req-task-leak",
		})
		require.Equal(t, http.StatusCreated, runRec.Code)

		var run map[string]any
		require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &run))
		runID := run["id"].(string)

		listRec := doRequest(t, h, http.MethodGet, "/run/"+runID+"/task", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "runId")
	})

	t.Run("runs omit describe-only fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-run-leak", "engine": "WDL"})
		require.Equal(t, http.StatusCreated, wfRec.Code)

		var wf map[string]any
		require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
		wfID := wf["id"].(string)

		runRec := doRequest(t, h, http.MethodPost, "/run", map[string]any{
			"workflowId": wfID,
			"roleArn":    "arn:aws:iam::000000000000:role/role",
			"outputUri":  "s3://bucket/out/",
			"requestId":  "req-run-leak",
			"tags":       map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, runRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/run", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0],
			"configuration", "tags", "parameters", "roleArn", "runGroupId",
			"runSettingId", "networkingMode", "cacheId", "cacheBehavior",
			"retentionMode", "scratchStorageMode", "workflowType", "uuid",
		)
	})

	t.Run("runs in batch omit run fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-batch-leak", "engine": "WDL"})
		require.Equal(t, http.StatusCreated, wfRec.Code)

		var wf map[string]any
		require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
		wfID := wf["id"].(string)

		batchRec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
			"requestId": "req-batch-leak",
			"batchName": "batch-leak",
			"defaultRunSetting": map[string]any{
				"roleArn":    "arn:aws:iam::000000000000:role/role",
				"workflowId": wfID,
			},
			"batchRunSettings": map[string]any{
				"inlineSettings": []map[string]any{{"runSettingId": "s1"}},
			},
		})
		require.Equal(t, http.StatusCreated, batchRec.Code)

		var batch map[string]any
		require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batch))
		batchID := batch["id"].(string)

		listRec := doRequest(t, h, http.MethodGet, "/runBatch/"+batchID+"/run", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "runs")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0],
			"creationTime", "name", "workflowId", "roleArn", "runGroupId",
			"batchId", "networkingMode", "cacheId", "cacheBehavior",
			"retentionMode", "scratchStorageMode", "storageType",
			"workflowType", "status", "tags", "parameters", "configuration",
		)
	})

	t.Run("workflows omit describe-only fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
			"name":        "wf-leak",
			"engine":      "WDL",
			"description": "a workflow",
			"tags":        map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/workflow", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0],
			"tags", "parameterTemplate", "storageCapacity", "description", "engine", "storageType", "uuid",
		)
	})

	t.Run("workflow versions omit describe-only fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-ver-leak", "engine": "WDL"})
		require.Equal(t, http.StatusCreated, wfRec.Code)

		var wf map[string]any
		require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
		wfID := wf["id"].(string)

		verRec := doRequest(t, h, http.MethodPost, "/workflow/"+wfID+"/version", map[string]any{
			"versionName": "v1",
			"tags":        map[string]string{"k": "v"},
		})
		require.Equal(t, http.StatusCreated, verRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/workflow/"+wfID+"/version", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "items")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "tags", "parameterTemplate", "storageCapacity", "engine", "storageType")
	})
}

// listItems unmarshals a List response body and returns the array found
// under key.
func listItems(t *testing.T, body []byte, key string) []map[string]any {
	t.Helper()

	var resp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &resp))

	raw, ok := resp[key]
	require.True(t, ok, "response missing key %q", key)

	var items []map[string]any
	require.NoError(t, json.Unmarshal(raw, &items))

	return items
}

// assertKeysAbsent fails if any of keys is present in item.
func assertKeysAbsent(t *testing.T, item map[string]any, keys ...string) {
	t.Helper()

	for _, k := range keys {
		assert.NotContains(t, item, k)
	}
}
