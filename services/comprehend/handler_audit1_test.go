package comprehend_test

// Audit batch-1: AWS-accuracy coverage for Comprehend (go-0u26).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.1 on success/error, error envelope
//     __type+message fields, non-POST → 405, missing X-Amz-Target → 400, GET / → op list
//   - DetectSentiment: Sentiment string, SentimentScore with Positive/Negative/Neutral/Mixed keys
//   - DetectEntities: Entities list with Text/Score/BeginOffset/EndOffset/Type fields
//   - DetectKeyPhrases: KeyPhrases list with Text/Score/BeginOffset/EndOffset fields
//   - DetectPiiEntities: Entities list with Text/Score/BeginOffset/EndOffset/Type fields
//   - DetectSyntax: SyntaxTokens with TokenId/Text/BeginOffset/EndOffset/PartOfSpeech.Tag/Score
//   - DetectDominantLanguage: Languages with LanguageCode/Score fields
//   - DetectToxicContent: ResultList with Toxicity/Labels (Name/Score) fields
//   - Batch operations: ResultList with Index field on each item, ErrorList present
//   - Async job lifecycle: Start → Describe field shapes (JobId/JobArn/JobStatus/SubmitTime)
//   - Resource lifecycle: classifier/recognizer/endpoint/flywheel/dataset ARN + Status shapes
//   - Tag round-trip: TagResource/ListTagsForResource (ResourceArn+Tags Key/Value)/UntagResource
//   - Resource policy: PutResourcePolicy → PolicyRevisionId, GetResourcePolicy field shapes
//   - Error paths: not-found → ResourceNotFoundException, conflict → ResourceInUseException,
//     missing required field → InvalidRequestException

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

// --- audit helpers ---

func a1ComprehendHandler(t *testing.T) *comprehend.Handler {
	t.Helper()

	return comprehend.NewHandler(comprehend.NewInMemoryBackend("000000000000", "us-east-1"))
}

func a1ComprehendDo(t *testing.T, h *comprehend.Handler, action, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "Comprehend_20171127."+action)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1ComprehendUnmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// --- Protocol accuracy ---

func TestAudit1_Comprehend_Protocol_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		body       string
		wantCT     string
		wantStatus int
	}{
		{
			name:       "success_response_has_json11_content_type",
			action:     "DetectDominantLanguage",
			body:       `{"Text":"hello world"}`,
			wantStatus: http.StatusOK,
			wantCT:     "application/x-amz-json-1.1",
		},
		{
			name:       "error_response_has_json11_content_type",
			action:     "DetectSentiment",
			body:       `{"Text":"","LanguageCode":"en"}`,
			wantStatus: http.StatusBadRequest,
			wantCT:     "application/x-amz-json-1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			rec := a1ComprehendDo(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestAudit1_Comprehend_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "not_found_resource",
			action: "DescribeDocumentClassifier",
			body:   `{"DocumentClassifierArn":"arn:aws:comprehend:us-east-1:000000000000:document-classifier/missing"}`,
		},
		{
			name:   "missing_required_text",
			action: "DetectSentiment",
			body:   `{"Text":"","LanguageCode":"en"}`,
		},
		{
			name:   "unknown_operation",
			action: "NoSuchOperation",
			body:   `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			rec := a1ComprehendDo(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			m := a1ComprehendUnmarshal(t, rec)
			assert.NotEmpty(t, m["__type"], "__type must be present in error envelope")
			assert.NotEmpty(t, m["message"], "message must be present in error envelope")
		})
	}
}

func TestAudit1_Comprehend_Protocol_HTTPRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		target     string
		wantStatus int
	}{
		{
			name:       "GET_slash_returns_op_list",
			method:     http.MethodGet,
			path:       "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "PUT_returns_405",
			method:     http.MethodPut,
			path:       "/",
			target:     "Comprehend_20171127.DetectSentiment",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "POST_missing_target_returns_400",
			method:     http.MethodPost,
			path:       "/",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader("{}"))
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit1_Comprehend_Protocol_OpList(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec.Code)

	var ops []string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
	assert.NotEmpty(t, ops, "GET / must return non-empty operation list")
	assert.Contains(t, ops, "DetectSentiment")
	assert.Contains(t, ops, "DetectEntities")
	assert.Contains(t, ops, "StartDocumentClassificationJob")
}

// --- DetectSentiment field shapes ---

func TestAudit1_Comprehend_DetectSentiment_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		text          string
		wantSentiment string
	}{
		{name: "positive_text", text: "I love this great product!", wantSentiment: "POSITIVE"},
		{name: "negative_text", text: "I hate this terrible experience", wantSentiment: "NEGATIVE"},
		{name: "neutral_text", text: "The package arrived yesterday", wantSentiment: "NEUTRAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			body := `{"Text":"` + tt.text + `","LanguageCode":"en"}`
			rec := a1ComprehendDo(t, h, "DetectSentiment", body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ComprehendUnmarshal(t, rec)
			assert.Equal(t, tt.wantSentiment, m["Sentiment"], "Sentiment field must match keyword")

			score, ok := m["SentimentScore"].(map[string]any)
			require.True(t, ok, "SentimentScore must be a map")
			assert.Contains(t, score, "Positive", "SentimentScore must have Positive key")
			assert.Contains(t, score, "Negative", "SentimentScore must have Negative key")
			assert.Contains(t, score, "Neutral", "SentimentScore must have Neutral key")
			assert.Contains(t, score, "Mixed", "SentimentScore must have Mixed key")
		})
	}
}

// --- DetectEntities field shapes ---

func TestAudit1_Comprehend_DetectEntities_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	rec := a1ComprehendDo(t, h, "DetectEntities", `{"Text":"Alice went to Paris.","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1ComprehendUnmarshal(t, rec)
	entities, ok := m["Entities"].([]any)
	require.True(t, ok, "Entities must be a list")
	require.NotEmpty(t, entities, "capitalized words should produce entities")

	entity := entities[0].(map[string]any)
	assert.Contains(t, entity, "Text", "entity must have Text field")
	assert.Contains(t, entity, "Score", "entity must have Score field")
	assert.Contains(t, entity, "BeginOffset", "entity must have BeginOffset field")
	assert.Contains(t, entity, "EndOffset", "entity must have EndOffset field")
	assert.Contains(t, entity, "Type", "entity must have Type field")
}

// --- DetectKeyPhrases field shapes ---

func TestAudit1_Comprehend_DetectKeyPhrases_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	rec := a1ComprehendDo(t, h, "DetectKeyPhrases", `{"Text":"customer support ticket","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1ComprehendUnmarshal(t, rec)
	phrases, ok := m["KeyPhrases"].([]any)
	require.True(t, ok, "KeyPhrases must be a list")
	require.NotEmpty(t, phrases)

	phrase := phrases[0].(map[string]any)
	assert.Contains(t, phrase, "Text", "key phrase must have Text field")
	assert.Contains(t, phrase, "Score", "key phrase must have Score field")
	assert.Contains(t, phrase, "BeginOffset", "key phrase must have BeginOffset field")
	assert.Contains(t, phrase, "EndOffset", "key phrase must have EndOffset field")
}

// --- DetectPiiEntities field shapes ---

func TestAudit1_Comprehend_DetectPiiEntities_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	rec := a1ComprehendDo(t, h, "DetectPiiEntities", `{"Text":"Email user@example.com please.","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1ComprehendUnmarshal(t, rec)
	entities, ok := m["Entities"].([]any)
	require.True(t, ok, "Entities must be a list")
	require.NotEmpty(t, entities, "email address should produce PII entity")

	entity := entities[0].(map[string]any)
	assert.Contains(t, entity, "Text", "PII entity must have Text field")
	assert.Contains(t, entity, "Score", "PII entity must have Score field")
	assert.Contains(t, entity, "BeginOffset", "PII entity must have BeginOffset field")
	assert.Contains(t, entity, "EndOffset", "PII entity must have EndOffset field")
	assert.Contains(t, entity, "Type", "PII entity must have Type field")
	assert.Equal(t, "EMAIL", entity["Type"])
}

// --- DetectSyntax field shapes ---

func TestAudit1_Comprehend_DetectSyntax_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	rec := a1ComprehendDo(t, h, "DetectSyntax", `{"Text":"The quick brown fox","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1ComprehendUnmarshal(t, rec)
	tokens, ok := m["SyntaxTokens"].([]any)
	require.True(t, ok, "SyntaxTokens must be a list")
	require.NotEmpty(t, tokens)

	token := tokens[0].(map[string]any)
	assert.Contains(t, token, "TokenId", "syntax token must have TokenId")
	assert.Contains(t, token, "Text", "syntax token must have Text")
	assert.Contains(t, token, "BeginOffset", "syntax token must have BeginOffset")
	assert.Contains(t, token, "EndOffset", "syntax token must have EndOffset")

	pos, ok := token["PartOfSpeech"].(map[string]any)
	require.True(t, ok, "PartOfSpeech must be a map")
	assert.Contains(t, pos, "Tag", "PartOfSpeech must have Tag field")
	assert.Contains(t, pos, "Score", "PartOfSpeech must have Score field")
}

// --- DetectDominantLanguage field shapes ---

func TestAudit1_Comprehend_DetectDominantLanguage_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	rec := a1ComprehendDo(t, h, "DetectDominantLanguage", `{"Text":"Hello world this is a test"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	m := a1ComprehendUnmarshal(t, rec)
	langs, ok := m["Languages"].([]any)
	require.True(t, ok, "Languages must be a list")
	require.NotEmpty(t, langs)

	lang := langs[0].(map[string]any)
	assert.Contains(t, lang, "LanguageCode", "language must have LanguageCode field")
	assert.Contains(t, lang, "Score", "language must have Score field")
}

// --- DetectToxicContent field shapes ---

func TestAudit1_Comprehend_DetectToxicContent_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		text     string
		wantHigh bool
	}{
		{name: "toxic_text", text: "I hate everyone", wantHigh: true},
		{name: "normal_text", text: "Have a great day", wantHigh: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			body := `{"TextSegments":[{"Text":"` + tt.text + `"}],"LanguageCode":"en"}`
			rec := a1ComprehendDo(t, h, "DetectToxicContent", body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ComprehendUnmarshal(t, rec)
			results, ok := m["ResultList"].([]any)
			require.True(t, ok, "ResultList must be a list")
			require.Len(t, results, 1)

			result := results[0].(map[string]any)
			assert.Contains(t, result, "Toxicity", "result must have Toxicity field")
			labels, ok := result["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			require.NotEmpty(t, labels)

			label := labels[0].(map[string]any)
			assert.Contains(t, label, "Name", "label must have Name field")
			assert.Contains(t, label, "Score", "label must have Score field")

			toxicity := result["Toxicity"].(float64)
			if tt.wantHigh {
				assert.Greater(t, toxicity, 0.5, "hate text should score high toxicity")
			} else {
				assert.Less(t, toxicity, 0.5, "normal text should score low toxicity")
			}
		})
	}
}

// --- Batch operation field shapes ---

func TestAudit1_Comprehend_BatchOperations_FieldShapes(t *testing.T) {
	t.Parallel()

	ops := []struct {
		action string
		body   string
	}{
		{
			action: "BatchDetectSentiment",
			body:   `{"TextList":["great product","bad service"],"LanguageCode":"en"}`,
		},
		{
			action: "BatchDetectEntities",
			body:   `{"TextList":["Alice went to Paris","Bob met Charlie"],"LanguageCode":"en"}`,
		},
		{
			action: "BatchDetectKeyPhrases",
			body:   `{"TextList":["customer service","product launch"],"LanguageCode":"en"}`,
		},
		{
			action: "BatchDetectSyntax",
			body:   `{"TextList":["quick brown fox","lazy dog"],"LanguageCode":"en"}`,
		},
		{
			action: "BatchDetectDominantLanguage",
			body:   `{"TextList":["hello world","bonjour monde"]}`,
		},
	}

	for _, op := range ops {
		t.Run(op.action, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			rec := a1ComprehendDo(t, h, op.action, op.body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ComprehendUnmarshal(t, rec)
			results, ok := m["ResultList"].([]any)
			require.True(t, ok, "ResultList must be a list")
			require.Len(t, results, 2, "one result per input text")

			for i, raw := range results {
				item := raw.(map[string]any)
				idx, idxOK := item["Index"].(float64)
				require.True(t, idxOK, "each batch result must have Index field")
				assert.Equal(t, i, int(idx), "Index must be sequential")
			}

			assert.Contains(t, m, "ErrorList", "batch response must have ErrorList")
		})
	}
}

// --- Async job lifecycle field shapes ---

func TestAudit1_Comprehend_AsyncJob_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefix      string
		objectField string
	}{
		{prefix: "SentimentDetectionJob", objectField: "SentimentDetectionJobProperties"},
		{prefix: "EntitiesDetectionJob", objectField: "EntitiesDetectionJobProperties"},
		{prefix: "TopicsDetectionJob", objectField: "TopicsDetectionJobProperties"},
	}

	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			startBody := `{"JobName":"audit-job","LanguageCode":"en"}`
			startRec := a1ComprehendDo(t, h, "Start"+tt.prefix, startBody)
			require.Equal(t, http.StatusOK, startRec.Code, startRec.Body.String())

			startResp := a1ComprehendUnmarshal(t, startRec)
			assert.NotEmpty(t, startResp["JobId"], "start response must have JobId")
			assert.NotEmpty(t, startResp["JobArn"], "start response must have JobArn")
			assert.Equal(t, "SUBMITTED", startResp["JobStatus"], "initial status must be SUBMITTED")

			jobID := startResp["JobId"].(string)
			descBody := `{"JobId":"` + jobID + `"}`
			descRec := a1ComprehendDo(t, h, "Describe"+tt.prefix, descBody)
			require.Equal(t, http.StatusOK, descRec.Code)

			descResp := a1ComprehendUnmarshal(t, descRec)
			props, ok := descResp[tt.objectField].(map[string]any)
			require.True(t, ok, "describe response must have %s key", tt.objectField)
			assert.NotEmpty(t, props["JobId"], "job properties must have JobId")
			assert.NotEmpty(t, props["JobArn"], "job properties must have JobArn")
			assert.NotEmpty(t, props["JobName"], "job properties must have JobName")
			assert.NotEmpty(t, props["JobStatus"], "job properties must have JobStatus")
			assert.NotEmpty(t, props["SubmitTime"], "job properties must have SubmitTime")
		})
	}
}

func TestAudit1_Comprehend_AsyncJob_StopLifecycle(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	startRec := a1ComprehendDo(t, h, "StartSentimentDetectionJob", `{"JobName":"stop-me","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, startRec.Code)

	startResp := a1ComprehendUnmarshal(t, startRec)
	jobID := startResp["JobId"].(string)

	stopRec := a1ComprehendDo(t, h, "StopSentimentDetectionJob", `{"JobId":"`+jobID+`"}`)
	require.Equal(t, http.StatusOK, stopRec.Code)

	stopResp := a1ComprehendUnmarshal(t, stopRec)
	assert.Equal(t, jobID, stopResp["JobId"])
	assert.Equal(t, "STOP_REQUESTED", stopResp["JobStatus"])

	descRec := a1ComprehendDo(t, h, "DescribeSentimentDetectionJob", `{"JobId":"`+jobID+`"}`)
	descResp := a1ComprehendUnmarshal(t, descRec)
	props := descResp["SentimentDetectionJobProperties"].(map[string]any)
	assert.Equal(t, "STOPPED", props["JobStatus"])
	assert.NotEmpty(t, props["EndTime"], "stopped job must have EndTime")
}

// --- Resource lifecycle field shapes ---

func TestAudit1_Comprehend_DocumentClassifier_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *comprehend.Handler)
		name string
	}{
		{
			name: "create_returns_arn",
			fn: func(t *testing.T, h *comprehend.Handler) {
				t.Helper()
				rec := a1ComprehendDo(t, h, "CreateDocumentClassifier",
					`{"DocumentClassifierName":"audit-clf","LanguageCode":"en"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				m := a1ComprehendUnmarshal(t, rec)
				arn, ok := m["DocumentClassifierArn"].(string)
				require.True(t, ok, "create response must have DocumentClassifierArn")
				assert.Contains(t, arn, "comprehend", "ARN must reference comprehend service")
				assert.Contains(t, arn, "document-classifier", "ARN must reference resource type")
				assert.Contains(t, arn, "audit-clf", "ARN must contain classifier name")
			},
		},
		{
			name: "describe_response_shape",
			fn: func(t *testing.T, h *comprehend.Handler) {
				t.Helper()
				createRec := a1ComprehendDo(t, h, "CreateDocumentClassifier",
					`{"DocumentClassifierName":"shape-clf","LanguageCode":"en"}`)
				createResp := a1ComprehendUnmarshal(t, createRec)
				arn := createResp["DocumentClassifierArn"].(string)

				descRec := a1ComprehendDo(t, h, "DescribeDocumentClassifier",
					`{"DocumentClassifierArn":"`+arn+`"}`)
				require.Equal(t, http.StatusOK, descRec.Code)
				m := a1ComprehendUnmarshal(t, descRec)
				props, ok := m["DocumentClassifierProperties"].(map[string]any)
				require.True(t, ok, "describe must return DocumentClassifierProperties")
				assert.NotEmpty(t, props["DocumentClassifierArn"], "properties must have ARN")
				assert.NotEmpty(t, props["Status"], "properties must have Status")
				assert.NotEmpty(t, props["SubmitTime"], "properties must have SubmitTime")
			},
		},
		{
			name: "duplicate_create_returns_conflict",
			fn: func(t *testing.T, h *comprehend.Handler) {
				t.Helper()
				body := `{"DocumentClassifierName":"dupe-clf","LanguageCode":"en"}`
				rec1 := a1ComprehendDo(t, h, "CreateDocumentClassifier", body)
				require.Equal(t, http.StatusOK, rec1.Code)
				rec2 := a1ComprehendDo(t, h, "CreateDocumentClassifier", body)
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
				m := a1ComprehendUnmarshal(t, rec2)
				assert.Equal(t, "ResourceInUseException", m["__type"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			tt.fn(t, h)
		})
	}
}

func TestAudit1_Comprehend_Endpoint_UpdateAndStatus(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	createRec := a1ComprehendDo(t, h, "CreateEndpoint", `{"EndpointName":"audit-ep","DesiredInferenceUnits":1}`)
	require.Equal(t, http.StatusOK, createRec.Code)

	createResp := a1ComprehendUnmarshal(t, createRec)
	arn := createResp["EndpointArn"].(string)
	assert.NotEmpty(t, arn)

	descRec := a1ComprehendDo(t, h, "DescribeEndpoint", `{"EndpointArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp := a1ComprehendUnmarshal(t, descRec)
	props := descResp["EndpointProperties"].(map[string]any)
	assert.Equal(t, "ACTIVE", props["Status"], "new endpoint must be ACTIVE")

	updateRec := a1ComprehendDo(t, h, "UpdateEndpoint",
		`{"EndpointArn":"`+arn+`","DesiredInferenceUnits":4}`)
	assert.Equal(t, http.StatusOK, updateRec.Code)
}

// --- Tag round-trip field shapes ---

func TestAudit1_Comprehend_Tags_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	createRec := a1ComprehendDo(t, h, "CreateEntityRecognizer",
		`{"RecognizerName":"tag-test","LanguageCode":"en","Tags":[{"Key":"env","Value":"prod"}]}`)
	require.Equal(t, http.StatusOK, createRec.Code)

	arn := a1ComprehendUnmarshal(t, createRec)["EntityRecognizerArn"].(string)

	// List initial tag
	listRec := a1ComprehendDo(t, h, "ListTagsForResource", `{"ResourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := a1ComprehendUnmarshal(t, listRec)
	assert.Equal(t, arn, listResp["ResourceArn"], "ListTagsForResource must echo ResourceArn")
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok, "Tags must be a list")
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "prod", tag["Value"])

	// Add a second tag
	tagRec := a1ComprehendDo(t, h, "TagResource",
		`{"ResourceArn":"`+arn+`","Tags":[{"Key":"team","Value":"nlp"}]}`)
	assert.Equal(t, http.StatusOK, tagRec.Code)

	listRec2 := a1ComprehendDo(t, h, "ListTagsForResource", `{"ResourceArn":"`+arn+`"}`)
	listResp2 := a1ComprehendUnmarshal(t, listRec2)
	assert.Len(t, listResp2["Tags"], 2)

	// Remove one tag
	untagRec := a1ComprehendDo(t, h, "UntagResource", `{"ResourceArn":"`+arn+`","TagKeys":["env"]}`)
	assert.Equal(t, http.StatusOK, untagRec.Code)

	listRec3 := a1ComprehendDo(t, h, "ListTagsForResource", `{"ResourceArn":"`+arn+`"}`)
	listResp3 := a1ComprehendUnmarshal(t, listRec3)
	assert.Len(t, listResp3["Tags"], 1)
}

// --- Resource policy field shapes ---

func TestAudit1_Comprehend_ResourcePolicy_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)
	createRec := a1ComprehendDo(t, h, "CreateDocumentClassifier",
		`{"DocumentClassifierName":"policy-clf","LanguageCode":"en"}`)
	require.Equal(t, http.StatusOK, createRec.Code)
	arn := a1ComprehendUnmarshal(t, createRec)["DocumentClassifierArn"].(string)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	putRec := a1ComprehendDo(t, h, "PutResourcePolicy",
		`{"ResourceArn":"`+arn+`","ResourcePolicy":"`+strings.ReplaceAll(policy, `"`, `\"`)+`"}`)
	require.Equal(t, http.StatusOK, putRec.Code)
	putResp := a1ComprehendUnmarshal(t, putRec)
	revision, ok := putResp["PolicyRevisionId"].(string)
	require.True(t, ok, "PutResourcePolicy must return PolicyRevisionId")
	assert.NotEmpty(t, revision)

	getRec := a1ComprehendDo(t, h, "DescribeResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := a1ComprehendUnmarshal(t, getRec)
	assert.NotEmpty(t, getResp["ResourcePolicy"], "DescribeResourcePolicy must return ResourcePolicy")
	assert.NotEmpty(t, getResp["PolicyRevisionId"], "DescribeResourcePolicy must return PolicyRevisionId")
	assert.NotEmpty(t, getResp["CreationTime"], "DescribeResourcePolicy must return CreationTime")
	assert.NotEmpty(t, getResp["LastModifiedTime"], "DescribeResourcePolicy must return LastModifiedTime")

	delRec := a1ComprehendDo(t, h, "DeleteResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusOK, delRec.Code)

	getRec2 := a1ComprehendDo(t, h, "DescribeResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	assert.Equal(t, http.StatusBadRequest, getRec2.Code, "describe after delete must return error")
}

// --- Error paths ---

func TestAudit1_Comprehend_Errors_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "describe_missing_classifier",
			action: "DescribeDocumentClassifier",
			body:   `{"DocumentClassifierArn":"arn:aws:comprehend:us-east-1:000000000000:document-classifier/nope"}`,
		},
		{
			name:   "describe_missing_entity_recognizer",
			action: "DescribeEntityRecognizer",
			body:   `{"EntityRecognizerArn":"arn:aws:comprehend:us-east-1:000000000000:entity-recognizer/nope"}`,
		},
		{
			name:   "describe_missing_endpoint",
			action: "DescribeEndpoint",
			body:   `{"EndpointArn":"arn:aws:comprehend:us-east-1:000000000000:endpoint/nope"}`,
		},
		{
			name:   "describe_missing_job",
			action: "DescribeSentimentDetectionJob",
			body:   `{"JobId":"no-such-job-id"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			rec := a1ComprehendDo(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := a1ComprehendUnmarshal(t, rec)
			assert.Equal(t, "ResourceNotFoundException", m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
}

func TestAudit1_Comprehend_Errors_InvalidRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "detect_sentiment_empty_text",
			action: "DetectSentiment",
			body:   `{"Text":"","LanguageCode":"en"}`,
		},
		{
			name:   "detect_entities_whitespace_text",
			action: "DetectEntities",
			body:   `{"Text":"   ","LanguageCode":"en"}`,
		},
		{
			name:   "create_classifier_missing_name",
			action: "CreateDocumentClassifier",
			body:   `{"DocumentClassifierName":"","LanguageCode":"en"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			rec := a1ComprehendDo(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := a1ComprehendUnmarshal(t, rec)
			assert.Equal(t, "InvalidRequestException", m["__type"])
		})
	}
}

// --- ContainsPiiEntities and ClassifyDocument ---

func TestAudit1_Comprehend_ContainsPiiEntities_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		text    string
		wantPii bool
	}{
		{name: "with_pii", text: "My SSN is 123-45-6789", wantPii: true},
		{name: "without_pii", text: "The weather is nice today", wantPii: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			body := `{"Text":"` + tt.text + `","LanguageCode":"en"}`
			rec := a1ComprehendDo(t, h, "ContainsPiiEntities", body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ComprehendUnmarshal(t, rec)
			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			if tt.wantPii {
				assert.NotEmpty(t, labels, "text with PII should have non-empty Labels")
			} else {
				assert.Empty(t, labels, "text without PII should have empty Labels")
			}
		})
	}
}

func TestAudit1_Comprehend_ClassifyDocument_FieldShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		wantClass string
	}{
		{name: "finance_text", text: "The finance sector money report", wantClass: "FINANCE"},
		{name: "sports_text", text: "The sports game was exciting", wantClass: "SPORTS"},
		{name: "tech_text", text: "The tech computer innovation", wantClass: "TECHNOLOGY"},
		{name: "other_text", text: "Random unclassified content here", wantClass: "OTHER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1ComprehendHandler(t)
			arn := "arn:aws:comprehend:us-east-1:000000000000:document-classifier/clf"
			body := `{"Text":"` + tt.text + `","EndpointArn":"` + arn + `"}`
			rec := a1ComprehendDo(t, h, "ClassifyDocument", body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1ComprehendUnmarshal(t, rec)
			classes, ok := m["Classes"].([]any)
			require.True(t, ok, "Classes must be a list")
			require.NotEmpty(t, classes)

			cls := classes[0].(map[string]any)
			assert.Contains(t, cls, "Name", "class must have Name field")
			assert.Contains(t, cls, "Score", "class must have Score field")
			assert.Equal(t, tt.wantClass, cls["Name"])

			labels, ok := m["Labels"].([]any)
			require.True(t, ok, "Labels must be a list")
			assert.NotEmpty(t, labels)
		})
	}
}

// --- Flywheel iteration field shapes ---

func TestAudit1_Comprehend_FlywheelIteration_FieldShapes(t *testing.T) {
	t.Parallel()

	h := a1ComprehendHandler(t)

	createRec := a1ComprehendDo(t, h, "CreateFlywheel", `{"FlywheelName":"audit-fw"}`)
	require.Equal(t, http.StatusOK, createRec.Code)
	fwArn := a1ComprehendUnmarshal(t, createRec)["FlywheelArn"].(string)

	startRec := a1ComprehendDo(t, h, "StartFlywheelIteration", `{"FlywheelArn":"`+fwArn+`"}`)
	require.Equal(t, http.StatusOK, startRec.Code)
	startResp := a1ComprehendUnmarshal(t, startRec)
	iterID, ok := startResp["FlywheelIterationId"].(string)
	require.True(t, ok, "StartFlywheelIteration must return FlywheelIterationId")
	assert.NotEmpty(t, iterID)

	getRec := a1ComprehendDo(t, h, "GetFlywheelIteration", `{"FlywheelIterationId":"`+iterID+`"}`)
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := a1ComprehendUnmarshal(t, getRec)
	props, ok := getResp["FlywheelIterationProperties"].(map[string]any)
	require.True(t, ok, "GetFlywheelIteration must return FlywheelIterationProperties")
	assert.NotEmpty(t, props["FlywheelArn"], "iteration properties must have FlywheelArn")
	assert.NotEmpty(t, props["FlywheelIterationId"], "iteration properties must have FlywheelIterationId")
	assert.NotEmpty(t, props["FlywheelIterationStatus"], "iteration properties must have FlywheelIterationStatus")
	assert.NotEmpty(t, props["CreationTime"], "iteration properties must have CreationTime")

	histRec := a1ComprehendDo(t, h, "ListFlywheelIterationHistory", `{"FlywheelArn":"`+fwArn+`"}`)
	require.Equal(t, http.StatusOK, histRec.Code)
	histResp := a1ComprehendUnmarshal(t, histRec)
	hist, ok := histResp["FlywheelIterationPropertiesList"].([]any)
	require.True(t, ok, "ListFlywheelIterationHistory must return FlywheelIterationPropertiesList")
	assert.Len(t, hist, 1)
}
