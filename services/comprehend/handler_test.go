package comprehend_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/comprehend"
)

func newHandler() *comprehend.Handler {
	return comprehend.NewHandler(comprehend.NewInMemoryBackend("123456789012", "us-east-1"))
}

func request(t *testing.T, handler *comprehend.Handler, operation string, input map[string]any) map[string]any {
	t.Helper()

	payload, err := json.Marshal(input)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Comprehend_20171127."+operation)
	rec := httptest.NewRecorder()
	ctx := echo.New().NewContext(req, rec)
	require.NoError(t, handler.Handler()(ctx))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var output map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &output))

	return output
}

func TestHandlerMetadataAndRouting(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	assert.Equal(t, "Comprehend", handler.Name())
	assert.Equal(t, "comprehend", handler.ChaosServiceName())
	assert.Equal(t, []string{"us-east-1"}, handler.ChaosRegions())
	assert.Contains(t, handler.GetSupportedOperations(), "DetectSentiment")
	assert.Contains(t, handler.GetSupportedOperations(), "StartDocumentClassificationJob")

	tests := []struct {
		name   string
		target string
		op     string
		want   bool
	}{
		{name: "match", target: "Comprehend_20171127.DetectSentiment", want: true, op: "DetectSentiment"},
		{name: "foreign", target: "Textract.DetectDocumentText", want: false, op: "Unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", test.target)
			ctx := echo.New().NewContext(req, httptest.NewRecorder())
			assert.Equal(t, test.want, handler.RouteMatcher()(ctx))
			assert.Equal(t, test.op, handler.ExtractOperation(ctx))
		})
	}
}

func TestSynchronousDetectionOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		input     map[string]any
		field     string
	}{
		{
			name:      "sentiment",
			operation: "DetectSentiment",
			input:     map[string]any{"Text": "This product is great.", "LanguageCode": "en"},
			field:     "Sentiment",
		},
		{
			name:      "entities",
			operation: "DetectEntities",
			input:     map[string]any{"Text": "Alice works here.", "LanguageCode": "en"},
			field:     "Entities",
		},
		{
			name:      "key_phrases",
			operation: "DetectKeyPhrases",
			input:     map[string]any{"Text": "customer support response.", "LanguageCode": "en"},
			field:     "KeyPhrases",
		},
		{
			name:      "pii",
			operation: "DetectPiiEntities",
			input:     map[string]any{"Text": "Email me@example.com.", "LanguageCode": "en"},
			field:     "Entities",
		},
		{
			name:      "syntax",
			operation: "DetectSyntax",
			input:     map[string]any{"Text": "Syntax works", "LanguageCode": "en"},
			field:     "SyntaxTokens",
		},
		{
			name:      "language",
			operation: "DetectDominantLanguage",
			input:     map[string]any{"Text": "Language works"},
			field:     "Languages",
		},
		{
			name:      "toxic",
			operation: "DetectToxicContent",
			input:     map[string]any{"TextSegments": []any{map[string]any{"Text": "I hate this"}}},
			field:     "ResultList",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			output := request(t, newHandler(), test.operation, test.input)
			assert.Contains(t, output, test.field)
			assert.NotEmpty(t, output[test.field])
		})
	}
}

func TestBatchDetectionOperations(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{
		"BatchDetectSentiment",
		"BatchDetectEntities",
		"BatchDetectKeyPhrases",
		"BatchDetectPiiEntities",
		"BatchDetectSyntax",
		"BatchDetectDominantLanguage",
	} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			output := request(t, newHandler(), operation, map[string]any{
				"TextList":     []any{"Alice has a great launch.", "Contact me@example.com."},
				"LanguageCode": "en",
			})
			results, ok := output["ResultList"].([]any)
			require.True(t, ok)
			assert.Len(t, results, 2)
			assert.Empty(t, output["ErrorList"])
		})
	}
}

func TestAsyncJobCompletionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prefix      string
		objectField string
	}{
		{prefix: "DocumentClassificationJob", objectField: "DocumentClassificationJobProperties"},
		{prefix: "EntitiesDetectionJob", objectField: "EntitiesDetectionJobProperties"},
		{prefix: "KeyPhrasesDetectionJob", objectField: "KeyPhrasesDetectionJobProperties"},
		{prefix: "SentimentDetectionJob", objectField: "SentimentDetectionJobProperties"},
		{prefix: "PiiEntitiesDetectionJob", objectField: "PiiEntitiesDetectionJobProperties"},
		{prefix: "TopicsDetectionJob", objectField: "TopicsDetectionJobProperties"},
		{prefix: "TargetedSentimentDetectionJob", objectField: "TargetedSentimentDetectionJobProperties"},
		{prefix: "DominantLanguageDetectionJob", objectField: "DominantLanguageDetectionJobProperties"},
		{prefix: "EventsDetectionJob", objectField: "EventsDetectionJobProperties"},
	}
	for _, test := range tests {
		t.Run(test.prefix, func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			started := request(
				t,
				handler,
				"Start"+test.prefix,
				map[string]any{"JobName": test.prefix, "LanguageCode": "en"},
			)
			assert.Equal(t, "SUBMITTED", started["JobStatus"])
			id := started["JobId"].(string)

			first := request(t, handler, "Describe"+test.prefix, map[string]any{"JobId": id})
			assert.Equal(t, "IN_PROGRESS", first[test.objectField].(map[string]any)["JobStatus"])
			second := request(t, handler, "Describe"+test.prefix, map[string]any{"JobId": id})
			assert.Equal(t, "COMPLETED", second[test.objectField].(map[string]any)["JobStatus"])

			listed := request(t, handler, "List"+test.prefix+"s", nil)
			assert.Len(t, listed[test.objectField+"List"], 1)
		})
	}
}

func TestAsyncJobFailureAndStopLifecycle(t *testing.T) {
	t.Parallel()

	for _, prefix := range []string{"SentimentDetectionJob", "EntitiesDetectionJob"} {
		t.Run(prefix+"_failed", func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			started := request(t, handler, "Start"+prefix, map[string]any{"JobName": "[fail]-job"})
			id := started["JobId"].(string)
			request(t, handler, "Describe"+prefix, map[string]any{"JobId": id})
			failed := request(t, handler, "Describe"+prefix, map[string]any{"JobId": id})
			properties := failed[prefix+"Properties"].(map[string]any)
			assert.Equal(t, "FAILED", properties["JobStatus"])
			assert.NotEmpty(t, properties["FailureReason"])
		})
		t.Run(prefix+"_stopped", func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			started := request(t, handler, "Start"+prefix, map[string]any{"JobName": "stop-job"})
			id := started["JobId"].(string)
			stopping := request(t, handler, "Stop"+prefix, map[string]any{"JobId": id})
			assert.Equal(t, "STOP_REQUESTED", stopping["JobStatus"])
			stopped := request(t, handler, "Describe"+prefix, map[string]any{"JobId": id})
			assert.Equal(t, "STOPPED", stopped[prefix+"Properties"].(map[string]any)["JobStatus"])
		})
	}
}

func TestResourceCRUDAndTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prefix       string
		nameField    string
		nameValue    string
		arnField     string
		objectField  string
		listField    string
		update       bool
		trainingType bool // must advance lifecycle to TRAINED before Delete
	}{
		{
			name:         "classifier",
			prefix:       "DocumentClassifier",
			nameField:    "DocumentClassifierName",
			nameValue:    "news",
			arnField:     "DocumentClassifierArn",
			objectField:  "DocumentClassifierProperties",
			listField:    "DocumentClassifierPropertiesList",
			trainingType: true,
		},
		{
			name:         "recognizer",
			prefix:       "EntityRecognizer",
			nameField:    "RecognizerName",
			nameValue:    "names",
			arnField:     "EntityRecognizerArn",
			objectField:  "EntityRecognizerProperties",
			listField:    "EntityRecognizerPropertiesList",
			trainingType: true,
		},
		{
			name:        "endpoint",
			prefix:      "Endpoint",
			nameField:   "EndpointName",
			nameValue:   "live",
			arnField:    "EndpointArn",
			objectField: "EndpointProperties",
			listField:   "EndpointPropertiesList",
			update:      true,
		},
		{
			name:        "flywheel",
			prefix:      "Flywheel",
			nameField:   "FlywheelName",
			nameValue:   "train",
			arnField:    "FlywheelArn",
			objectField: "FlywheelProperties",
			listField:   "FlywheelPropertiesList",
			update:      true,
		},
		{
			name:        "dataset",
			prefix:      "Dataset",
			nameField:   "DatasetName",
			nameValue:   "data",
			arnField:    "DatasetArn",
			objectField: "DatasetProperties",
			listField:   "DatasetPropertiesList",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			handler := newHandler()
			created := request(t, handler, "Create"+test.prefix, map[string]any{
				test.nameField: test.nameValue,
				"Tags":         []any{map[string]any{"Key": "team", "Value": "nlp"}},
			})
			resourceARN := created[test.arnField].(string)
			assert.NotEmpty(t, resourceARN)

			described := request(t, handler, "Describe"+test.prefix, map[string]any{test.arnField: resourceARN})
			assert.Contains(t, described, test.objectField)
			listed := request(t, handler, "List"+test.prefix+"s", nil)
			assert.Len(t, listed[test.listField], 1)

			request(t, handler, "TagResource", map[string]any{
				"ResourceArn": resourceARN,
				"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
			})
			tagged := request(t, handler, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
			assert.Len(t, tagged["Tags"], 2)
			request(t, handler, "UntagResource", map[string]any{"ResourceArn": resourceARN, "TagKeys": []any{"team"}})
			tagged = request(t, handler, "ListTagsForResource", map[string]any{"ResourceArn": resourceARN})
			assert.Len(t, tagged["Tags"], 1)

			if test.update {
				request(
					t,
					handler,
					"Update"+test.prefix,
					map[string]any{test.arnField: resourceARN, "DesiredInferenceUnits": 2},
				)
			}
			if test.trainingType {
				// Advance training lifecycle to TRAINED before delete.
				// 1st Describe (above) moved SUBMITTED→IN_PROGRESS; this call moves IN_PROGRESS→TRAINED.
				request(t, handler, "Describe"+test.prefix, map[string]any{test.arnField: resourceARN})
			}
			request(t, handler, "Delete"+test.prefix, map[string]any{test.arnField: resourceARN})
			listed = request(t, handler, "List"+test.prefix+"s", nil)
			assert.Empty(t, listed[test.listField])
		})
	}
}

func TestModelVersionsAndFlywheelIteration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		parentPrefix  string
		parentInput   map[string]any
		parentARN     string
		versionPrefix string
	}{
		{
			parentPrefix:  "DocumentClassifier",
			parentInput:   map[string]any{"DocumentClassifierName": "documents"},
			parentARN:     "DocumentClassifierArn",
			versionPrefix: "DocumentClassifierVersion",
		},
		{
			parentPrefix:  "EntityRecognizer",
			parentInput:   map[string]any{"RecognizerName": "entities"},
			parentARN:     "EntityRecognizerArn",
			versionPrefix: "EntityRecognizerVersion",
		},
	}
	for _, test := range tests {
		handler := newHandler()
		parent := request(t, handler, "Create"+test.parentPrefix, test.parentInput)
		created := request(t, handler, "Create"+test.versionPrefix, map[string]any{
			test.parentARN: parent[test.parentARN],
			"VersionName":  "v2",
		})
		assert.NotEmpty(t, created[test.parentARN])
		listed := request(t, handler, "List"+test.versionPrefix+"s", nil)
		assert.Len(t, listed[test.parentPrefix+"PropertiesList"], 1)
	}

	handler := newHandler()
	flywheel := request(t, handler, "CreateFlywheel", map[string]any{"FlywheelName": "quality"})
	flywheelARN := flywheel["FlywheelArn"].(string)
	started := request(t, handler, "StartFlywheelIteration", map[string]any{"FlywheelArn": flywheelARN})
	id := started["FlywheelIterationId"].(string)
	inProgress := request(t, handler, "GetFlywheelIteration", map[string]any{"FlywheelIterationId": id})
	assert.Equal(
		t,
		"IN_PROGRESS",
		inProgress["FlywheelIterationProperties"].(map[string]any)["FlywheelIterationStatus"],
	)
	completed := request(t, handler, "GetFlywheelIteration", map[string]any{"FlywheelIterationId": id})
	assert.Equal(t, "COMPLETED", completed["FlywheelIterationProperties"].(map[string]any)["FlywheelIterationStatus"])
	history := request(t, handler, "ListFlywheelIterationHistory", map[string]any{"FlywheelArn": flywheelARN})
	assert.Len(t, history["FlywheelIterationPropertiesList"], 1)
}

func TestResetRemovesState(t *testing.T) {
	t.Parallel()

	handler := newHandler()
	request(t, handler, "CreateDataset", map[string]any{"DatasetName": "temporary"})
	handler.Reset()
	output := request(t, handler, "ListDatasets", nil)
	assert.Empty(t, output["DatasetPropertiesList"])
}
