package bedrock_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func newTestHandler(t *testing.T) *bedrock.Handler {
	t.Helper()

	return bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))
}

// testCustomizationRoleArn, testOutputDataConfig and testTrainingDataConfig
// are stand-in values for CreateModelCustomizationJob's required RoleArn,
// OutputDataConfig and TrainingDataConfig members, shared by tests that
// don't specifically exercise those fields.
const testCustomizationRoleArn = "arn:aws:iam::000000000000:role/test-role"

func testOutputDataConfig() bedrock.OutputDataConfig {
	return bedrock.OutputDataConfig{S3Uri: "s3://test-bucket/output/"}
}

func testTrainingDataConfig() bedrock.TrainingDataConfig {
	return bedrock.TrainingDataConfig{S3Uri: "s3://test-bucket/training/"}
}

// withCustomizationJobRequiredFields adds RoleArn/OutputDataConfig/
// TrainingDataConfig to an HTTP CreateModelCustomizationJob request body,
// for tests that don't specifically exercise those fields.
func withCustomizationJobRequiredFields(body map[string]any) map[string]any {
	body["roleArn"] = testCustomizationRoleArn
	body["outputDataConfig"] = map[string]any{"s3Uri": "s3://test-bucket/output/"}
	body["trainingDataConfig"] = map[string]any{"s3Uri": "s3://test-bucket/training/"}

	return body
}

// createCustomizationJob calls CreateModelCustomizationJob with stand-in
// RoleArn/OutputDataConfig/TrainingDataConfig, for tests that don't
// specifically exercise those fields.
func createCustomizationJob(
	b *bedrock.InMemoryBackend, jobName, customModelName string,
) (*bedrock.ModelCustomizationJob, error) {
	return b.CreateModelCustomizationJob(
		jobName, customModelName, "amazon.titan-text-express-v1", "",
		testCustomizationRoleArn, testOutputDataConfig(), testTrainingDataConfig(),
		nil, nil,
	)
}

// testModelSource is a stand-in for CreateInferenceProfile's required
// ModelSource member, for tests that don't specifically exercise it.
const testModelSource = "anthropic.claude-v2"

func doRequest(
	t *testing.T,
	h *bedrock.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustUnmarshal(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}

func typesAsStrings(raw []any) []string {
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = v.(string)
	}

	return out
}

// keyStatus is duplicated here to avoid an import cycle; it must match the package constant.
const keyStatus = "status"
