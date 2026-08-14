package iotanalytics_test

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real IoT
// Analytics operation, extracted from iotanalytics@v1.32.0 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {channelName}/{datasetName}/{datastoreName}/{pipelineName}/
// {reprocessingId} URI label -- parseIoTAnalyticsPath and its per-family
// helpers (handler.go) never validate identifier shape, so the literal
// value doesn't matter here, only path depth and static segments. 34 real
// ops here, matching IoT Analytics's real op count exactly (also matches
// GetSupportedOperations's own 34 entries one-for-one).
//
// A systematic check for a shared method+path across all 34 ops found zero
// collisions -- every op has its own unique (method, path) pair, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"BatchPutMessage", "POST", "/messages/batch"},
		{"CancelPipelineReprocessing", "DELETE", "/pipelines/PLACEHOLDER/reprocessing/PLACEHOLDER"},
		{"CreateChannel", "POST", "/channels"},
		{"CreateDataset", "POST", "/datasets"},
		{"CreateDatasetContent", "POST", "/datasets/PLACEHOLDER/content"},
		{"CreateDatastore", "POST", "/datastores"},
		{"CreatePipeline", "POST", "/pipelines"},
		{"DeleteChannel", "DELETE", "/channels/PLACEHOLDER"},
		{"DeleteDataset", "DELETE", "/datasets/PLACEHOLDER"},
		{"DeleteDatasetContent", "DELETE", "/datasets/PLACEHOLDER/content"},
		{"DeleteDatastore", "DELETE", "/datastores/PLACEHOLDER"},
		{"DeletePipeline", "DELETE", "/pipelines/PLACEHOLDER"},
		{"DescribeChannel", "GET", "/channels/PLACEHOLDER"},
		{"DescribeDataset", "GET", "/datasets/PLACEHOLDER"},
		{"DescribeDatastore", "GET", "/datastores/PLACEHOLDER"},
		{"DescribeLoggingOptions", "GET", "/logging"},
		{"DescribePipeline", "GET", "/pipelines/PLACEHOLDER"},
		{"GetDatasetContent", "GET", "/datasets/PLACEHOLDER/content"},
		{"ListChannels", "GET", "/channels"},
		{"ListDatasetContents", "GET", "/datasets/PLACEHOLDER/contents"},
		{"ListDatasets", "GET", "/datasets"},
		{"ListDatastores", "GET", "/datastores"},
		{"ListPipelines", "GET", "/pipelines"},
		{"ListTagsForResource", "GET", "/tags"},
		{"PutLoggingOptions", "PUT", "/logging"},
		{"RunPipelineActivity", "POST", "/pipelineactivities/run"},
		{"SampleChannelData", "GET", "/channels/PLACEHOLDER/sample"},
		{"StartPipelineReprocessing", "POST", "/pipelines/PLACEHOLDER/reprocessing"},
		{"TagResource", "POST", "/tags"},
		{"UntagResource", "DELETE", "/tags"},
		{"UpdateChannel", "PUT", "/channels/PLACEHOLDER"},
		{"UpdateDataset", "PUT", "/datasets/PLACEHOLDER"},
		{"UpdateDatastore", "PUT", "/datastores/PLACEHOLDER"},
		{"UpdatePipeline", "PUT", "/pipelines/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real IoT Analytics op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseIoTAnalyticsPath (handler.go) resolves it to the right op,
// all 34 ops against IoT Analytics's real op count. It then drives the same
// request through the real Handler() and asserts the response's decoded
// "message" field is not the exact literal "not found" that Handler() emits
// via writeError(c, http.StatusNotFound, "ResourceNotFoundException", "not
// found") when parseIoTAnalyticsPath returns an empty op.
//
// A bare substring check on "not found" is NOT safe for this service --
// every one of its per-resource not-found sentinels in errors.go
// (ErrChannelNotFound, ErrDatastoreNotFound, ErrDatasetNotFound, etc.) is
// built via newNotFoundError("<resource> not found"), so e.g. describing a
// channel that doesn't exist legitimately returns the message "channel not
// found" -- which *contains* the miss sentinel's "not found" substring,
// exactly the amplify/xray collision trap called out for this campaign.
// Resolved by decoding the JSON body and comparing the "message" field for
// exact equality to "not found" rather than substring containment; every
// route case's PLACEHOLDER target does not exist in a fresh backend, so
// most GET/PUT/DELETE-by-name ops legitimately 404 with a resource-prefixed
// message, and only the miss sentinel itself is the bare two-word string.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))

			var resp struct {
				Message string `json:"message"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEqual(t, "not found", resp.Message,
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
