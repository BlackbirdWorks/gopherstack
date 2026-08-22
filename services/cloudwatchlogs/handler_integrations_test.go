package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// validResourceConfigJSON is a valid PutIntegrationInput.ResourceConfig body
// fragment satisfying OpenSearchResourceConfig's required members
// (dataSourceRoleArn/dashboardViewerPrincipals/retentionDays --
// validateOpenSearchResourceConfig, validators.go).
const validResourceConfigJSON = `{"openSearchResourceConfig":{` +
	`"dataSourceRoleArn":"arn:aws:iam::123456789012:role/cwl-opensearch",` +
	`"dashboardViewerPrincipals":["arn:aws:iam::123456789012:user/viewer"],` +
	`"retentionDays":30}}`

func validResourceConfigMap() map[string]any {
	return map[string]any{
		"openSearchResourceConfig": map[string]any{
			"dataSourceRoleArn":         "arn:aws:iam::123456789012:role/cwl-opensearch",
			"dashboardViewerPrincipals": []string{"arn:aws:iam::123456789012:user/viewer"},
			"retentionDays":             30,
		},
	}
}

func TestHandler_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutIntegration/OK",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "my-opensearch",
				"integrationType": "OPENSEARCH",
				"resourceConfig":  validResourceConfigMap(),
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutIntegration/EmptyName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "",
				"integrationType": "OPENSEARCH",
				"resourceConfig":  validResourceConfigMap(),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "PutIntegration/MissingResourceConfig",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "no-config",
				"integrationType": "OPENSEARCH",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetIntegration/OK",
			action: "GetIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH","resourceConfig":`+
						validResourceConfigJSON+`}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetIntegration/NotFound",
			action:   "GetIntegration",
			body:     map[string]any{"integrationName": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "ListIntegrations/WithEntries",
			action: "ListIntegrations",
			body:   map[string]any{},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"ig1","integrationType":"OPENSEARCH","resourceConfig":`+
						validResourceConfigJSON+`}`)
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"ig2","integrationType":"OPENSEARCH","resourceConfig":`+
						validResourceConfigJSON+`}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteIntegration/OK",
			action: "DeleteIntegration",
			body:   map[string]any{"integrationName": "my-opensearch"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutIntegration",
					`{"integrationName":"my-opensearch","integrationType":"OPENSEARCH","resourceConfig":`+
						validResourceConfigJSON+`}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteIntegration/NotFound",
			action:   "DeleteIntegration",
			body:     map[string]any{"integrationName": "ghost"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_AssociateSourceToS3TableIntegrationOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantKey  string
		wantVal  string
		wantCode int
	}{
		// AssociateSourceToS3TableIntegration
		{
			name:   "AssociateSourceToS3TableIntegration/OK",
			action: "AssociateSourceToS3TableIntegration",
			body: map[string]any{
				"integrationArn": "arn:aws:s3tables:us-east-1:123:integration/my-int",
				"dataSource":     map[string]any{"name": "source1", "type": "CloudWatchLogs"},
			},
			wantCode: http.StatusOK,
			wantKey:  "identifier",
		},
		{
			name:     "AssociateSourceToS3TableIntegration/MissingArn",
			action:   "AssociateSourceToS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.wantKey != "" {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				if tt.wantVal != "" {
					assert.Equal(t, tt.wantVal, out[tt.wantKey])
				} else {
					assert.NotEmpty(t, out[tt.wantKey], "expected non-empty %s", tt.wantKey)
				}
			}
		})
	}
}

func TestHandler_S3TableIntegrationSourceOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		action        string
		wantListField string
		wantCode      int
	}{
		{
			// Real ListSourcesForS3TableIntegrationInput.IntegrationArn is
			// required (validateOpListSourcesForS3TableIntegrationInput) --
			// a previous revision accepted an empty body and always
			// returned an empty list regardless, which a real client could
			// never actually trigger since its own client-side validator
			// refuses to send the request without it.
			name:          "ListSourcesForS3TableIntegration/ReturnsEmptyForUnknownArn",
			action:        "ListSourcesForS3TableIntegration",
			body:          map[string]any{"integrationArn": "arn:aws:s3tables:us-east-1:123:integration/none"},
			wantCode:      http.StatusOK,
			wantListField: "sources",
		},
		{
			name:     "ListSourcesForS3TableIntegration/MissingArn",
			action:   "ListSourcesForS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DisassociateSourceFromS3TableIntegration/EmptyIdentifier",
			action:   "DisassociateSourceFromS3TableIntegration",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DisassociateSourceFromS3TableIntegration/NotFound",
			action:   "DisassociateSourceFromS3TableIntegration",
			body:     map[string]any{"identifier": "ghost"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			rec := makeLogsRequest(t, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantListField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				list, ok := resp[tt.wantListField].([]any)
				require.True(t, ok, "expected list field %q in response", tt.wantListField)
				assert.Empty(t, list)
			}
		})
	}
}

func TestHandler_DisassociateSourceFromS3TableIntegration_RoundTrip(t *testing.T) {
	t.Parallel()

	h, e := newTestHandler(t)

	assocRec := doLogsRequest(t, h, e, "AssociateSourceToS3TableIntegration",
		`{"integrationArn":"arn:aws:logs::123456789012:integration:my-integration",`+
			`"dataSource":{"name":"my-source","type":"S3"}}`)
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	identifier, ok := assocResp["identifier"].(string)
	require.True(t, ok, "expected identifier in AssociateSourceToS3TableIntegration response")
	require.NotEmpty(t, identifier)

	disRec := doLogsRequest(t, h, e, "DisassociateSourceFromS3TableIntegration",
		`{"identifier":"`+identifier+`"}`)
	require.Equal(t, http.StatusOK, disRec.Code)

	var disResp map[string]any
	require.NoError(t, json.Unmarshal(disRec.Body.Bytes(), &disResp))
	assert.Equal(t, identifier, disResp["identifier"])

	// A second disassociate of the same identifier must now fail: the
	// association was actually removed, not silently accepted.
	repeatRec := doLogsRequest(t, h, e, "DisassociateSourceFromS3TableIntegration",
		`{"identifier":"`+identifier+`"}`)
	assert.Equal(t, http.StatusNotFound, repeatRec.Code)
}

func TestHandler_IntegrationResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body       map[string]any
		name       string
		action     string
		wantFields []string
		wantCode   int
	}{
		{
			name:   "PutIntegration/HasIntegrationName",
			action: "PutIntegration",
			body: map[string]any{
				"integrationName": "ig",
				"integrationType": "OPENSEARCH",
				"resourceConfig":  validResourceConfigMap(),
			},
			wantFields: []string{"integrationName"},
			wantCode:   http.StatusOK,
		},
		{
			name:       "ListIntegrations/HasIntegrationSummaries",
			action:     "ListIntegrations",
			body:       map[string]any{},
			wantFields: []string{"integrationSummaries"},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			for _, field := range tt.wantFields {
				assert.Contains(t, resp, field, "response should contain field %q", field)
			}
		})
	}
}

// TestListSourcesForS3TableIntegration_RealRoundTrip drives
// AssociateSourceToS3TableIntegration then ListSourcesForS3TableIntegration
// through the real aws-sdk-go-v2 client. A previous revision's
// handleListSourcesForS3TableIntegration ignored its request body entirely
// and unconditionally returned an empty list, so an association that was
// genuinely stored (b.s3TableIntegrations) could never actually be
// observed through this op regardless of which integrationArn a real
// caller listed.
func TestListSourcesForS3TableIntegration_RealRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	integrationArn := "arn:aws:s3tables:us-east-1:000000000000:integration/my-int"

	assocOut, err := client.AssociateSourceToS3TableIntegration(
		t.Context(), &cwlsdk.AssociateSourceToS3TableIntegrationInput{
			IntegrationArn: aws.String(integrationArn),
			DataSource: &cwlsdktypes.DataSource{
				Name: aws.String("source1"),
				Type: aws.String("CloudWatchLogs"),
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, assocOut.Identifier)

	out, err := client.ListSourcesForS3TableIntegration(
		t.Context(), &cwlsdk.ListSourcesForS3TableIntegrationInput{
			IntegrationArn: aws.String(integrationArn),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.Sources, 1)
	assert.Equal(t, *assocOut.Identifier, aws.ToString(out.Sources[0].Identifier))
	require.NotNil(t, out.Sources[0].DataSource)
	assert.Equal(t, "source1", aws.ToString(out.Sources[0].DataSource.Name))
	assert.Equal(t, "CloudWatchLogs", aws.ToString(out.Sources[0].DataSource.Type))
	assert.Equal(t, cwlsdktypes.S3TableIntegrationSourceStatusActive, out.Sources[0].Status)
	assert.NotZero(t, aws.ToInt64(out.Sources[0].CreatedTimeStamp))

	// A different integrationArn must not see this association.
	otherOut, err := client.ListSourcesForS3TableIntegration(
		t.Context(), &cwlsdk.ListSourcesForS3TableIntegrationInput{
			IntegrationArn: aws.String("arn:aws:s3tables:us-east-1:000000000000:integration/other"),
		},
	)
	require.NoError(t, err)
	assert.Empty(t, otherOut.Sources)
}
