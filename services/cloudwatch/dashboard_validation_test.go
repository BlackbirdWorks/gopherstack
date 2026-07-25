package cloudwatch_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestBackend_PutDashboard_BodyValidation locks the DashboardBody JSON/widget
// schema validation that closes bd gopherstack-3ro: PutDashboard previously
// stored the body verbatim with no validation, so DashboardValidationMessages
// was always empty even for malformed input. Error-level messages must fail
// the whole call (dashboard not persisted); warning-level messages must not
// (dashboard persisted, messages returned informationally).
func TestBackend_PutDashboard_BodyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		body        string
		wantErrPath string
		wantErr     bool
		wantWarn    bool
	}{
		{
			name: "empty widgets array is fully valid",
			body: `{"widgets":[]}`,
		},
		{
			name: "empty object is fully valid",
			body: `{}`,
		},
		{
			name: "well-formed metric widget is fully valid",
			body: `{"widgets":[{"type":"metric","x":0,"y":0,"width":6,"height":6,` +
				`"properties":{"metrics":[["AWS/EC2","CPUUtilization"]]}}]}`,
		},
		{
			name:        "not JSON at all",
			body:        "not json",
			wantErr:     true,
			wantErrPath: "/",
		},
		{
			name:        "truncated JSON",
			body:        `{"widgets":[`,
			wantErr:     true,
			wantErrPath: "/",
		},
		{
			name:        "JSON array instead of object",
			body:        `["a","b"]`,
			wantErr:     true,
			wantErrPath: "/",
		},
		{
			name:        "widgets not an array",
			body:        `{"widgets":"nope"}`,
			wantErr:     true,
			wantErrPath: "/widgets",
		},
		{
			name:        "widget entry not an object",
			body:        `{"widgets":["nope"]}`,
			wantErr:     true,
			wantErrPath: "/widgets/0",
		},
		{
			name:        "widget missing type",
			body:        `{"widgets":[{"properties":{}}]}`,
			wantErr:     true,
			wantErrPath: "/widgets/0/type",
		},
		{
			name:        "widget layout field wrong type",
			body:        `{"widgets":[{"type":"text","properties":{},"x":"zero"}]}`,
			wantErr:     true,
			wantErrPath: "/widgets/0/x",
		},
		{
			name:        "invalid periodOverride",
			body:        `{"periodOverride":"sometimes"}`,
			wantErr:     true,
			wantErrPath: "/periodOverride",
		},
		{
			name:     "unrecognized widget type is a warning only",
			body:     `{"widgets":[{"type":"futuristic","properties":{}}]}`,
			wantWarn: true,
		},
		{
			name:     "widget missing properties is a warning only",
			body:     `{"widgets":[{"type":"text"}]}`,
			wantWarn: true,
		},
		{
			name:     "metric widget missing properties.metrics is a warning only",
			body:     `{"widgets":[{"type":"metric","properties":{}}]}`,
			wantWarn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()

			messages, err := b.PutDashboard("dash-"+strings.ReplaceAll(tt.name, " ", "-"), tt.body)

			if tt.wantErr {
				require.Error(t, err)

				var valErr *cloudwatch.DashboardValidationError
				require.ErrorAs(t, err, &valErr)
				require.NotEmpty(t, valErr.Messages)

				var found bool
				for _, m := range valErr.Messages {
					if m.IsError && m.DataPath == tt.wantErrPath {
						found = true
					}
				}
				assert.True(t, found, "expected an error message at DataPath %q, got %+v",
					tt.wantErrPath, valErr.Messages)

				// The dashboard must not have been persisted.
				_, _, getErr := b.GetDashboard("dash-" + strings.ReplaceAll(tt.name, " ", "-"))
				assert.Error(t, getErr)

				return
			}

			require.NoError(t, err)

			if tt.wantWarn {
				assert.NotEmpty(t, messages)
			}

			// The dashboard must have been persisted regardless of warnings.
			_, body, getErr := b.GetDashboard("dash-" + strings.ReplaceAll(tt.name, " ", "-"))
			require.NoError(t, getErr)
			assert.Equal(t, tt.body, body)
		})
	}
}

// TestBackend_PutDashboard_InvalidBodyDoesNotOverwriteExisting locks that a
// failed validation on an update leaves the previously-stored (valid) body
// untouched.
func TestBackend_PutDashboard_InvalidBodyDoesNotOverwriteExisting(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	_, err := b.PutDashboard("dash", `{"widgets":[]}`)
	require.NoError(t, err)

	_, err = b.PutDashboard("dash", `not json`)
	require.Error(t, err)

	_, body, err := b.GetDashboard("dash")
	require.NoError(t, err)
	assert.JSONEq(t, `{"widgets":[]}`, body)
}

// TestHandler_PutDashboard_InvalidBody locks the XML/query-protocol wire shape
// for a validation failure: HTTP 400, Error>Code=DashboardInvalidInputError,
// and the DashboardValidationMessages list embedded in the error body.
func TestHandler_PutDashboard_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, "Action=PutDashboard&DashboardName=bad-dash&DashboardBody=not-json")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DashboardInvalidInputError")
	assert.Contains(t, body, "DashboardValidationMessages")
	assert.Contains(t, body, "<DataPath>/</DataPath>")

	// Confirm nothing was persisted.
	rec = postForm(t, h, "Action=GetDashboard&DashboardName=bad-dash")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_PutDashboard_ValidBodyWithWarning locks that a structurally
// valid-but-imperfect dashboard body succeeds (HTTP 200) and reports the
// warning in DashboardValidationMessages instead of failing the call.
func TestHandler_PutDashboard_ValidBodyWithWarning(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postForm(t, h, "Action=PutDashboard&DashboardName=warn-dash&DashboardBody="+
		"%7B%22widgets%22%3A%5B%7B%22type%22%3A%22text%22%7D%5D%7D") // {"widgets":[{"type":"text"}]}
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DashboardValidationMessages")
	assert.Contains(t, rec.Body.String(), "may not render")
}

// TestCBOR_PutDashboard_InvalidBody locks the rpc-v2-cbor wire shape for a
// validation failure, which is encoded independently from the XML path.
func TestCBOR_PutDashboard_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postCBOR(t, h, "PutDashboard", cbor.Map{
		"DashboardName": cbor.String("bad-dash"),
		"DashboardBody": cbor.String("not json"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "DashboardInvalidInputError", rec.Header().Get("X-Amzn-Errortype"))

	rec = postCBOR(t, h, "GetDashboard", cbor.Map{
		"DashboardName": cbor.String("bad-dash"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCBOR_PutDashboard_ValidBodyWithWarning locks the success-with-warnings
// path over rpc-v2-cbor.
func TestCBOR_PutDashboard_ValidBodyWithWarning(t *testing.T) {
	t.Parallel()

	h := newCWHandler()

	rec := postCBOR(t, h, "PutDashboard", cbor.Map{
		"DashboardName": cbor.String("warn-dash"),
		"DashboardBody": cbor.String(`{"widgets":[{"type":"text"}]}`),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := decodeCBORResponse(t, rec)
	msgs, ok := resp["DashboardValidationMessages"].(cbor.List)
	require.True(t, ok)
	assert.NotEmpty(t, msgs)
}
