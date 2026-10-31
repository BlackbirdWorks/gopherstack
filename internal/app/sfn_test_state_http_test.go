package app

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestStepFunctionsTestStateHTTPTrace(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	tg := newHTTPTarget(t)

	fx.sfn.SetSDKIntegration(sfnbackend.NewSDKIntegrationWithHTTP(fx.handler, "us-east-1", nil, fakeConnections{
		httpTaskConnARN + "apikey": {AuthType: "API_KEY", APIKeyName: "X-Api-Key", APIKeyValue: "s3cret"},
	}))

	client := sfn.NewFromConfig(fx.cfg)
	definition := `{"Type":"Task","Resource":"arn:aws:states:::http:invoke","Parameters":{` +
		`"ApiEndpoint":"` + tg.srv.URL + `/echo","Method":"POST","QueryParameters":{"id":"trace"},` +
		`"Headers":{"X-Custom":"h1"},"RequestBody":{"message":"hi"},` +
		`"Authentication":{"ConnectionArn":"` + httpTaskConnARN + `apikey"}},"End":true}`

	tests := []struct {
		name        string
		level       sfntypes.InspectionLevel
		reveal      bool
		wantSecret  bool
		wantRequest bool
	}{
		{name: "trace_redacted", level: sfntypes.InspectionLevelTrace, wantRequest: true},
		{
			name:        "trace_revealed",
			level:       sfntypes.InspectionLevelTrace,
			reveal:      true,
			wantSecret:  true,
			wantRequest: true,
		},
		{name: "debug_has_no_http_data", level: sfntypes.InspectionLevelDebug},
		{name: "info_has_no_inspection", level: sfntypes.InspectionLevelInfo},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := client.TestState(t.Context(), &sfn.TestStateInput{
				Definition:      aws.String(definition),
				RoleArn:         aws.String(sfnTestRole),
				Input:           aws.String(`{}`),
				InspectionLevel: tc.level,
				RevealSecrets:   tc.reveal,
			})
			require.NoError(t, err)
			require.Equal(t, sfntypes.TestExecutionStatusSucceeded, out.Status)
			assert.Contains(t, aws.ToString(out.Output), `"ResponseBody":{"ok":true}`)

			if tc.level == sfntypes.InspectionLevelInfo {
				assert.Nil(t, out.InspectionData)

				return
			}

			require.NotNil(t, out.InspectionData)

			if !tc.wantRequest {
				assert.Nil(t, out.InspectionData.Request)
				assert.Nil(t, out.InspectionData.Response)

				return
			}

			req, resp := out.InspectionData.Request, out.InspectionData.Response
			require.NotNil(t, req)
			require.NotNil(t, resp)

			assert.Equal(t, "POST", aws.ToString(req.Method))
			assert.Equal(t, "HTTP/1.1", aws.ToString(req.Protocol))
			assert.Contains(t, aws.ToString(req.Url), "/echo?id=trace")
			assert.JSONEq(t, `{"message":"hi"}`, aws.ToString(req.Body))
			assert.Contains(t, aws.ToString(req.Headers), `"X-Custom":"h1"`)
			assert.Equal(t, tc.wantSecret, strings.Contains(aws.ToString(req.Headers), "s3cret"))
			assert.Equal(t, "200", aws.ToString(resp.StatusCode))
			assert.JSONEq(t, `{"ok":true}`, aws.ToString(resp.Body))
		})
	}
}

func TestStepFunctionsTestStateTraceValidation(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	client := sfn.NewFromConfig(fx.cfg)

	const httpDef = `{"Type":"Task","Resource":"arn:aws:states:::http:invoke","Parameters":{` +
		`"ApiEndpoint":"https://example.invalid","Method":"GET",` +
		`"Authentication":{"ConnectionArn":"arn:aws:events:us-east-1:000000000000:connection/c/x"}},"End":true}`

	tests := []struct {
		input *sfn.TestStateInput
		name  string
	}{
		{
			name: "trace_on_non_http_state",
			input: &sfn.TestStateInput{
				Definition: aws.String(`{"Type":"Pass","End":true}`), InspectionLevel: sfntypes.InspectionLevelTrace,
			},
		},
		{
			name: "reveal_secrets_with_mock",
			input: &sfn.TestStateInput{
				Definition: aws.String(httpDef), InspectionLevel: sfntypes.InspectionLevelTrace, RevealSecrets: true,
				Mock: &sfntypes.MockInput{Result: aws.String(`{}`)},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.input.RoleArn = aws.String(sfnTestRole)
			_, err := client.TestState(t.Context(), tc.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}
