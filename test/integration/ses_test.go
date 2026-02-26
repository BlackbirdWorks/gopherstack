package integration_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sesPost sends a form-encoded POST request to the SES endpoint.
func sesPost(t *testing.T, form url.Values) *http.Response {
	t.Helper()

	form.Set("Version", "2010-12-01")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func sesReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_SES_VerifyEmailIdentity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := sesPost(t, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"EmailAddress": {"integ-verify@example.com"},
	})
	body := sesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "VerifyEmailIdentityResponse")
}

func TestIntegration_SES_SendEmail(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Verify sender first.
	sesPost(t, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"EmailAddress": {"sender@integ-test.com"},
	})

	// Send email.
	resp := sesPost(t, url.Values{
		"Action":                           {"SendEmail"},
		"Source":                           {"sender@integ-test.com"},
		"Destination.ToAddresses.member.1": {"recipient@integ-test.com"},
		"Message.Subject.Data":             {"Integration Test Email"},
		"Message.Body.Text.Data":           {"Hello from integration test"},
		"Message.Body.Html.Data":           {"<p>Hello from integration test</p>"},
	})
	body := sesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var sendResp struct {
		XMLName xml.Name `xml:"SendEmailResponse"`
		Result  struct {
			MessageID string `xml:"MessageId"`
		} `xml:"SendEmailResult"`
	}

	require.NoError(t, xml.Unmarshal([]byte(body), &sendResp))
	assert.NotEmpty(t, sendResp.Result.MessageID)
}

func TestIntegration_SES_ListIdentities(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Verify two identities.
	sesPost(t, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"EmailAddress": {"list-a@integ-test.com"},
	})
	sesPost(t, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"EmailAddress": {"list-b@integ-test.com"},
	})

	resp := sesPost(t, url.Values{
		"Action": {"ListIdentities"},
	})
	body := sesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ListIdentitiesResponse")
}

func TestIntegration_SES_GetIdentityVerificationAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Verify identity.
	sesPost(t, url.Values{
		"Action":       {"VerifyEmailIdentity"},
		"EmailAddress": {"attr-test@integ-test.com"},
	})

	resp := sesPost(t, url.Values{
		"Action":              {"GetIdentityVerificationAttributes"},
		"Identities.member.1": {"attr-test@integ-test.com"},
	})
	body := sesReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "GetIdentityVerificationAttributesResponse")
	assert.Contains(t, body, "Success")
}
