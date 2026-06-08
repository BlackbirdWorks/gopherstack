package route53_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

func createZone(t *testing.T, h *route53.Handler, name string) string {
	t.Helper()

	rec := send(
		t,
		h,
		http.MethodPost,
		"/2013-04-01/hostedzone",
		`<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
<Name>`+name+`</Name><CallerReference>ref-`+name+`</CallerReference></CreateHostedZoneRequest>`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Extract zone ID from response
	body := rec.Body.String()
	idx := 0
	for i := range body {
		if body[i:min53(i+12, len(body))] == `/hostedzone/` {
			idx = i + 12

			break
		}
	}
	if idx == 0 {
		return ""
	}
	end := idx
	for end < len(body) && body[end] != '<' {
		end++
	}

	return body[idx:end]
}

func min53(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func TestRoute53_QueryLoggingConfig(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	zoneID := createZone(t, h, "query-log.example.com")
	require.NotEmpty(t, zoneID)

	// CreateQueryLoggingConfig
	const cwLogGroup = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/route53/query-log"
	rec := send(
		t, h, http.MethodPost, "/2013-04-01/queryloggingconfig",
		`<CreateQueryLoggingConfigRequest>`+
			`<HostedZoneId>`+zoneID+`</HostedZoneId>`+
			`<CloudWatchLogsLogGroupArn>`+cwLogGroup+`</CloudWatchLogsLogGroupArn>`+
			`</CreateQueryLoggingConfigRequest>`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// ListQueryLoggingConfigs
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig?hostedzoneid="+zoneID, "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRoute53_ReusableDelegationSets(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// CreateReusableDelegationSet
	rec := send(
		t,
		h,
		http.MethodPost,
		"/2013-04-01/delegationset",
		`<CreateReusableDelegationSetRequest>`+
			`<CallerReference>ref-ds-1</CallerReference>`+
			`</CreateReusableDelegationSetRequest>`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// ListReusableDelegationSets
	rec = send(t, h, http.MethodGet, "/2013-04-01/delegationset", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}
