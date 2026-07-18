package route53_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

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

func TestQueryLoggingConfigAlreadyExists_Is409(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>` + zoneID + `</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:/r53/test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`

	first := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
	require.Equal(t, http.StatusCreated, first.Code)

	second := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
	assert.Equal(t, http.StatusConflict, second.Code,
		"real AWS QueryLoggingConfigAlreadyExists has httpStatusCode 409, not 400")
	assert.Contains(t, second.Body.String(), "QueryLoggingConfigAlreadyExists")
}

func TestDeleteZone_CascadesQueryLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		deleteZone        bool
		wantQLCAfterCount int
	}{
		{
			name:              "without_delete_qlc_remains",
			deleteZone:        false,
			wantQLCAfterCount: 1,
		},
		{
			name:              "with_delete_cascades_qlc",
			deleteZone:        true,
			wantQLCAfterCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53.NewInMemoryBackend()
			hz, err := b.CreateHostedZone("example.com", "ref-1", "", false, "")
			require.NoError(t, err)

			_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123456789012:log-group:test")
			require.NoError(t, err)
			assert.Equal(t, 1, route53.QueryLoggingConfigCount(b))

			if tt.deleteZone {
				require.NoError(t, b.DeleteHostedZone(hz.ID))
			}

			assert.Equal(t, tt.wantQLCAfterCount, route53.QueryLoggingConfigCount(b))
		})
	}
}

func TestExtractOperation_CreateQueryLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_query_logging",
			path:   "/2013-04-01/queryloggingconfig",
			method: http.MethodPost,
			wantOp: "CreateQueryLoggingConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestCreateQueryLoggingConfig_Uniqueness(t *testing.T) {
	t.Parallel()

	b := route53.NewInMemoryBackend()
	hz, err := b.CreateHostedZone("example.com", "ref", "", false, "")
	require.NoError(t, err)

	_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123:log-group:test")
	require.NoError(t, err)

	// Second creation on same zone should fail.
	_, err = b.CreateQueryLoggingConfig(hz.ID, "arn:aws:logs:us-east-1:123:log-group:other")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "QueryLoggingConfigAlreadyExists")
}

func TestQueryLoggingConfig_DeleteAndList(t *testing.T) {
	t.Parallel()

	h := newHandler(t)
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	createBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123:log-group:test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`, zoneID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type createResp struct {
		QueryLoggingConfig struct {
			ID string `xml:"Id"`
		} `xml:"QueryLoggingConfig"`
	}

	var cr createResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cr))

	// List configs.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), cr.QueryLoggingConfig.ID)

	// Get config.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig/"+cr.QueryLoggingConfig.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete config.
	rec = send(t, h, http.MethodDelete, "/2013-04-01/queryloggingconfig/"+cr.QueryLoggingConfig.ID, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// List again — empty.
	rec = send(t, h, http.MethodGet, "/2013-04-01/queryloggingconfig", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), cr.QueryLoggingConfig.ID)
}

func TestRoute53_CreateQueryLoggingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
		useZone      bool
	}{
		{
			name:    "create_qlc_success",
			useZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123456789012:log-group:my-log-group</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateQueryLoggingConfigResponse", "my-log-group"},
		},
		{
			name:    "create_qlc_missing_log_group",
			useZone: true,
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>PLACEHOLDER</HostedZoneId>
</CreateQueryLoggingConfigRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_qlc_zone_not_found",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateQueryLoggingConfigRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>ZNONEXISTENT</HostedZoneId>
  <CloudWatchLogsLogGroupArn>arn:aws:logs:us-east-1:123:log-group:test</CloudWatchLogsLogGroupArn>
</CreateQueryLoggingConfigRequest>`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			body := tt.body

			if tt.useZone {
				rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
				require.Equal(t, http.StatusCreated, rec.Code)
				zoneID := extractZoneID(t, rec.Body.String())
				body = strings.ReplaceAll(body, "PLACEHOLDER", zoneID)
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/queryloggingconfig", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
