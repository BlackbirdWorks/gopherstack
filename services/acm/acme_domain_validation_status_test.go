package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcmeDomainValidation_SettlesToValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wait       time.Duration
		update     bool
	}{
		{name: "validating", wait: 0, wantStatus: "VALIDATING"},
		{name: "valid", wait: time.Second, wantStatus: "VALID"},
		{name: "update restarts", wait: time.Second, update: true, wantStatus: "VALIDATING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newACMHandler()
				epARN := createTestAcmeEndpoint(t, h)

				body, err := json.Marshal(map[string]any{
					"AcmeEndpointArn": epARN,
					"DomainName":      "dv.example.com",
					"PrevalidationOptions": map[string]any{
						"DnsPrevalidation": map[string]any{"HostedZoneId": "Z123456"},
					},
				})
				require.NoError(t, err)

				rec := postACMJSON(t, h, "CreateAcmeDomainValidation", string(body))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					Arn string `json:"AcmeDomainValidationArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				time.Sleep(tt.wait)

				if tt.update {
					upd, mErr := json.Marshal(map[string]any{
						"AcmeDomainValidationArn": out.Arn,
						"PrevalidationOptions": map[string]any{
							"DnsPrevalidation": map[string]any{"HostedZoneId": "Z999999"},
						},
					})
					require.NoError(t, mErr)
					require.Equal(t, http.StatusOK,
						postACMJSON(t, h, "UpdateAcmeDomainValidation", string(upd)).Code)
				}

				descBody, err := json.Marshal(map[string]string{"AcmeDomainValidationArn": out.Arn})
				require.NoError(t, err)

				desc := postACMJSON(t, h, "DescribeAcmeDomainValidation", string(descBody))
				require.Equal(t, http.StatusOK, desc.Code)

				var got struct {
					V struct {
						Status string `json:"Status"`
					} `json:"AcmeDomainValidation"`
				}
				require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &got))
				assert.Equal(t, tt.wantStatus, got.V.Status)
			})
		})
	}
}
