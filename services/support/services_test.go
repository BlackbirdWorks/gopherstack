package support_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSupport_DescribeServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             map[string]any
		name             string
		wantContainsCode string
		wantCode         int
		wantMinCount     int
	}{
		{
			name:         "all_services",
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantMinCount: 1,
		},
		{
			name:             "filtered_by_service_code",
			body:             map[string]any{"serviceCodeList": []string{"amazon-s3"}},
			wantCode:         http.StatusOK,
			wantMinCount:     1,
			wantContainsCode: "amazon-s3",
		},
		{
			name:         "filter_no_match",
			body:         map[string]any{"serviceCodeList": []string{"nonexistent-service"}},
			wantCode:     http.StatusOK,
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSupportHandler(t)
			rec := doSupportRequest(t, h, "DescribeServices", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			resp := decodeSupportResponse(t, rec)
			services, ok := resp["services"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(services), tt.wantMinCount)

			if tt.wantContainsCode != "" {
				found := false
				for _, s := range services {
					svc, svcOK := s.(map[string]any)
					require.True(t, svcOK)

					if svc["code"] == tt.wantContainsCode {
						found = true

						break
					}
				}
				assert.True(t, found, "expected service code %s in response", tt.wantContainsCode)
			}
		})
	}
}
