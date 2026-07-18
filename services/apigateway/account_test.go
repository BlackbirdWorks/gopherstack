package apigateway_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAPIGW_Account covers GetAccount, UpdateAccount.
func TestAPIGW_Account(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	// GetAccount.
	rec := restRequest(t, h, http.MethodGet, "/account", "")
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")

	// UpdateAccount.
	rec = restRequest(t, h, http.MethodPatch, "/account",
		`{"cloudwatchRoleArn":"arn:aws:iam::000000000000:role/test-role"}`)
	assert.True(t, rec.Code >= 200 && rec.Code < 300, "expected 2xx")
}

// TestGetAccount_And_UpdateAccount tests GetAccount and UpdateAccount.
func TestGetAccount_And_UpdateAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		throttleBurst  int
		throttleRate   float64
		wantUpdateCode int
		wantGetCode    int
	}{
		{
			name:        "get_account",
			wantGetCode: http.StatusOK,
		},
		{
			name:           "update_throttle_settings",
			throttleBurst:  500,
			throttleRate:   100.0,
			wantUpdateCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			if tt.wantGetCode != 0 {
				rec := postWithHandler(t, handler, e, "GetAccount", `{}`)
				assert.Equal(t, tt.wantGetCode, rec.Code)
			}

			if tt.wantUpdateCode != 0 {
				rec := postWithHandler(t, handler, e, "UpdateAccount",
					fmt.Sprintf(`{"throttleSettings":{"burstLimit":%d,"rateLimit":%g}}`,
						tt.throttleBurst, tt.throttleRate))
				assert.Equal(t, tt.wantUpdateCode, rec.Code)
			}
		})
	}
}
