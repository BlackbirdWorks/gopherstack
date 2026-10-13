package forecast_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/forecast"
)

func TestCreateDelay_StatusProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		delete  string
		delay   time.Duration
		elapsed time.Duration
	}{
		{
			name:    "pending",
			delay:   time.Hour,
			elapsed: time.Minute,
			want:    "CREATE_PENDING",
			delete:  "ResourceInUseException",
		},
		{
			name:    "in progress",
			delay:   time.Hour,
			elapsed: 40 * time.Minute,
			want:    "CREATE_IN_PROGRESS",
			delete:  "ResourceInUseException",
		},
		{name: "active", delay: time.Hour, elapsed: 2 * time.Hour, want: "ACTIVE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				backend := forecast.NewInMemoryBackend(testAccountID, testRegion)
				backend.SetCreateDelay(tt.delay)
				h := forecast.NewHandler(backend)

				code, created := request(t, h, "CreateDatasetGroup", map[string]any{
					"DatasetGroupName": "dg_delay", "Domain": "RETAIL",
				})
				require.Equal(t, http.StatusOK, code)
				arn := created["DatasetGroupArn"].(string)

				time.Sleep(tt.elapsed)

				_, desc := request(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": arn})
				assert.Equal(t, tt.want, desc["Status"])

				code, body := request(t, h, "DeleteDatasetGroup", map[string]any{"DatasetGroupArn": arn})
				if tt.delete != "" {
					assert.Equal(t, http.StatusBadRequest, code)
					assert.Equal(t, tt.delete, body["__type"])
				} else {
					assert.Equal(t, http.StatusOK, code)
				}
			})
		})
	}
}
