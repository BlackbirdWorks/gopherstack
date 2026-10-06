package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestHandler_DescribeExportTasks_OmitsLogStreamNamePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
	}{
		{name: "with_prefix", prefix: "stream-"},
		{name: "without_prefix", prefix: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudwatchlogs.NewInMemoryBackend()
			h := cloudwatchlogs.NewHandler(backend)

			cloudwatchlogs.AddExportTaskInternal(backend, cloudwatchlogs.ExportTask{
				TaskID: "t1", LogGroupName: "/grp", Destination: "b", Status: "COMPLETED",
				LogStreamNamePrefix: tc.prefix,
			})

			rec := doLogsRequest(t, h, echo.New(), "DescribeExportTasks", `{}`)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				ExportTasks []map[string]any `json:"exportTasks"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.ExportTasks, 1)
			assert.NotContains(t, out.ExportTasks[0], "logStreamNamePrefix")
			assert.Equal(t, "/grp", out.ExportTasks[0]["logGroupName"])
		})
	}
}
