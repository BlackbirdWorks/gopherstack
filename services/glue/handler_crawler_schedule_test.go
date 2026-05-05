package glue_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerCrawlerSchedule_StartCrawler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		wantCode    int
	}{
		{
			name:        "success_ready_to_running",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
		{
			name:        "already_running_error",
			crawlerName: "my-crawler",
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "not_found",
			crawlerName: "no-such-crawler",
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			if tt.name == "already_running_error" {
				rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StopCrawler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		makeRunning bool
		wantCode    int
	}{
		{
			name:        "success_running_to_stopping",
			crawlerName: "my-crawler",
			makeRunning: true,
			wantCode:    http.StatusOK,
		},
		{
			name:        "not_running_error",
			crawlerName: "my-crawler",
			makeRunning: false,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			if tt.makeRunning {
				rec := doGlueRequest(t, h, "StartCrawler", map[string]any{"Name": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StopCrawler", map[string]any{"Name": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_UpdateCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		schedule    string
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			schedule:    "cron(0 12 * * ? *)",
			wantCode:    http.StatusOK,
		},
		{
			name:        "not_found",
			crawlerName: "no-such-crawler",
			schedule:    "cron(0 12 * * ? *)",
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			rec := doGlueRequest(t, h, "UpdateCrawlerSchedule", map[string]any{
				"Name":     tt.crawlerName,
				"Schedule": tt.schedule,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StartCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		preSchedule bool
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
		{
			name:        "already_scheduled",
			crawlerName: "my-crawler",
			preSchedule: true,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			if tt.preSchedule {
				rec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": "my-crawler"})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "StartCrawlerSchedule", map[string]any{"CrawlerName": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerCrawlerSchedule_StopCrawlerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		crawlerName string
		wantCode    int
	}{
		{
			name:        "success",
			crawlerName: "my-crawler",
			wantCode:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]string{"Name": "db"}})
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name":         "my-crawler",
				"Role":         "arn:aws:iam::000000000000:role/r",
				"DatabaseName": "db",
			})

			rec := doGlueRequest(t, h, "StopCrawlerSchedule", map[string]any{"CrawlerName": tt.crawlerName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
