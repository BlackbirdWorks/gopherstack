package accessanalyzer_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestApplyArchiveRule verifies ApplyArchiveRule archives active findings.
func TestApplyArchiveRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "archives_active_findings",
			body: map[string]any{"ruleName": "my-rule"},
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				arn := mustAnalyzer(t, b, "arch-analyzer")
				_, _ = b.CreateArchiveRule("arch-analyzer", "my-rule", nil)
				_, _ = b.AddFinding("arch-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::b", nil, nil, nil)

				return arn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found_returns_404",
			body: map[string]any{
				"analyzerArn": "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing",
				"ruleName":    "r",
			},
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return ""
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			arn := tt.setupFn(b)

			if arn != "" {
				tt.body["analyzerArn"] = arn
			}

			rec := doRequest(t, h, http.MethodPut, "/archive-rule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
