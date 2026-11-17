package gcpsql

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinalBackupRechecksTheCountLimit(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	defer b.Close()

	b.SetBackupLimits(1, 0)

	b.mu.Lock("test")
	b.projBackups["p/other"] = &projBackup{Project: "p", ID: "other", State: backupStateOK}
	b.mu.Unlock()

	rec := &instRec{Inst: Instance{Project: "p", Name: "i", DatabaseVersion: "POSTGRES_16"}}
	err := b.takeFinalBackup(t.Context(), rec, FinalBackupRequest{Enabled: true, TTLDays: 5})

	var apiErr *APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusTooManyRequests, apiErr.Status)
	assert.Equal(t, 1, b.BackupCount("p"))
}

func TestResolveExpiryBounds(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 20, 0, 0, 0, 0, time.UTC)
	created := now.AddDate(0, 0, -10)

	tests := []struct {
		name    string
		expiry  string
		ttl     int64
		wantErr bool
	}{
		{name: "ttl within ceiling", ttl: 300},
		{name: "ttl beyond ceiling from creation", ttl: 360, wantErr: true},
		{name: "ttl max on a fresh backup", ttl: 365},
		{name: "negative ttl", ttl: -1, wantErr: true},
		{name: "ttl over max", ttl: 366, wantErr: true},
		{name: "both", ttl: 1, expiry: "2026-02-01T00:00:00Z", wantErr: true},
		{name: "past expiry", expiry: "2026-01-01T00:00:00Z", wantErr: true},
		{name: "future expiry", expiry: "2026-02-01T00:00:00Z"},
		{name: "expiry over ceiling", expiry: "2027-06-01T00:00:00Z", wantErr: true},
		{name: "garbage expiry", expiry: "tomorrow", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			from := created
			if tt.name == "ttl max on a fresh backup" {
				from = now
			}

			_, err := resolveExpiry(now, from, tt.ttl, tt.expiry)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestParseBackupFilterMalformed(t *testing.T) {
	t.Parallel()

	long := "instance:" + strings.Repeat("a", maxBackupFilterLen)

	tests := []struct {
		name    string
		filter  string
		wantErr bool
	}{
		{name: "empty"},
		{name: "instance", filter: "instance:abc"},
		{name: "and", filter: "instance:abc AND type:FINAL"},
		{name: "time", filter: "backupInterval.startTime>=1950-01-01T01:01:25.771Z"},
		{name: "bad time", filter: "backupInterval.startTime>=yesterday", wantErr: true},
		{name: "unknown field", filter: "state:RUNNING", wantErr: true},
		{name: "bare word", filter: "abc", wantErr: true},
		{name: "operator only", filter: "AND AND", wantErr: false},
		{name: "colon only", filter: ":", wantErr: true},
		{name: "oversized", filter: long, wantErr: true},
		{name: "nul", filter: "instance:a\x00b"},
		{name: "unicode", filter: "instance:é世"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseBackupFilter(tt.filter)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestFinalBackupParamsRejectMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		want    FinalBackupRequest
		wantErr bool
	}{
		{name: "none"},
		{
			name: "enabled", query: "enableFinalBackup=true&finalBackupTtlDays=7",
			want: FinalBackupRequest{Enabled: true, TTLDays: 7},
		},
		{name: "bad bool", query: "enableFinalBackup=maybe", wantErr: true},
		{name: "bad ttl", query: "enableFinalBackup=true&finalBackupTtlDays=seven", wantErr: true},
		{name: "overflow ttl", query: "finalBackupTtlDays=99999999999999999999", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodDelete, "/?"+tt.query, nil)
			c := echo.New().NewContext(req, httptest.NewRecorder())

			got, err := finalBackupParams(c)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
