package qldb_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/qldb"
)

func newBackend(t *testing.T) *qldb.InMemoryBackend {
	t.Helper()

	return qldb.NewInMemoryBackend("000000000000", "us-east-1")
}

// ---------------------------------------------------------------------------
// Backend method tests
// ---------------------------------------------------------------------------

func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		want      string
	}{
		{name: "returns_configured_account_id", accountID: "123456789012", want: "123456789012"},
		{name: "default_account_id", accountID: "000000000000", want: "000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := qldb.NewInMemoryBackend(tt.accountID, "us-east-1")
			assert.Equal(t, tt.want, b.AccountID())
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "clears_all_state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("ledger-a", "ALLOW_ALL", false, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, qldb.LedgerCount(b))

			b.Reset()

			assert.Equal(t, 0, qldb.LedgerCount(b))
			assert.Equal(t, 0, qldb.StreamCount(b))
			assert.Equal(t, 0, qldb.ExportCount(b))
		})
	}
}

func TestBackend_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ledgerNames []string
		wantLedgers int
	}{
		{
			name:        "empty_backend",
			ledgerNames: nil,
			wantLedgers: 0,
		},
		{
			name:        "single_ledger",
			ledgerNames: []string{"ledger-x"},
			wantLedgers: 1,
		},
		{
			name:        "multiple_ledgers",
			ledgerNames: []string{"a", "b", "c"},
			wantLedgers: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			for _, name := range tt.ledgerNames {
				_, err := b.CreateLedger(name, "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			}

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := newBackend(t)
			err := b2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, tt.wantLedgers, qldb.LedgerCount(b2))
		})
	}
}

func TestBackend_ListLedgers_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ledgerNames []string
		wantOrder   []string
	}{
		{
			name:        "sorted_alphabetically",
			ledgerNames: []string{"charlie", "alpha", "bravo"},
			wantOrder:   []string{"alpha", "bravo", "charlie"},
		},
		{
			name:        "single_ledger",
			ledgerNames: []string{"only"},
			wantOrder:   []string{"only"},
		},
		{
			name:        "already_sorted",
			ledgerNames: []string{"aaa", "bbb"},
			wantOrder:   []string{"aaa", "bbb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			for _, name := range tt.ledgerNames {
				_, err := b.CreateLedger(name, "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			}

			ledgers := b.ListLedgers()
			require.Len(t, ledgers, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, ledgers[i].Name)
			}
		})
	}
}

func TestBackend_ListJournalKinesisStreams_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		streamNames []string
		wantOrder   []string
	}{
		{
			name:        "sorted_by_stream_name",
			streamNames: []string{"zebra", "apple", "mango"},
			wantOrder:   []string{"apple", "mango", "zebra"},
		},
		{
			name:        "single_stream",
			streamNames: []string{"only-stream"},
			wantOrder:   []string{"only-stream"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("my-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			for _, sn := range tt.streamNames {
				b.AddJournalKinesisStreamInternal(&qldb.JournalKinesisStream{
					LedgerName: "my-ledger",
					StreamID:   sn + "-id",
					StreamName: sn,
					Status:     "ACTIVE",
				})
			}

			streams, err := b.ListJournalKinesisStreamsForLedger("my-ledger")
			require.NoError(t, err)
			require.Len(t, streams, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, streams[i].StreamName)
			}
		})
	}
}

func TestBackend_ListJournalS3Exports_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		exportCount int
	}{
		{name: "multiple_exports_sorted", exportCount: 3},
		{name: "single_export", exportCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("export-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			for range tt.exportCount {
				_, err = b.ExportJournalToS3(
					"export-ledger", "arn:aws:iam::000000000000:role/qldb",
					qldb.S3ExportConfiguration{Bucket: "bucket", Prefix: "prefix/"},
					time.Now().Add(-time.Hour), time.Now(), "ION_BINARY",
				)
				require.NoError(t, err)
			}

			exports := b.ListJournalS3Exports()
			require.Len(t, exports, tt.exportCount)

			for i := 1; i < len(exports); i++ {
				assert.LessOrEqual(t, exports[i-1].ExportID, exports[i].ExportID)
			}

			ledgerExports, err := b.ListJournalS3ExportsForLedger("export-ledger")
			require.NoError(t, err)
			require.Len(t, ledgerExports, tt.exportCount)

			for i := 1; i < len(ledgerExports); i++ {
				assert.LessOrEqual(t, ledgerExports[i-1].ExportID, ledgerExports[i].ExportID)
			}
		})
	}
}

func TestBackend_CreateLedger_ValidationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		permissionsMode string
		wantErr         bool
	}{
		{name: "allow_all_valid", permissionsMode: "ALLOW_ALL", wantErr: false},
		{name: "superuser_valid", permissionsMode: "SUPERUSER", wantErr: false},
		{name: "standard_valid", permissionsMode: "STANDARD", wantErr: false},
		{name: "empty_defaults_to_allow_all", permissionsMode: "", wantErr: false},
		{name: "invalid_mode_returns_error", permissionsMode: "INVALID_MODE", wantErr: true},
		{name: "lowercase_invalid", permissionsMode: "allow_all", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("test-ledger", tt.permissionsMode, false, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestBackend_DeleteLedger_Cascade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		streamCount int
		exportCount int
		wantStreams int
		wantExports int
	}{
		{
			name:        "deletes_streams_and_exports",
			streamCount: 2,
			exportCount: 2,
			wantStreams: 0,
			wantExports: 0,
		},
		{
			name:        "no_streams_no_exports",
			streamCount: 0,
			exportCount: 0,
			wantStreams: 0,
			wantExports: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("cascade-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			for i := range tt.streamCount {
				b.AddJournalKinesisStreamInternal(&qldb.JournalKinesisStream{
					LedgerName: "cascade-ledger",
					StreamID:   "stream-" + string(rune('a'+i)),
					StreamName: "stream-name-" + string(rune('a'+i)),
					Status:     "ACTIVE",
				})
			}

			for range tt.exportCount {
				_, err = b.ExportJournalToS3(
					"cascade-ledger", "arn:aws:iam::000000000000:role/qldb",
					qldb.S3ExportConfiguration{Bucket: "bucket", Prefix: "prefix/"},
					time.Now().Add(-time.Hour), time.Now(), "ION_BINARY",
				)
				require.NoError(t, err)
			}

			err = b.DeleteLedger("cascade-ledger")
			require.NoError(t, err)

			assert.Equal(t, tt.wantStreams, qldb.StreamCount(b))
			assert.Equal(t, tt.wantExports, qldb.ExportCount(b))
		})
	}
}

func TestBackend_ErrValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "err_validation_is_non_nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, qldb.ErrValidation, tt.name)
			assert.Contains(t, qldb.ErrValidation.Error(), "ValidationException")
		})
	}
}

// ---------------------------------------------------------------------------
// Handler tests
// ---------------------------------------------------------------------------

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "clears_backend_via_handler_reset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			h := qldb.NewHandler(b)

			_, err := b.CreateLedger("temp-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, qldb.LedgerCount(b))

			h.Reset()

			assert.Equal(t, 0, qldb.LedgerCount(b), tt.name)
		})
	}
}

func TestHandler_CreateLedger_InvalidPermissionsMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		permissionsMode string
		wantStatus      int
	}{
		{name: "invalid_mode_returns_400", permissionsMode: "BOGUS_MODE", wantStatus: http.StatusBadRequest},
		{name: "allow_all_returns_200", permissionsMode: "ALLOW_ALL", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doQLDBRequest(t, h, http.MethodPost, "/ledgers", map[string]any{
				"Name":            "test-ledger",
				"PermissionsMode": tt.permissionsMode,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ledgerARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ledgerName string
		accountID  string
		region     string
		wantARN    string
	}{
		{
			name:       "builds_correct_arn",
			ledgerName: "my-ledger",
			accountID:  "123456789012",
			region:     "us-east-1",
			wantARN:    "arn:aws:qldb:us-east-1:123456789012:ledger/my-ledger",
		},
		{
			name:       "different_region",
			ledgerName: "other-ledger",
			accountID:  "000000000000",
			region:     "eu-west-1",
			wantARN:    "arn:aws:qldb:eu-west-1:000000000000:ledger/other-ledger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := qldb.NewInMemoryBackend(tt.accountID, tt.region)
			_, err := b.CreateLedger(tt.ledgerName, "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			ledger, err := b.GetLedger(tt.ledgerName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantARN, ledger.ARN)
		})
	}
}

// ---------------------------------------------------------------------------
// Provider tests
// ---------------------------------------------------------------------------

func TestProvider_NilContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		ctx     *service.AppContext
		name    string
	}{
		{
			name:    "nil_context_returns_error",
			ctx:     nil,
			wantErr: qldb.ErrNilAppContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &qldb.Provider{}
			_, err := p.Init(tt.ctx)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// ---------------------------------------------------------------------------
// Export helper tests
// ---------------------------------------------------------------------------

func TestExport_LedgerCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ledgerCount int
		want        int
	}{
		{name: "zero_ledgers", ledgerCount: 0, want: 0},
		{name: "one_ledger", ledgerCount: 1, want: 1},
		{name: "three_ledgers", ledgerCount: 3, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			for i := range tt.ledgerCount {
				_, err := b.CreateLedger("ledger-"+string(rune('a'+i)), "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, qldb.LedgerCount(b))
		})
	}
}

func TestExport_StreamCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		streamCount int
		want        int
	}{
		{name: "zero_streams", streamCount: 0, want: 0},
		{name: "two_streams", streamCount: 2, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("stream-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			for i := range tt.streamCount {
				b.AddJournalKinesisStreamInternal(&qldb.JournalKinesisStream{
					LedgerName: "stream-ledger",
					StreamID:   "stream-id-" + string(rune('a'+i)),
					StreamName: "stream-" + string(rune('a'+i)),
					Status:     "ACTIVE",
				})
			}

			assert.Equal(t, tt.want, qldb.StreamCount(b))
		})
	}
}

func TestExport_ExportCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		exportCount int
		want        int
	}{
		{name: "zero_exports", exportCount: 0, want: 0},
		{name: "two_exports", exportCount: 2, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateLedger("export-count-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			for range tt.exportCount {
				_, err = b.ExportJournalToS3(
					"export-count-ledger", "arn:aws:iam::000000000000:role/qldb",
					qldb.S3ExportConfiguration{Bucket: "bucket", Prefix: "prefix/"},
					time.Now().Add(-time.Hour), time.Now(), "ION_BINARY",
				)
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, qldb.ExportCount(b))
		})
	}
}

func TestExport_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantMin int
	}{
		{name: "handler_has_operations", wantMin: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			count := qldb.HandlerOpsLen(h)
			assert.GreaterOrEqual(t, count, tt.wantMin)
		})
	}
}

func TestExport_AddLedgerInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ledgerName string
		want       int
	}{
		{name: "inserts_ledger_directly", ledgerName: "direct-ledger", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			ledger := &qldb.Ledger{
				Name:  tt.ledgerName,
				ARN:   "arn:aws:qldb:us-east-1:000000000000:ledger/" + tt.ledgerName,
				State: "ACTIVE",
			}
			qldb.AddLedgerInternal(b, ledger)

			assert.Equal(t, tt.want, qldb.LedgerCount(b))

			got, err := b.GetLedger(tt.ledgerName)
			require.NoError(t, err)
			assert.Equal(t, tt.ledgerName, got.Name)
		})
	}
}

// ---------------------------------------------------------------------------
// Integration-style tests
// ---------------------------------------------------------------------------

func TestBackend_Sorted_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst string
		wantLast  string
	}{
		{name: "multiple_ledgers_sorted", wantFirst: "aaa", wantLast: "zzz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			names := []string{"zzz", "mmm", "aaa", "nnn", "bbb"}
			for _, n := range names {
				_, err := b.CreateLedger(n, "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			}

			ledgers := b.ListLedgers()
			require.Len(t, ledgers, len(names))
			assert.Equal(t, tt.wantFirst, ledgers[0].Name)
			assert.Equal(t, tt.wantLast, ledgers[len(ledgers)-1].Name)
		})
	}
}

func TestPersistence_Roundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ledgerNames []string
		wantLedgers int
		wantStreams int
		wantExports int
	}{
		{
			name:        "full_state_roundtrip",
			ledgerNames: []string{"ledger-one", "ledger-two"},
			wantLedgers: 2,
			wantStreams: 1,
			wantExports: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			for _, name := range tt.ledgerNames {
				_, err := b.CreateLedger(name, "ALLOW_ALL", false, nil)
				require.NoError(t, err)
			}

			b.AddJournalKinesisStreamInternal(&qldb.JournalKinesisStream{
				LedgerName: tt.ledgerNames[0],
				StreamID:   "stream-1",
				StreamName: "my-stream",
				Status:     "ACTIVE",
			})

			_, err := b.ExportJournalToS3(
				tt.ledgerNames[0], "arn:aws:iam::000000000000:role/qldb",
				qldb.S3ExportConfiguration{Bucket: "bucket", Prefix: "prefix/"},
				time.Now().Add(-time.Hour), time.Now(), "ION_BINARY",
			)
			require.NoError(t, err)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := newBackend(t)
			err = b2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, tt.wantLedgers, qldb.LedgerCount(b2))
			assert.Equal(t, tt.wantStreams, qldb.StreamCount(b2))
			assert.Equal(t, tt.wantExports, qldb.ExportCount(b2))
		})
	}
}

func TestBackend_Reset_EmptyState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_produces_empty_state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)

			_, err := b.CreateLedger("pre-reset-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			b.AddJournalKinesisStreamInternal(&qldb.JournalKinesisStream{
				LedgerName: "pre-reset-ledger",
				StreamID:   "s-1",
				StreamName: "stream-1",
				Status:     "ACTIVE",
			})

			_, err = b.ExportJournalToS3(
				"pre-reset-ledger", "arn:aws:iam::000000000000:role/qldb",
				qldb.S3ExportConfiguration{Bucket: "bucket", Prefix: "prefix/"},
				time.Now().Add(-time.Hour), time.Now(), "ION_BINARY",
			)
			require.NoError(t, err)

			b.Reset()

			assert.Equal(t, 0, qldb.LedgerCount(b), tt.name)
			assert.Equal(t, 0, qldb.StreamCount(b), tt.name)
			assert.Equal(t, 0, qldb.ExportCount(b), tt.name)

			// Verify backend is still usable after reset
			_, err = b.CreateLedger("new-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, qldb.LedgerCount(b))
		})
	}
}

// TestHandler_Snapshot_Restore tests the handler's Snapshot and Restore delegation.
func TestHandler_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_delegates_snapshot_restore"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			h := qldb.NewHandler(b)

			_, err := b.CreateLedger("snap-ledger", "ALLOW_ALL", false, nil)
			require.NoError(t, err)

			snap := h.Snapshot()
			require.NotNil(t, snap, tt.name)

			b2 := newBackend(t)
			h2 := qldb.NewHandler(b2)
			err = h2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, 1, qldb.LedgerCount(b2))
		})
	}
}

func TestHandler_CreateLedger_InvalidMode_Body(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "invalid_permissions_mode_body_contains_message",
			body: map[string]any{
				"Name":            "bad-mode-ledger",
				"PermissionsMode": "NOT_VALID",
			},
			wantStatus: http.StatusBadRequest,
			wantBody:   "NOT_VALID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doQLDBRequest(t, h, http.MethodPost, "/ledgers", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var respBody map[string]string
			err := json.Unmarshal(rec.Body.Bytes(), &respBody)
			require.NoError(t, err)
			assert.Contains(t, respBody["message"], tt.wantBody)
		})
	}
}
