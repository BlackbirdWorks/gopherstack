package sesv2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

func newRefinement1Handler(t *testing.T) (*sesv2.Handler, *sesv2.InMemoryBackend) {
	t.Helper()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)

	return h, backend
}

// doReq performs a request against the sesv2 handler via echo.
func doReq(
	t *testing.T,
	h *sesv2.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestRefinement1_HandlerOpsLen verifies 22 operations are supported.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	_, backend := newRefinement1Handler(t)
	h := sesv2.NewHandler(backend)

	assert.Equal(t, 110, sesv2.HandlerOpsLen(h))
}

// TestRefinement1_AccountID verifies AccountID returns a non-empty value.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.NotEmpty(t, backend.AccountID())
}

// TestRefinement1_Region verifies Region returns a non-empty value.
func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.NotEmpty(t, backend.Region())
}

// TestRefinement1_Reset verifies Reset clears all data.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateEmailIdentity("reset@example.com", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, sesv2.IdentityCount(backend))

	backend.Reset()
	assert.Equal(t, 0, sesv2.IdentityCount(backend))
}

// TestRefinement1_SnapshotRestore verifies snapshot and restore work for new fields.
func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateContactList("my-list", "desc")
	require.NoError(t, err)

	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	backend2 := sesv2.NewInMemoryBackend()
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, sesv2.ContactListCount(backend2))
}

// TestRefinement1_BatchGetMetricData tests the BatchGetMetricData operation.
func TestRefinement1_BatchGetMetricData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queries   []sesv2.MetricDataQuery
		wantCount int
		wantErr   bool
	}{
		{
			name:      "single_query",
			queries:   []sesv2.MetricDataQuery{{ID: "q1", Namespace: "VDM", Metric: "SEND"}},
			wantCount: 1,
		},
		{
			name:      "multiple_queries",
			queries:   []sesv2.MetricDataQuery{{ID: "q1"}, {ID: "q2"}, {ID: "q3"}},
			wantCount: 3,
		},
		{
			name:      "empty_queries",
			queries:   []sesv2.MetricDataQuery{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			results, err := backend.BatchGetMetricData(tt.queries)

			require.NoError(t, err)
			assert.Len(t, results, tt.wantCount)

			for i, r := range results {
				assert.Equal(t, tt.queries[i].ID, r.ID)
				assert.NotEmpty(t, r.Timestamps, "BatchGetMetricData should return synthetic data")
				assert.NotEmpty(t, r.Values, "BatchGetMetricData should return synthetic data")
			}
		})
	}
}

// TestRefinement1_CancelExportJob tests the CancelExportJob operation.
func TestRefinement1_CancelExportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*sesv2.InMemoryBackend)
		jobID   string
		wantErr bool
	}{
		{
			name: "cancel_existing_job",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddExportJobInternal("job-1", "IN_PROGRESS")
			},
			jobID: "job-1",
		},
		{
			name:    "cancel_nonexistent_job",
			setup:   func(*sesv2.InMemoryBackend) {},
			jobID:   "no-such-job",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			err := backend.CancelExportJob(tt.jobID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_CreateConfigurationSetEventDestination tests event destination creation.
func TestRefinement1_CreateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(*sesv2.InMemoryBackend)
		configSetName string
		destName      string
		wantErr       bool
	}{
		{
			name: "happy_path",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateConfigurationSet("my-set")
			},
			configSetName: "my-set",
			destName:      "my-dest",
		},
		{
			name:          "config_set_not_found",
			setup:         func(*sesv2.InMemoryBackend) {},
			configSetName: "no-such-set",
			destName:      "my-dest",
			wantErr:       true,
		},
		{
			name: "duplicate_destination",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateConfigurationSet("my-set")
				_, _ = b.CreateConfigurationSetEventDestination("my-set", "my-dest", true, nil)
			},
			configSetName: "my-set",
			destName:      "my-dest",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateConfigurationSetEventDestination(
				tt.configSetName, tt.destName, true, []string{"SEND"},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_CreateContactList tests CreateContactList.
func TestRefinement1_CreateContactList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*sesv2.InMemoryBackend)
		list    string
		wantErr bool
	}{
		{
			name:  "create_new",
			setup: func(*sesv2.InMemoryBackend) {},
			list:  "my-list",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateContactList("my-list", "")
			},
			list:    "my-list",
			wantErr: true,
		},
		{
			name:    "empty_name",
			setup:   func(*sesv2.InMemoryBackend) {},
			list:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateContactList(tt.list, "desc")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.ContactListCount(backend))
		})
	}
}

// TestRefinement1_CreateContact tests CreateContact.
func TestRefinement1_CreateContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*sesv2.InMemoryBackend)
		listName     string
		emailAddress string
		wantErr      bool
	}{
		{
			name: "happy_path",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddContactListInternal("my-list")
			},
			listName:     "my-list",
			emailAddress: "user@example.com",
		},
		{
			name:         "list_not_found",
			setup:        func(*sesv2.InMemoryBackend) {},
			listName:     "no-such-list",
			emailAddress: "user@example.com",
			wantErr:      true,
		},
		{
			name: "duplicate_contact",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddContactListInternal("my-list")
				_, _ = b.CreateContact("my-list", "user@example.com", nil)
			},
			listName:     "my-list",
			emailAddress: "user@example.com",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateContact(tt.listName, tt.emailAddress, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.ContactCount(backend, tt.listName))
		})
	}
}

// TestRefinement1_CreateCustomVerificationEmailTemplate tests template creation.
func TestRefinement1_CreateCustomVerificationEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*sesv2.InMemoryBackend)
		templateName string
		wantErr      bool
	}{
		{
			name:         "create_new",
			setup:        func(*sesv2.InMemoryBackend) {},
			templateName: "my-tmpl",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateCustomVerificationEmailTemplate(
					&sesv2.CustomVerificationEmailTemplate{
						TemplateName: "my-tmpl",
					},
				)
			},
			templateName: "my-tmpl",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateCustomVerificationEmailTemplate(
				&sesv2.CustomVerificationEmailTemplate{
					TemplateName:     tt.templateName,
					FromEmailAddress: "verify@example.com",
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_CreateDedicatedIPPool tests dedicated IP pool creation.
func TestRefinement1_CreateDedicatedIPPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*sesv2.InMemoryBackend)
		poolName    string
		scalingMode string
		wantErr     bool
	}{
		{
			name:        "standard_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool",
			scalingMode: "STANDARD",
		},
		{
			name:        "managed_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool-2",
			scalingMode: "MANAGED",
		},
		{
			name:        "invalid_scaling_mode",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "my-pool",
			scalingMode: "INVALID",
			wantErr:     true,
		},
		{
			name:        "empty_name",
			setup:       func(*sesv2.InMemoryBackend) {},
			poolName:    "",
			scalingMode: "STANDARD",
			wantErr:     true,
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateDedicatedIPPool("my-pool", "STANDARD")
			},
			poolName:    "my-pool",
			scalingMode: "STANDARD",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateDedicatedIPPool(tt.poolName, tt.scalingMode)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.DedicatedIPPoolCount(backend))
		})
	}
}

// TestRefinement1_CreateDeliverabilityTestReport tests report creation.
func TestRefinement1_CreateDeliverabilityTestReport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		reportName       string
		fromEmailAddress string
	}{
		{
			name:             "creates_report",
			reportName:       "my-report",
			fromEmailAddress: "test@example.com",
		},
		{
			name:             "empty_from",
			reportName:       "my-report-2",
			fromEmailAddress: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			report, err := backend.CreateDeliverabilityTestReport(
				tt.reportName,
				tt.fromEmailAddress,
			)

			require.NoError(t, err)
			require.NotNil(t, report)
			assert.NotEmpty(t, report.ReportID)
			assert.Equal(t, "IN_PROGRESS", report.DeliverabilityTestStatus)
		})
	}
}

// TestRefinement1_CreateEmailIdentityPolicy tests policy creation.
func TestRefinement1_CreateEmailIdentityPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*sesv2.InMemoryBackend)
		identity   string
		policyName string
		wantErr    bool
	}{
		{
			name: "happy_path",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateEmailIdentity("owner@example.com", "", nil)
			},
			identity:   "owner@example.com",
			policyName: "my-policy",
		},
		{
			name:       "identity_not_found",
			setup:      func(*sesv2.InMemoryBackend) {},
			identity:   "no-such@example.com",
			policyName: "my-policy",
			wantErr:    true,
		},
		{
			name: "duplicate_policy",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateEmailIdentity("owner@example.com", "", nil)
				_ = b.CreateEmailIdentityPolicy("owner@example.com", "my-policy", "{}")
			},
			identity:   "owner@example.com",
			policyName: "my-policy",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			err := backend.CreateEmailIdentityPolicy(
				tt.identity,
				tt.policyName,
				`{"Version":"2012-10-17"}`,
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_CreateEmailTemplate tests email template creation.
func TestRefinement1_CreateEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*sesv2.InMemoryBackend)
		templateName string
		wantErr      bool
	}{
		{
			name:         "create_new",
			setup:        func(*sesv2.InMemoryBackend) {},
			templateName: "my-template",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateEmailTemplate("my-template", nil)
			},
			templateName: "my-template",
			wantErr:      true,
		},
		{
			name:         "empty_name",
			setup:        func(*sesv2.InMemoryBackend) {},
			templateName: "",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			content := &sesv2.EmailTemplateContent{Subject: "Hello", Text: "Hello world"}
			_, err := backend.CreateEmailTemplate(tt.templateName, content)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.EmailTemplateCount(backend))
		})
	}
}

// TestRefinement1_DeepCopyGetEmailIdentity verifies GetEmailIdentity returns a copy.
func TestRefinement1_DeepCopyGetEmailIdentity(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateEmailIdentity("copy@example.com", "", nil)
	require.NoError(t, err)

	ei, err := backend.GetEmailIdentity("copy@example.com")
	require.NoError(t, err)
	require.NotNil(t, ei)

	// Modifying the returned pointer should not affect the stored value.
	ei.Identity = "mutated@example.com"

	ei2, err := backend.GetEmailIdentity("copy@example.com")
	require.NoError(t, err)
	assert.Equal(t, "copy@example.com", ei2.Identity)
}

// TestRefinement1_DeleteConfigurationSetCascade verifies cascade delete of event destinations.
func TestRefinement1_DeleteConfigurationSetCascade(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateConfigurationSet("my-set")
	require.NoError(t, err)

	_, err = backend.CreateConfigurationSetEventDestination("my-set", "dest-1", true, nil)
	require.NoError(t, err)

	err = backend.DeleteConfigurationSet("my-set")
	require.NoError(t, err)

	assert.Equal(t, 0, sesv2.ConfigSetCount(backend))
}

// TestRefinement1_ErrNilAppContext verifies provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sesv2.ErrNilAppContext)
}

// TestRefinement1_ExportJobCount verifies ExportJobCount helper.
func TestRefinement1_ExportJobCount(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.Equal(t, 0, sesv2.ExportJobCount(backend))

	backend.AddExportJobInternal("job-1", "CREATED")
	backend.AddExportJobInternal("job-2", "IN_PROGRESS")
	assert.Equal(t, 2, sesv2.ExportJobCount(backend))
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)

	_, err := backend.CreateEmailIdentity("reset@example.com", "", nil)
	require.NoError(t, err)

	h.Reset()
	assert.Equal(t, 0, sesv2.IdentityCount(backend))
}

// TestRefinement1_ContactListSeedHelper verifies AddContactListInternal.
func TestRefinement1_ContactListSeedHelper(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	cl := backend.AddContactListInternal("seed-list")
	require.NotNil(t, cl)
	assert.Equal(t, "seed-list", cl.Name)
	assert.Equal(t, 1, sesv2.ContactListCount(backend))
}

// TestRefinement1_ListConfigurationSetsSorted verifies sorted listing.
func TestRefinement1_ListConfigurationSetsSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	for _, name := range []string{"z-set", "a-set", "m-set"} {
		_, err := backend.CreateConfigurationSet(name)
		require.NoError(t, err)
	}

	page := backend.ListConfigurationSets("", 0)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "a-set", page.Data[0].Name)
	assert.Equal(t, "m-set", page.Data[1].Name)
	assert.Equal(t, "z-set", page.Data[2].Name)
}

// TestRefinement1_ListEmailIdentitiesSorted verifies sorted listing.
func TestRefinement1_ListEmailIdentitiesSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	for _, id := range []string{"z@example.com", "a@example.com", "m@example.com"} {
		_, err := backend.CreateEmailIdentity(id, "", nil)
		require.NoError(t, err)
	}

	page := backend.ListEmailIdentities("", 0)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "a@example.com", page.Data[0].Identity)
}

// TestRefinement1_SnapshotRestoreRoundTrip tests all maps persist and restore.
func TestRefinement1_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	// Populate all new resource types.
	_, err := backend.CreateEmailIdentity("snap@example.com", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateContactList("snap-list", "desc")
	require.NoError(t, err)

	_, err = backend.CreateDedicatedIPPool("snap-pool", "STANDARD")
	require.NoError(t, err)

	_, err = backend.CreateEmailTemplate("snap-tmpl", &sesv2.EmailTemplateContent{Subject: "hi"})
	require.NoError(t, err)

	backend.AddExportJobInternal("snap-job", "CREATED")

	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	backend2 := sesv2.NewInMemoryBackend()
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, sesv2.IdentityCount(backend2))
	assert.Equal(t, 1, sesv2.ContactListCount(backend2))
	assert.Equal(t, 1, sesv2.DedicatedIPPoolCount(backend2))
	assert.Equal(t, 1, sesv2.EmailTemplateCount(backend2))
	assert.Equal(t, 1, sesv2.ExportJobCount(backend2))
}

// TestRefinement1_ConfigSetCount verifies ConfigSetCount helper.
func TestRefinement1_ConfigSetCount(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.Equal(t, 0, sesv2.ConfigSetCount(backend))

	_, err := backend.CreateConfigurationSet("set-1")
	require.NoError(t, err)
	assert.Equal(t, 1, sesv2.ConfigSetCount(backend))
}

// TestRefinement1_GetConfigurationSetDeepCopy verifies deep copy.
func TestRefinement1_GetConfigurationSetDeepCopy(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateConfigurationSet("copy-set")
	require.NoError(t, err)

	cs, err := backend.GetConfigurationSet("copy-set")
	require.NoError(t, err)
	cs.Name = "mutated"

	cs2, err := backend.GetConfigurationSet("copy-set")
	require.NoError(t, err)
	assert.Equal(t, "copy-set", cs2.Name)
}

// TestRefinement1_ErrorSentinels verifies error aliases work with [errors.Is].
func TestRefinement1_ErrorSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantIs error
		doErr  func() error
		name   string
	}{
		{
			name: "identity_not_found",
			doErr: func() error {
				_, err := sesv2.NewInMemoryBackend().GetEmailIdentity("no@example.com")

				return err
			},
			wantIs: sesv2.ErrNotFound,
		},
		{
			name: "identity_already_exists",
			doErr: func() error {
				b := sesv2.NewInMemoryBackend()
				_, _ = b.CreateEmailIdentity("dup@example.com", "", nil)
				_, err := b.CreateEmailIdentity("dup@example.com", "", nil)

				return err
			},
			wantIs: sesv2.ErrAlreadyExists,
		},
		{
			name: "invalid_input",
			doErr: func() error {
				_, err := sesv2.NewInMemoryBackend().CreateEmailIdentity("", "", nil)

				return err
			},
			wantIs: sesv2.ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.doErr()
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantIs)
		})
	}
}

// TestRefinement1_HTTPBatchGetMetricData tests BatchGetMetricData via HTTP.
func TestRefinement1_HTTPBatchGetMetricData(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{
		"Queries": []map[string]any{
			{"Id": "q1", "Namespace": "VDM", "Metric": "SEND"},
		},
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/metrics/batch", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPBatchGetMetricDataBadJSON tests bad JSON for BatchGetMetricData.
func TestRefinement1_HTTPBatchGetMetricDataBadJSON(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v2/email/metrics/batch",
		bytes.NewReader([]byte("not-json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_HTTPCancelExportJob tests CancelExportJob via HTTP.
func TestRefinement1_HTTPCancelExportJob(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)
	backend.AddExportJobInternal("job-123", "IN_PROGRESS")

	rec := doReq(t, h, http.MethodPut, "/v2/email/export-jobs/job-123/cancel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCancelExportJobNotFound tests CancelExportJob 404.
func TestRefinement1_HTTPCancelExportJobNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	rec := doReq(t, h, http.MethodPut, "/v2/email/export-jobs/no-such/cancel", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_HTTPCreateConfigurationSetEventDestination tests via HTTP.
func TestRefinement1_HTTPCreateConfigurationSetEventDestination(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)
	_, err := backend.CreateConfigurationSet("my-cs")
	require.NoError(t, err)

	body := map[string]any{
		"EventDestinationName": "my-dest",
		"EventDestination": map[string]any{
			"Enabled":            true,
			"MatchingEventTypes": []string{"SEND"},
		},
	}
	rec := doReq(
		t,
		h,
		http.MethodPost,
		"/v2/email/configuration-sets/my-cs/event-destinations",
		body,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateContact tests CreateContact via HTTP.
func TestRefinement1_HTTPCreateContact(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)
	backend.AddContactListInternal("cl-1")

	body := map[string]any{"EmailAddress": "user@example.com"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/contact-lists/cl-1/contacts", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateContactList tests CreateContactList via HTTP.
func TestRefinement1_HTTPCreateContactList(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{"ContactListName": "my-list", "Description": "desc"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/contact-lists", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateCustomVerificationTemplate tests via HTTP.
func TestRefinement1_HTTPCreateCustomVerificationTemplate(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{
		"TemplateName":          "my-tmpl",
		"FromEmailAddress":      "verify@example.com",
		"TemplateSubject":       "Verify",
		"TemplateContent":       "<p>Verify</p>",
		"SuccessRedirectionURL": "https://example.com/success",
		"FailureRedirectionURL": "https://example.com/failure",
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/custom-verification-email-templates", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateDedicatedIPPool tests CreateDedicatedIpPool via HTTP.
func TestRefinement1_HTTPCreateDedicatedIPPool(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{"PoolName": "my-pool", "ScalingMode": "STANDARD"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateDedicatedIPPoolInvalidMode tests invalid scaling mode.
func TestRefinement1_HTTPCreateDedicatedIPPoolInvalidMode(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{"PoolName": "my-pool", "ScalingMode": "INVALID"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/dedicated-ip-pools", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_HTTPCreateDeliverabilityTestReport tests via HTTP.
func TestRefinement1_HTTPCreateDeliverabilityTestReport(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{
		"ReportName":       "my-report",
		"FromEmailAddress": "test@example.com",
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/deliverability-dashboard/test", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateEmailIdentityPolicy tests via HTTP.
func TestRefinement1_HTTPCreateEmailIdentityPolicy(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)
	_, err := backend.CreateEmailIdentity("owner@example.com", "", nil)
	require.NoError(t, err)

	body := map[string]any{"Policy": `{"Version":"2012-10-17"}`}
	rec := doReq(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities/owner@example.com/policies/my-policy",
		body,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateEmailTemplate tests via HTTP.
func TestRefinement1_HTTPCreateEmailTemplate(t *testing.T) {
	t.Parallel()

	h, _ := newRefinement1Handler(t)
	body := map[string]any{
		"TemplateName": "my-template",
		"TemplateContent": map[string]any{
			"Subject": "Hello",
			"Text":    "Hello world",
			"Html":    "<p>Hello</p>",
		},
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/templates", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_HTTPCreateEmailTemplateDuplicate tests duplicate template error.
func TestRefinement1_HTTPCreateEmailTemplateDuplicate(t *testing.T) {
	t.Parallel()

	h, backend := newRefinement1Handler(t)
	_, err := backend.CreateEmailTemplate("my-template", nil)
	require.NoError(t, err)

	body := map[string]any{"TemplateName": "my-template", "TemplateContent": map[string]any{}}
	rec := doReq(t, h, http.MethodPost, "/v2/email/templates", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestSESv2Backend_SendEmailCap(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()

	// Send beyond 2x the cap so the amortized compaction path runs at least
	// once. After compaction the slice length must stay between
	// maxRetainedEmails and 2*maxRetainedEmails.
	total := sesv2.EmailCompactionHighWater + 5
	for i := range total {
		_, err := b.SendEmail("a@example.com", []string{"b@example.com"},
			"s", "h", "t")
		require.NoError(t, err, "iteration %d", i)
	}

	got := sesv2.EmailCount(b)
	assert.GreaterOrEqual(t, got, sesv2.MaxRetainedEmails,
		"retain at least the cap")
	assert.LessOrEqual(t, got, sesv2.EmailCompactionHighWater,
		"never exceed the compaction high-water mark")
}
