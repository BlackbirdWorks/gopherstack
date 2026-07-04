package transfer_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// --- CreateAccess ---

func TestHandler_CreateAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"ExternalId": "S-1-5-21-1234",
			},
			wantCode: http.StatusOK,
			wantKey:  "ExternalId",
		},
		{
			name:     "missing external id",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["ServerId"] = s.ServerID

			rec := doTransferRequest(t, h, "CreateAccess", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp[tt.wantKey])
			}
		})
	}
}

func TestHandler_CreateAccess_MissingServerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ExternalId": "S-1-5-21-1234",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAccess_ServerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":   "s-doesnotexist",
		"ExternalId": "S-1-5-21-1234",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- DeleteAccess ---

func TestHandler_DeleteAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) (serverID, externalID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)
				_, _ = h.Backend.CreateAccess(s.ServerID, "S-1-5-21-1234", "", "", nil)

				return s.ServerID, "S-1-5-21-1234"
			},
			wantCode: http.StatusOK,
		},
		{
			name: "access not found",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID, "S-missing"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "server not found",
			setup: func(_ *transfer.Handler) (string, string) {
				return "s-doesnotexist", "S-1-5-21-1234"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serverID, externalID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteAccess", map[string]any{
				"ServerId":   serverID,
				"ExternalId": externalID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteAccess_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"ExternalId": "S-1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing external id",
			body:     map[string]any{"ServerId": s.ServerID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "DeleteAccess", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- CreateAgreement ---

func TestHandler_CreateAgreement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			body: map[string]any{
				"LocalProfileId":   "p-local",
				"PartnerProfileId": "p-partner",
				"BaseDirectory":    "/home/as2",
				"AccessRole":       "arn:aws:iam::123456789012:role/as2-role",
				"Description":      "test agreement",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "minimal fields",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)
			body["ServerId"] = s.ServerID

			rec := doTransferRequest(t, h, "CreateAgreement", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["AgreementId"])
			}
		})
	}
}

func TestHandler_CreateAgreement_MissingServerID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
		"ServerId": "s-doesnotexist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- DeleteAgreement ---

func TestHandler_DeleteAgreement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) (serverID, agreementID string)
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)
				ag, _ := h.Backend.CreateAgreement(s.ServerID, "desc", "p-local", "p-partner", "/base", "arn:role", nil)

				return s.ServerID, ag.AgreementID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "agreement not found",
			setup: func(h *transfer.Handler) (string, string) {
				s, _ := h.Backend.CreateServer(nil, nil)

				return s.ServerID, "a-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "server not found",
			setup: func(_ *transfer.Handler) (string, string) {
				return "s-doesnotexist", "a-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serverID, agreementID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteAgreement", map[string]any{
				"ServerId":    serverID,
				"AgreementId": agreementID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteAgreement_MissingFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing server id",
			body:     map[string]any{"AgreementId": "a-123"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing agreement id",
			body:     map[string]any{"ServerId": s.ServerID},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "DeleteAgreement", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- CreateConnector ---

func TestHandler_CreateConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success with access role",
			body: map[string]any{
				"Url":        "https://partner.example.com",
				"AccessRole": "arn:aws:iam::123456789012:role/connector-role",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "success minimal",
			body:     map[string]any{"Url": "https://minimal.example.com"},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing url",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateConnector", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["ConnectorId"])
			}
		})
	}
}

// --- DeleteConnector ---

func TestHandler_DeleteConnector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *transfer.Handler) string {
				c, _ := h.Backend.CreateConnector("https://example.com", "", nil, nil, nil)

				return c.ConnectorID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *transfer.Handler) string {
				return "c-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing connector id",
			setup: func(_ *transfer.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connectorID := tt.setup(h)

			rec := doTransferRequest(t, h, "DeleteConnector", map[string]any{
				"ConnectorId": connectorID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- CreateProfile ---

func TestHandler_CreateProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "local profile",
			body: map[string]any{
				"ProfileType": "LOCAL",
				"As2Id":       "AS2-local-id",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "partner profile",
			body: map[string]any{
				"ProfileType": "PARTNER",
				"As2Id":       "AS2-partner-id",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "missing profile type",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateProfile", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["ProfileId"])
			}
		})
	}
}

// --- CreateWebApp ---

func TestHandler_CreateWebApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success no tags",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "success with tags",
			body: map[string]any{
				"Tags": []map[string]string{{"Key": "env", "Value": "test"}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWebApp", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["WebAppId"])
		})
	}
}

// --- CreateWorkflow ---

func TestHandler_CreateWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "success no description",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "success with description",
			body: map[string]any{
				"Description": "my workflow",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success with tags",
			body: map[string]any{
				"Description": "tagged workflow",
				"Tags":        []map[string]string{{"Key": "team", "Value": "data"}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWorkflow", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["WorkflowId"])
		})
	}
}

// --- DeleteCertificate ---

func TestHandler_DeleteCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*transfer.InMemoryBackend) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(b *transfer.InMemoryBackend) string {
				certID := "cert-abc123"
				b.AddCertificateInternal(certID)

				return certID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *transfer.InMemoryBackend) string {
				return "cert-doesnotexist"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing certificate id",
			setup: func(_ *transfer.InMemoryBackend) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transfer.NewInMemoryBackend(t.Context(), testAccountID, testRegion)
			h := transfer.NewHandler(b)
			certID := tt.setup(b)

			rec := doTransferRequest(t, h, "DeleteCertificate", map[string]any{
				"CertificateId": certID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Backend unit tests ---

func TestBackend_CreateAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		tags       map[string]string
		name       string
		serverID   string
		externalID string
		role       string
		homeDir    string
		wantErr    bool
	}{
		{
			name:       "success",
			externalID: "S-1-5-21-1234",
			role:       "arn:aws:iam::123456789012:role/role",
			homeDir:    "/home",
		},
		{
			name:       "server not found",
			serverID:   "s-doesnotexist",
			externalID: "S-1-5-21-1234",
			wantErr:    true,
			errTarget:  awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			serverID := tt.serverID
			if serverID == "" {
				s, err := b.CreateServer(nil, nil)
				require.NoError(t, err)
				serverID = s.ServerID
			}

			a, err := b.CreateAccess(serverID, tt.externalID, tt.role, tt.homeDir, tt.tags)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.externalID, a.ExternalID)
			assert.Equal(t, serverID, a.ServerID)
			assert.Equal(t, tt.role, a.Role)
			assert.Equal(t, tt.homeDir, a.HomeDir)
		})
	}
}

func TestBackend_DeleteAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		serverID  string
		wantErr   bool
	}{
		{
			name: "success",
		},
		{
			name:      "server not found",
			serverID:  "s-doesnotexist",
			wantErr:   true,
			errTarget: awserr.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			serverID := tt.serverID
			externalID := "S-1-5-21-1234"

			if serverID == "" {
				s, err := b.CreateServer(nil, nil)
				require.NoError(t, err)
				serverID = s.ServerID
				_, err = b.CreateAccess(serverID, externalID, "", "", nil)
				require.NoError(t, err)
			}

			err := b.DeleteAccess(serverID, externalID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestBackend_DeleteAccess_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	err = b.DeleteAccess(s.ServerID, "S-missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_CreateAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	ag, err := b.CreateAgreement(
		s.ServerID,
		"test agreement",
		"p-local",
		"p-partner",
		"/base",
		"arn:role",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, ag.AgreementID)
	assert.Equal(t, s.ServerID, ag.ServerID)
	assert.Equal(t, "test agreement", ag.Description)
	assert.Equal(t, "ACTIVE", ag.Status)
}

func TestBackend_CreateAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	_, err := b.CreateAgreement("s-doesnotexist", "", "", "", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_DeleteAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	s, err := b.CreateServer(nil, nil)
	require.NoError(t, err)

	ag, err := b.CreateAgreement(s.ServerID, "", "", "", "", "", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteAgreement(s.ServerID, ag.AgreementID))

	// Double delete should fail
	err = b.DeleteAgreement(s.ServerID, ag.AgreementID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_DeleteAgreement_ServerNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	err := b.DeleteAgreement("s-doesnotexist", "a-123")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_CreateConnector(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	c, err := b.CreateConnector("https://example.com", "arn:role", nil, nil, map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.NotEmpty(t, c.ConnectorID)
	assert.Equal(t, "https://example.com", c.URL)
	assert.Equal(t, "arn:role", c.AccessRole)
}

func TestBackend_DeleteConnector(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	c, err := b.CreateConnector("https://example.com", "", nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteConnector(c.ConnectorID))

	// Double delete should fail
	err = b.DeleteConnector(c.ConnectorID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_CreateProfile(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	p, err := b.CreateProfile("LOCAL", "AS2-id", map[string]string{"env": "test"})
	require.NoError(t, err)
	assert.NotEmpty(t, p.ProfileID)
	assert.Equal(t, "LOCAL", p.ProfileType)
	assert.Equal(t, "AS2-id", p.As2ID)
}

func TestBackend_CreateWebApp(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	w, err := b.CreateWebApp(map[string]string{"env": "prod"})
	require.NoError(t, err)
	assert.NotEmpty(t, w.WebAppID)
}

func TestBackend_CreateWorkflow(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	wf, err := b.CreateWorkflow("my workflow", nil, nil, map[string]string{"team": "data"})
	require.NoError(t, err)
	assert.NotEmpty(t, wf.WorkflowID)
	assert.Equal(t, "my workflow", wf.Description)
}

func TestBackend_DeleteCertificate(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	certID := "cert-abc123"
	b.AddCertificateInternal(certID)

	require.NoError(t, b.DeleteCertificate(certID))

	// Double delete should fail
	err := b.DeleteCertificate(certID)
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestBackend_DeleteCertificate_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	err := b.DeleteCertificate("cert-doesnotexist")
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}
