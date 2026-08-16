package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestHandler_GetPolicyVersion_VersionIdNotHardcoded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestedVersion string
		wantVersionID    string
		setAsDefault     bool
	}{
		{"v1", "v1", "v1", false},
		{"v2 non-default", "v2", "v2", false},
		{"v2 set as default", "v2", "v2", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPVPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
			_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
			require.NoError(t, err)

			if tc.setAsDefault {
				require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))
			}

			req := iamRequest("GetPolicyVersion", map[string]string{
				"PolicyArn": p.Arn,
				"VersionId": tc.requestedVersion,
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<VersionId>"+tc.wantVersionID+"</VersionId>",
				"GetPolicyVersion response VersionId must match requested version, not hardcoded v1")
		})
	}
}

func TestHandler_GetPolicy_ReturnsAWSFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setup            func(b *iam.InMemoryBackend, policyArn string)
		wantDefaultVer   string
		wantAttachCount  string
		wantIsAttachable string
	}{
		{
			name:             "fresh policy",
			setup:            func(_ *iam.InMemoryBackend, _ string) {},
			wantDefaultVer:   "v1",
			wantAttachCount:  "0",
			wantIsAttachable: "true",
		},
		{
			name: "after attach to user",
			setup: func(b *iam.InMemoryBackend, policyArn string) {
				_, _ = b.CreateUser("gp-fields-user", "/", "")
				_ = b.AttachUserPolicy("gp-fields-user", policyArn)
			},
			wantDefaultVer:   "v1",
			wantAttachCount:  "1",
			wantIsAttachable: "true",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)

			doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
			p, err := b.CreatePolicy("GPFieldsPolicy-"+tc.name, "/", doc)
			require.NoError(t, err)

			tc.setup(b, p.Arn)

			req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<DefaultVersionId>"+tc.wantDefaultVer+"</DefaultVersionId>",
				"GetPolicy must include DefaultVersionId")
			assert.Contains(t, body, "<AttachmentCount>"+tc.wantAttachCount+"</AttachmentCount>",
				"GetPolicy must include AttachmentCount")
			assert.Contains(t, body, "<IsAttachable>"+tc.wantIsAttachable+"</IsAttachable>",
				"GetPolicy must include IsAttachable")
			assert.Contains(t, body, "<UpdateDate>",
				"GetPolicy must include UpdateDate")
		})
	}
}

func TestHandler_GetPolicy_DefaultVersionIdUpdatesAfterNewDefault(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, err := b.CreatePolicy("GPDefVer", "/", doc)
	require.NoError(t, err)

	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	_, err = b.CreatePolicyVersion(p.Arn, doc2, false)
	require.NoError(t, err)

	require.NoError(t, b.SetDefaultPolicyVersion(p.Arn, "v2"))

	req := iamRequest("GetPolicy", map[string]string{"PolicyArn": p.Arn})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<DefaultVersionId>v2</DefaultVersionId>",
		"DefaultVersionId must reflect v2 after SetDefaultPolicyVersion")
	assert.NotContains(t, body, "<DefaultVersionId>v1</DefaultVersionId>",
		"DefaultVersionId must not still say v1 after v2 was set as default")
}

func TestCreatePolicyVersion_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *iam.InMemoryBackend) string
		name          string
		policyDoc     string
		wantVersionID string
		setAsDefault  bool
		wantErr       bool
	}{
		{
			name: "create_version_success",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"ReadOnly",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantVersionID: "v2",
		},
		{
			name: "create_version_set_as_default",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"WritePolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			setAsDefault:  true,
			wantVersionID: "v2",
		},
		{
			name: "policy_not_found_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/NonExistent"
			},
			policyDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantErr:   true,
		},
		{
			name: "empty_document_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/SomePolicy"
			},
			policyDoc: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			policyArn := tt.setup(b)

			pv, err := b.CreatePolicyVersion(policyArn, tt.policyDoc, tt.setAsDefault)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, pv)
			assert.Equal(t, tt.wantVersionID, pv.VersionID)
			assert.Equal(t, tt.setAsDefault, pv.IsDefaultVersion)
		})
	}
}

func TestIAMHandler_ListPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params           map[string]string
		name             string
		wantBodyContains []string
		wantCode         int
		setupData        bool
	}{
		{
			name:      "returns_multiple_versions",
			setupData: true,
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			wantCode: http.StatusOK,
			wantBodyContains: []string{
				"<VersionId>v1</VersionId>",
				"<VersionId>v2</VersionId>",
				"<IsDefaultVersion>false</IsDefaultVersion>",
				"<IsDefaultVersion>true</IsDefaultVersion>",
			},
		},
		{
			name: "policy_not_found_returns_404",
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setupData {
				pol, setupErr := b.CreatePolicy(
					"VersionListPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, setupErr)
				_, setupErr = b.CreatePolicyVersion(
					pol.Arn,
					`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
					true,
				)
				require.NoError(t, setupErr)
			}

			req := iamRequest("ListPolicyVersions", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, expected := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), expected)
			}
		})
	}
}

func TestHandler_PolicyVersion_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("VersionedPolicy", "/", doc)

	// CreatePolicyVersion.
	doc2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc2,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "CreatePolicyVersion must succeed")

	// ListPolicyVersions.
	req2 := iamRequest("ListPolicyVersions", map[string]string{"PolicyArn": p.Arn})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "v1")
	assert.Contains(t, rec2.Body.String(), "v2")

	// GetPolicyVersion.
	req3 := iamRequest("GetPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
	// Real AWS IAM returns PolicyVersion.Document URL-encoded (RFC 3986); "s3:*"
	// becomes "s3%3A%2A" on the wire.
	assert.Contains(t, rec3.Body.String(), "s3%3A%2A")

	// SetDefaultPolicyVersion.
	req4 := iamRequest("SetDefaultPolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v2",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeletePolicyVersion (non-default).
	req5 := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	require.Equal(t, http.StatusOK, rec5.Code)

	req6 := iamRequest("DeletePolicyVersion", map[string]string{
		"PolicyArn": p.Arn,
		"VersionId": "v1",
	})
	rec6 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req6, rec6)))
	assert.Equal(t, http.StatusOK, rec6.Code)
}

func TestHandler_PolicyVersion_LimitExceeded(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	p, _ := b.CreatePolicy("LP", "/", doc)

	// Create 4 more versions to hit the cap (v1 + v2 + v3 + v4 + v5 = 5).
	for range 4 {
		req := iamRequest("CreatePolicyVersion", map[string]string{
			"PolicyArn":      p.Arn,
			"PolicyDocument": doc,
			"SetAsDefault":   "false",
		})
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, "must succeed under limit")
	}

	// 6th version must fail.
	req := iamRequest("CreatePolicyVersion", map[string]string{
		"PolicyArn":      p.Arn,
		"PolicyDocument": doc,
		"SetAsDefault":   "false",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	// Real AWS IAM returns 409 for LimitExceeded, not 400.
	assert.Equal(t, http.StatusConflict, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceeded", errResp.Error.Code)
}
