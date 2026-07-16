package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateIdcApplication ----

func TestHandler_CreateIdcApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=my-app" +
				"&IdcInstanceArn=arn:aws:sso:::instance/abc" +
				"&IamRoleArn=arn:aws:iam::123:role/MyRole",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIdcApplicationResponse", "my-app"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationAlreadyExistsFault"},
		},
		{
			name: "missing_name",
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=&IdcInstanceArn=arn:idc",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteIdcApplication ----

func TestHandler_DeleteIdcApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=del-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=DeleteIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/del-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteIdcApplicationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationNotExistsFault"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteIdcApplication&Version=2012-12-01&IdcApplicationArn=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeIdcApplications ----

func TestHandler_DescribeIdcApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=app-a&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=app-b&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body:         "Action=DescribeIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIdcApplicationsResponse", "app-a", "app-b"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIdcApplicationsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyIdcApplication ----

func TestHandler_ModifyIdcApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=mod-app&IdcInstanceArn=arn:idc"+
						"&IdcDisplayName=OldName&IamRoleArn=arn:old-role",
				)
			},
			body: "Action=ModifyIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/mod-app" +
				"&IdcDisplayName=NewName&IamRoleArn=arn:new-role",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyIdcApplicationResponse", "mod-app"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyIdcApplication&Version=2012-12-01&IdcApplicationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationNotExistsFault"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend: IdcApplication count tracking ----

func TestBackend_IdcApplication_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.IdcApplicationCount(b))

	postRedshiftForm(
		t,
		h,
		"Action=CreateIdcApplication&"+
			"Version=2012-12-01&IdcApplicationName=app1&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
	)

	assert.Equal(t, 1, redshift.IdcApplicationCount(b))

	postRedshiftForm(
		t,
		h,
		"Action=DeleteIdcApplication&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/app1",
	)

	assert.Equal(t, 0, redshift.IdcApplicationCount(b))
}

// ---- CRUD Lifecycle ----

func TestHandler_IdcApplication_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateIdcApplication&"+
			"Version=2012-12-01&IdcApplicationName=lc-app"+
			"&IdcInstanceArn=arn:aws:sso:::instance/abc&IdcDisplayName=LifecycleApp"+
			"&IamRoleArn=arn:aws:iam::123:role/MyRole",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeIdcApplications&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Modify
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyIdcApplication&"+
			"Version=2012-12-01"+
			"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app"+
			"&IdcDisplayName=UpdatedApp",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteIdcApplication&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeIdcApplications&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IdcApplicationNotExistsFault")
}

// ---- GetIdentityCenterAuthToken ----

func TestHandler_GetIdentityCenterAuthToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "missing_arn_returns_400",
			body:     "Action=GetIdentityCenterAuthToken&Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_arn_returns_token",
			body: "Action=GetIdentityCenterAuthToken&Version=2012-12-01" +
				"&IdentityCenterApplicationArn=arn:aws:sso::123:application/app-abc",
			wantCode:     http.StatusOK,
			wantContains: []string{"<AuthToken>ict-", "<AuthTokenExpiration>"},
		},
		{
			name: "different_arn_returns_different_token",
			body: "Action=GetIdentityCenterAuthToken&Version=2012-12-01" +
				"&IdentityCenterApplicationArn=arn:aws:sso::456:application/app-xyz",
			wantCode:     http.StatusOK,
			wantContains: []string{"<AuthToken>ict-"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			for _, want := range tc.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
