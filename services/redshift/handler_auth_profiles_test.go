package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateAuthenticationProfile ----

func TestHandler_CreateAuthenticationProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateAuthenticationProfile&Version=2012-12-01" +
				"&AuthenticationProfileName=myprofile&AuthenticationProfileContent={\"x\":1}",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateAuthenticationProfileResponse", "myprofile"},
		},
		{
			name:     "missing_name",
			body:     "Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteAuthenticationProfile ----

func TestHandler_DeleteAuthenticationProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=prof-del",
				)
			},
			body:     "Action=DeleteAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=prof-del",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     "Action=DeleteAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=missing",
			wantCode: http.StatusBadRequest,
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
		})
	}
}

// ---- DescribeAuthenticationProfiles ----

func TestHandler_DescribeAuthenticationProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeAuthenticationProfiles&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAuthenticationProfilesResponse"},
		},
		{
			name: "with_profile",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=p1",
				)
			},
			body:         "Action=DescribeAuthenticationProfiles&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"p1"},
		},
		{
			name:     "not_found_filter",
			body:     "Action=DescribeAuthenticationProfiles&Version=2012-12-01&AuthenticationProfileName=missing",
			wantCode: http.StatusBadRequest,
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

// ---- ModifyAuthenticationProfile ----

func TestHandler_ModifyAuthenticationProfile(t *testing.T) {
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
					"Action=CreateAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=mod-prof",
				)
			},
			body: "Action=ModifyAuthenticationProfile&Version=2012-12-01" +
				"&AuthenticationProfileName=mod-prof&AuthenticationProfileContent={}",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyAuthenticationProfileResponse", "mod-prof"},
		},
		{
			name:     "not_found",
			body:     "Action=ModifyAuthenticationProfile&Version=2012-12-01&AuthenticationProfileName=missing",
			wantCode: http.StatusBadRequest,
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
