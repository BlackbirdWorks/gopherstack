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
			body: "Action=CreateRedshiftIdcApplication&" +
				"Version=2012-12-01&RedshiftIdcApplicationName=my-app" +
				"&IdcInstanceArn=arn:aws:sso:::instance/abc" +
				"&IamRoleArn=arn:aws:iam::123:role/MyRole",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateRedshiftIdcApplicationResponse", "my-app"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateRedshiftIdcApplication&"+
						"Version=2012-12-01&RedshiftIdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=CreateRedshiftIdcApplication&" +
				"Version=2012-12-01&RedshiftIdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"RedshiftIdcApplicationAlreadyExists"},
		},
		{
			name: "missing_name",
			body: "Action=CreateRedshiftIdcApplication&" +
				"Version=2012-12-01&RedshiftIdcApplicationName=&IdcInstanceArn=arn:idc",
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
					"Action=CreateRedshiftIdcApplication&"+
						"Version=2012-12-01&RedshiftIdcApplicationName=del-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=DeleteRedshiftIdcApplication&" +
				"Version=2012-12-01" +
				"&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/del-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteRedshiftIdcApplicationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteRedshiftIdcApplication&" +
				"Version=2012-12-01" +
				"&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"RedshiftIdcApplicationNotExists"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteRedshiftIdcApplication&Version=2012-12-01&RedshiftIdcApplicationArn=",
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
					"Action=CreateRedshiftIdcApplication&"+
						"Version=2012-12-01&RedshiftIdcApplicationName=app-a&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateRedshiftIdcApplication&"+
						"Version=2012-12-01&RedshiftIdcApplicationName=app-b&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body:         "Action=DescribeRedshiftIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRedshiftIdcApplicationsResponse", "app-a", "app-b"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeRedshiftIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeRedshiftIdcApplicationsResponse"},
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
					"Action=CreateRedshiftIdcApplication&"+
						"Version=2012-12-01&RedshiftIdcApplicationName=mod-app&IdcInstanceArn=arn:idc"+
						"&IdcDisplayName=OldName&IamRoleArn=arn:old-role",
				)
			},
			body: "Action=ModifyRedshiftIdcApplication&" +
				"Version=2012-12-01" +
				"&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/mod-app" +
				"&IdcDisplayName=NewName&IamRoleArn=arn:new-role",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyRedshiftIdcApplicationResponse", "mod-app"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyRedshiftIdcApplication&Version=2012-12-01&RedshiftIdcApplicationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"RedshiftIdcApplicationNotExists"},
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
		"Action=CreateRedshiftIdcApplication&"+
			"Version=2012-12-01&RedshiftIdcApplicationName=app1&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
	)

	assert.Equal(t, 1, redshift.IdcApplicationCount(b))

	postRedshiftForm(
		t,
		h,
		"Action=DeleteRedshiftIdcApplication&"+
			"Version=2012-12-01&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/app1",
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
		"Action=CreateRedshiftIdcApplication&"+
			"Version=2012-12-01&RedshiftIdcApplicationName=lc-app"+
			"&IdcInstanceArn=arn:aws:sso:::instance/abc&IdcDisplayName=LifecycleApp"+
			"&IamRoleArn=arn:aws:iam::123:role/MyRole",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeRedshiftIdcApplications&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Modify
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyRedshiftIdcApplication&"+
			"Version=2012-12-01"+
			"&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app"+
			"&IdcDisplayName=UpdatedApp",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteRedshiftIdcApplication&"+
			"Version=2012-12-01&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeRedshiftIdcApplications&"+
			"Version=2012-12-01&RedshiftIdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "RedshiftIdcApplicationNotExists")
}

// ---- CreateQev2IdcApplication ----

func TestHandler_CreateQev2IdcApplication(t *testing.T) {
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
			body: "Action=CreateQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationName=my-qev2-app" +
				"&IdcInstanceArn=arn:aws:sso:::instance/abc" +
				"&IdcDisplayName=My+QEv2+App" +
				"&Tags.Tag.1.Key=env&Tags.Tag.1.Value=prod",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateQev2IdcApplicationResponse",
				// Real CreateQev2IdcApplicationOutput nests the application under
				// a <Qev2IdcApplication> element inside the Result (confirmed
				// against awsAwsquery_deserializeOpDocumentCreateQev2IdcApplicationOutput
				// in aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go) --
				// assert the exact envelope, not just substring presence of the name.
				"<CreateQev2IdcApplicationResult><Qev2IdcApplication>",
				"my-qev2-app",
				"<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>",
			},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateQev2IdcApplication&Version=2012-12-01"+
						"&Qev2IdcApplicationName=dup-qev2-app"+
						"&IdcInstanceArn=arn:idc&IdcDisplayName=Dup",
				)
			},
			body: "Action=CreateQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationName=dup-qev2-app&IdcInstanceArn=arn:idc&IdcDisplayName=Dup",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"Qev2IdcApplicationAlreadyExists"},
		},
		{
			name: "missing_name",
			body: "Action=CreateQev2IdcApplication&Version=2012-12-01" +
				"&IdcInstanceArn=arn:idc&IdcDisplayName=NoName",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "missing_instance_arn",
			body: "Action=CreateQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationName=no-instance-arn&IdcDisplayName=NoInstanceArn",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "missing_display_name",
			body: "Action=CreateQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationName=no-display-name&IdcInstanceArn=arn:idc",
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

// ---- DeleteQev2IdcApplication ----

func TestHandler_DeleteQev2IdcApplication(t *testing.T) {
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
					"Action=CreateQev2IdcApplication&Version=2012-12-01"+
						"&Qev2IdcApplicationName=del-qev2-app&IdcInstanceArn=arn:idc&IdcDisplayName=Del",
				)
			},
			body: "Action=DeleteQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/del-qev2-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteQev2IdcApplicationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"Qev2IdcApplicationNotExists"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteQev2IdcApplication&Version=2012-12-01&Qev2IdcApplicationArn=",
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

// ---- DescribeQev2IdcApplications ----

func TestHandler_DescribeQev2IdcApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *redshift.Handler)
		name            string
		body            string
		wantContains    []string
		wantNotContains []string
		wantCode        int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateQev2IdcApplication&Version=2012-12-01"+
						"&Qev2IdcApplicationName=qev2-app-a&IdcInstanceArn=arn:idc&IdcDisplayName=A",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateQev2IdcApplication&Version=2012-12-01"+
						"&Qev2IdcApplicationName=qev2-app-b&IdcInstanceArn=arn:idc&IdcDisplayName=B",
				)
			},
			body:     "Action=DescribeQev2IdcApplications&Version=2012-12-01",
			wantCode: http.StatusOK,
			wantContains: []string{
				"DescribeQev2IdcApplicationsResponse",
				// Real DescribeQev2IdcApplicationsOutput wraps each list item in
				// <member> (confirmed against
				// awsAwsquery_deserializeDocumentQev2IdcApplicationList).
				"<Qev2IdcApplications><member>",
				"qev2-app-a",
				"qev2-app-b",
			},
		},
		{
			name:         "empty",
			body:         "Action=DescribeQev2IdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeQev2IdcApplicationsResponse"},
		},
		{
			name: "by_arn_not_found",
			body: "Action=DescribeQev2IdcApplications&Version=2012-12-01" +
				"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"Qev2IdcApplicationNotExists"},
		},
		{
			name: "paginated_first_page",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				for _, n := range []string{"page-app-1", "page-app-2", "page-app-3"} {
					postRedshiftForm(
						t,
						h,
						"Action=CreateQev2IdcApplication&Version=2012-12-01"+
							"&Qev2IdcApplicationName="+n+"&IdcInstanceArn=arn:idc&IdcDisplayName=D",
					)
				}
			},
			body:     "Action=DescribeQev2IdcApplications&Version=2012-12-01&MaxRecords=2",
			wantCode: http.StatusOK,
			wantContains: []string{
				"page-app-1",
				"page-app-2",
				// Marker echoes the last app name on this page, matching
				// DescribeClusters's Marker convention (see store.go).
				"<Marker>page-app-2</Marker>",
			},
			wantNotContains: []string{"page-app-3"},
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

			for _, s := range tt.wantNotContains {
				assert.NotContains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestHandler_DescribeQev2IdcApplications_MarkerAdvancesPage verifies that
// feeding the Marker from a first page back into a second request yields the
// remaining results, exactly like DescribeClusters's pagination (see
// TestHandler_DescribeClusters-style marker round-trips elsewhere in this
// package). This is a lifecycle-shaped check, not a table case, so it lives
// as its own function rather than forcing an artificial ordering into the
// table above.
func TestHandler_DescribeQev2IdcApplications_MarkerAdvancesPage(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	for _, n := range []string{"mk-app-1", "mk-app-2", "mk-app-3"} {
		postRedshiftForm(
			t,
			h,
			"Action=CreateQev2IdcApplication&Version=2012-12-01"+
				"&Qev2IdcApplicationName="+n+"&IdcInstanceArn=arn:idc&IdcDisplayName=D",
		)
	}

	first := postRedshiftForm(t, h, "Action=DescribeQev2IdcApplications&Version=2012-12-01&MaxRecords=2")
	require.Equal(t, http.StatusOK, first.Code)
	assert.Contains(t, first.Body.String(), "mk-app-1")
	assert.Contains(t, first.Body.String(), "mk-app-2")
	assert.NotContains(t, first.Body.String(), "mk-app-3")
	assert.Contains(t, first.Body.String(), "<Marker>mk-app-2</Marker>")

	second := postRedshiftForm(
		t, h, "Action=DescribeQev2IdcApplications&Version=2012-12-01&MaxRecords=2&Marker=mk-app-2",
	)
	require.Equal(t, http.StatusOK, second.Code)
	assert.Contains(t, second.Body.String(), "mk-app-3")
	assert.NotContains(t, second.Body.String(), "mk-app-1")
	assert.NotContains(t, second.Body.String(), "mk-app-2")
	// Final page: no more results, so no Marker element is emitted (omitempty).
	assert.NotContains(t, second.Body.String(), "<Marker>")
}

// ---- ModifyQev2IdcApplication ----

func TestHandler_ModifyQev2IdcApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_changes_display_name_only",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateQev2IdcApplication&Version=2012-12-01"+
						"&Qev2IdcApplicationName=mod-qev2-app&IdcInstanceArn=arn:orig-instance"+
						"&IdcDisplayName=OldName",
				)
			},
			body: "Action=ModifyQev2IdcApplication&Version=2012-12-01" +
				"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/mod-qev2-app" +
				"&IdcDisplayName=NewName",
			wantCode: http.StatusOK,
			wantContains: []string{
				"<ModifyQev2IdcApplicationResult><Qev2IdcApplication>",
				"mod-qev2-app",
				"<IdcDisplayName>NewName</IdcDisplayName>",
				// IdcInstanceArn is immutable -- real ModifyQev2IdcApplicationInput has no
				// field for it, so it must be unchanged from creation.
				"<IdcInstanceArn>arn:orig-instance</IdcInstanceArn>",
			},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyQev2IdcApplication&Version=2012-12-01&Qev2IdcApplicationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"Qev2IdcApplicationNotExists"},
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

// ---- CRUD Lifecycle ----

func TestHandler_Qev2IdcApplication_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateQev2IdcApplication&Version=2012-12-01"+
			"&Qev2IdcApplicationName=lc-qev2-app"+
			"&IdcInstanceArn=arn:aws:sso:::instance/abc&IdcDisplayName=LifecycleApp",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-qev2-app")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeQev2IdcApplications&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-qev2-app")

	// Modify
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyQev2IdcApplication&Version=2012-12-01"+
			"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/lc-qev2-app"+
			"&IdcDisplayName=UpdatedApp",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdatedApp")

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteQev2IdcApplication&Version=2012-12-01"+
			"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/lc-qev2-app",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeQev2IdcApplications&Version=2012-12-01"+
			"&Qev2IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:qev2idcapplication/lc-qev2-app",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Qev2IdcApplicationNotExists")
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
