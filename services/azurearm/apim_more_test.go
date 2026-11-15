package azurearm_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMGenericEntities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		body        string
		wantContain string
		wantAbsent  string
		wantStatus  int
	}{
		{
			name:        "tag",
			path:        "/tags/t1",
			body:        `{"properties":{"displayName":"Payments"}}`,
			wantStatus:  201,
			wantContain: "Payments",
		},
		{name: "tag without name", path: "/tags/t2", body: `{"properties":{}}`, wantStatus: 400},
		{
			name:        "group defaults to custom",
			path:        "/groups/g1",
			body:        `{"properties":{"displayName":"Ops"}}`,
			wantStatus:  201,
			wantContain: `"type":"custom"`,
		},
		{
			name:       "group bad type",
			path:       "/groups/g2",
			body:       `{"properties":{"displayName":"x","type":"system"}}`,
			wantStatus: 400,
		},
		{
			name:        "logger masks credentials",
			path:        "/loggers/l1",
			body:        `{"properties":{"loggerType":"applicationInsights","credentials":{"instrumentationKey":"secret-key"}}}`,
			wantStatus:  201,
			wantContain: "{{Logger-Credentials--",
			wantAbsent:  "secret-key",
		},
		{
			name:       "logger bad type",
			path:       "/loggers/l2",
			body:       `{"properties":{"loggerType":"kafka","credentials":{"a":"b"}}}`,
			wantStatus: 400,
		},
		{
			name: "version set query needs a name", path: "/apiVersionSets/v1",
			body: `{"properties":{"displayName":"v","versioningScheme":"Query"}}`, wantStatus: 400,
		},
		{
			name:        "version set header",
			path:        "/apiVersionSets/v2",
			body:        `{"properties":{"displayName":"v","versioningScheme":"Header","versionHeaderName":"api-version"}}`,
			wantStatus:  201,
			wantContain: "api-version",
		},
		{
			name: "fragment",
			path: "/policyFragments/f1",
			body: `{"properties":{"format":"rawxml","value":"<fragment><set-header name=\"a\" ` +
				`exists-action=\"override\"><value>b</value></set-header></fragment>"}}`,
			wantStatus: 201,
		},
		{
			name:       "fragment malformed",
			path:       "/policyFragments/f2",
			body:       `{"properties":{"value":"<fragment><a></fragment>"}}`,
			wantStatus: 400,
		},
		{
			name:       "fragment wrong root",
			path:       "/policyFragments/f3",
			body:       `{"properties":{"value":"<policies/>"}}`,
			wantStatus: 400,
		},
		{
			name: "user hides password", path: "/users/u1",
			body:       `{"properties":{"email":"a@b.io","firstName":"A","lastName":"B","password":"pw-secret"}}`,
			wantStatus: 201, wantContain: `"state":"active"`, wantAbsent: "pw-secret",
		},
		{
			name:       "user bad email",
			path:       "/users/u2",
			body:       `{"properties":{"email":"nope","firstName":"A","lastName":"B"}}`,
			wantStatus: 400,
		},
		{
			name: "identity provider hides secret", path: "/identityProviders/google",
			body:       `{"properties":{"clientId":"cid","clientSecret":"sh-secret"}}`,
			wantStatus: 201, wantContain: "cid", wantAbsent: "sh-secret",
		},
		{
			name:       "identity provider unknown",
			path:       "/identityProviders/myspace",
			body:       `{"properties":{"clientId":"c","clientSecret":"s"}}`,
			wantStatus: 400,
		},
		{
			name: "authorization server", path: "/authorizationServers/a1",
			body: `{"properties":{"displayName":"as","clientRegistrationEndpoint":"https://x.io/reg",` +
				`"authorizationEndpoint":"https://x.io/auth","grantTypes":["implicit"],"clientId":"c"}}`,
			wantStatus: 201,
		},
		{
			name: "authorization server bad grant", path: "/authorizationServers/a2",
			body: `{"properties":{"displayName":"as","clientRegistrationEndpoint":"https://x.io/reg",` +
				`"authorizationEndpoint":"https://x.io/auth","grantTypes":["magic"],"clientId":"c"}}`,
			wantStatus: 400,
		},
		{
			name:       "cache",
			path:       "/caches/c1",
			body:       `{"properties":{"connectionString":"redis:6379","useFromLocation":"default"}}`,
			wantStatus: 201,
		},
		{
			name:       "oidc",
			path:       "/openidConnectProviders/o1",
			body:       `{"properties":{"displayName":"o","metadataEndpoint":"https://x.io/.well-known","clientId":"c"}}`,
			wantStatus: 201,
		},
		{
			name:       "oidc relative endpoint",
			path:       "/openidConnectProviders/o2",
			body:       `{"properties":{"displayName":"o","metadataEndpoint":"/x","clientId":"c"}}`,
			wantStatus: 400,
		},
	}

	s := newAPIMStack(t)
	s.devService(t)

	for _, tc := range tests {
		status, body, _ := s.raw(t, http.MethodPut, s.armPath(tc.path), tc.body, nil)
		assert.Equal(t, tc.wantStatus, status, tc.name+": "+body)

		if tc.wantContain != "" {
			assert.Contains(t, body, tc.wantContain, tc.name)
		}

		if tc.wantAbsent != "" {
			assert.NotContains(t, body, tc.wantAbsent, tc.name)
		}
	}

	status, body, _ := s.raw(t, http.MethodGet, s.armPath("/tags"), "", nil)
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "Payments")
}

func TestAPIMPortalSettings(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	for _, id := range []string{"signin", "signup", "delegation"} {
		status, body, _ := s.raw(t, http.MethodGet, s.armPath("/portalsettings/"+id), "", nil)
		assert.Equal(t, http.StatusOK, status, id+": "+body)
		assert.NotContains(t, body, "validationKey")
	}

	status, body, _ := s.raw(
		t,
		http.MethodPut,
		s.armPath("/portalsettings/signin"),
		`{"properties":{"enabled":true}}`,
		nil,
	)
	assert.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, `"enabled":true`)

	status, _, _ = s.raw(
		t,
		http.MethodPut,
		s.armPath("/portalsettings/signin"),
		`{"properties":{"enabled":"yes"}}`,
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _, _ = s.raw(t, http.MethodPut, s.armPath("/portalsettings/other"), `{"properties":{"enabled":true}}`, nil)
	assert.Equal(t, http.StatusBadRequest, status)

	status, body, _ = s.raw(t, http.MethodPost, s.armPath("/portalsettings/delegation/listSecrets"), "", nil)
	assert.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "validationKey")

	status, body, _ = s.raw(t, http.MethodGet, s.armPath("/tenant/access"), "", nil)
	assert.Equal(t, http.StatusOK, status, body)
	assert.NotContains(t, body, "primaryKey")

	_, before, _ := s.raw(t, http.MethodPost, s.armPath("/tenant/access/listSecrets"), "", nil)
	assert.Contains(t, before, "primaryKey")

	status, _, _ = s.raw(t, http.MethodPost, s.armPath("/tenant/access/regeneratePrimaryKey"), "", nil)
	assert.Equal(t, http.StatusNoContent, status)

	_, after, _ := s.raw(t, http.MethodPost, s.armPath("/tenant/access/listSecrets"), "", nil)
	assert.NotEqual(t, before, after, "regenerating changes the primary key")
}

func TestAPIMDeleteIfMatch(t *testing.T) {
	t.Parallel()

	s := newAPIMStack(t)
	s.devService(t)

	for i, tc := range []struct {
		hdr  map[string]string
		name string
		want int
	}{
		{name: "no header", hdr: nil, want: http.StatusOK},
		{name: "empty header", hdr: map[string]string{"If-Match": ""}, want: http.StatusOK},
		{name: "star", hdr: map[string]string{"If-Match": "*"}, want: http.StatusOK},
		{name: "stale etag", hdr: map[string]string{"If-Match": `"stale"`}, want: http.StatusPreconditionFailed},
	} {
		status, _, _ := s.raw(
			t,
			http.MethodPut,
			s.armPath("/tags/t"+strconv.Itoa(i)),
			`{"properties":{"displayName":"x"}}`,
			nil,
		)
		require.Equal(t, http.StatusCreated, status, tc.name)

		status, _, _ = s.raw(t, http.MethodDelete, s.armPath("/tags/t"+strconv.Itoa(i)), "", tc.hdr)
		assert.Equal(t, tc.want, status, tc.name)
	}
}
