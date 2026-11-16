package gcpbigquery_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bqv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type principalTransport struct {
	base http.RoundTripper
	user string
}

func (p principalTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:"+p.user)

	return p.base.RoundTrip(r)
}

func (s *sdkEnv) asUser(t *testing.T, user string) *sdkEnv {
	t.Helper()

	hc := &http.Client{Transport: principalTransport{base: testclient.New(t).Transport, user: user}}
	client, err := bigquery.NewClient(t.Context(), sdkProject, option.WithEndpoint(s.url), option.WithHTTPClient(hc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &sdkEnv{client: client, backend: s.backend, objects: s.objects, url: s.url}
}

func (s *sdkEnv) exec(t *testing.T, sql string) {
	t.Helper()

	job, err := s.client.Query(sql).Run(t.Context())
	require.NoError(t, err)

	st, err := job.Wait(t.Context())
	require.NoError(t, err)
	require.NoError(t, st.Err())
}

func (s *sdkEnv) policyService(t *testing.T) *bqv2.Service {
	t.Helper()

	svc, err := bqv2.NewService(t.Context(), option.WithEndpoint(s.url), option.WithoutAuthentication())
	require.NoError(t, err)

	return svc
}

func seedSales(t *testing.T, env *sdkEnv) {
	t.Helper()

	env.dataset(t, "rls")
	env.exec(t, "CREATE TABLE rls.sales (region STRING, owner STRING, amount INT64)")
	env.exec(t, "INSERT INTO rls.sales VALUES ('EU','alice@example.com',1),('US','bob@example.org',2),"+
		"('APAC','carol@other.net',3)")
}

func TestRowAccessPolicyDDLEnforcement(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)

	const count = "SELECT region FROM rls.sales ORDER BY region"

	env.exec(t, "CREATE ROW ACCESS POLICY eu_only ON rls.sales GRANT TO ('user:alice@example.com') "+
		"FILTER USING (region = 'EU')")
	env.exec(t, "CREATE ROW ACCESS POLICY org_us ON rls.sales GRANT TO ('domain:example.org') "+
		"FILTER USING (region = 'US')")
	env.exec(t, "CREATE ROW ACCESS POLICY self ON rls.sales GRANT TO ('allAuthenticatedUsers') "+
		"FILTER USING (owner = SESSION_USER())")

	tests := []struct {
		name string
		user string
		want [][]bigquery.Value
	}{
		{"alice", "alice@example.com", [][]bigquery.Value{{"EU"}}},
		{"domain", "bob@example.org", [][]bigquery.Value{{"US"}}},
		{"owner", "carol@other.net", [][]bigquery.Value{{"APAC"}}},
		{"nobody", "dave@nowhere.io", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, env.asUser(t, tt.user).rows(t, count))
		})
	}
}

func TestRowAccessPolicyUnion(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)
	env.exec(t, "CREATE ROW ACCESS POLICY a ON rls.sales GRANT TO ('user:alice@example.com') "+
		"FILTER USING (region = 'EU')")
	env.exec(t, "CREATE ROW ACCESS POLICY b ON rls.sales GRANT TO ('user:alice@example.com') "+
		"FILTER USING (region = 'US')")

	assert.Equal(t, [][]bigquery.Value{{"EU"}, {"US"}},
		env.asUser(t, "alice@example.com").rows(t, "SELECT region FROM rls.sales ORDER BY region"))
}

func TestRowAccessPolicyDDLLifecycle(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)

	env.exec(t, "CREATE ROW ACCESS POLICY p1 ON rls.sales GRANT TO ('user:a@x.com') FILTER USING (region = 'EU')")
	env.exec(t, "CREATE ROW ACCESS POLICY p2 ON rls.sales GRANT TO ('user:a@x.com') FILTER USING (region = 'US')")
	env.exec(t, "CREATE ROW ACCESS POLICY IF NOT EXISTS p2 ON rls.sales GRANT TO ('user:a@x.com') "+
		"FILTER USING (TRUE)")
	env.exec(t, "CREATE OR REPLACE ROW ACCESS POLICY p2 ON rls.sales GRANT TO ('user:b@x.com') "+
		"FILTER USING (region = 'APAC')")

	assert.Equal(t, [][]bigquery.Value{{"APAC"}}, env.asUser(t, "b@x.com").rows(t, "SELECT region FROM rls.sales"))

	env.exec(t, "DROP ROW ACCESS POLICY IF EXISTS nope ON rls.sales")
	env.exec(t, "DROP ROW ACCESS POLICY p1 ON rls.sales")

	_, err := env.client.Query("DROP ROW ACCESS POLICY p2 ON rls.sales").Read(t.Context())
	require.ErrorContains(t, err, "last row access policy")

	env.exec(t, "DROP ALL ROW ACCESS POLICIES ON rls.sales")
	assert.Len(t, env.asUser(t, "anyone@x.com").rows(t, "SELECT region FROM rls.sales"), 3)

	env.exec(t, "CREATE ROW ACCESS POLICY p9 ON rls.sales GRANT TO ('allUsers') FILTER USING (TRUE)")
	env.exec(t, "CREATE OR REPLACE TABLE rls.sales AS SELECT 'EU' AS region, 'x' AS owner, 1 AS amount")
	assert.Len(t, env.asUser(t, "anyone@x.com").rows(t, "SELECT region FROM rls.sales"), 1)
}

func TestRowAccessPolicyDDLErrors(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)
	env.exec(t, "CREATE ROW ACCESS POLICY p1 ON rls.sales GRANT TO ('user:a@x.com') FILTER USING (region = 'EU')")

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{
			"dup",
			"CREATE ROW ACCESS POLICY p1 ON rls.sales GRANT TO ('user:a@x.com') FILTER USING (TRUE)",
			"Already Exists",
		},
		{
			"badgrantee",
			"CREATE ROW ACCESS POLICY p3 ON rls.sales GRANT TO ('nobody') FILTER USING (TRUE)",
			"Invalid grantee",
		},
		{"notbool", "CREATE ROW ACCESS POLICY p3 ON rls.sales GRANT TO ('allUsers') FILTER USING (amount)", "BOOL"},
		{
			"badcol",
			"CREATE ROW ACCESS POLICY p3 ON rls.sales GRANT TO ('allUsers') FILTER USING (nope = 1)",
			"Unrecognized name",
		},
		{
			"missingtable",
			"CREATE ROW ACCESS POLICY p3 ON rls.nope GRANT TO ('allUsers') FILTER USING (TRUE)",
			"Not found",
		},
		{"dropmissing", "DROP ROW ACCESS POLICY nope ON rls.sales", "Not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.client.Query(tt.sql).Read(t.Context())
			require.ErrorContains(t, err, tt.msg)
		})
	}
}

func TestRowAccessPolicyREST(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)

	ctx := t.Context()
	svc := env.policyService(t)
	pol := func(id, filter string, grantees ...string) *bqv2.RowAccessPolicy {
		return &bqv2.RowAccessPolicy{
			RowAccessPolicyReference: &bqv2.RowAccessPolicyReference{
				ProjectId: sdkProject, DatasetId: "rls", TableId: "sales", PolicyId: id,
			},
			FilterPredicate: filter, Grantees: grantees,
		}
	}

	created, err := svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales",
		pol("eu", "region = 'EU'", "user:alice@example.com")).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "region = 'EU'", created.FilterPredicate)
	assert.NotEmpty(t, created.Etag)
	assert.NotEmpty(t, created.CreationTime)

	_, err = svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales",
		pol("us", "region = 'US'", "user:bob@example.org")).Context(ctx).Do()
	require.NoError(t, err)

	assert.Equal(t, [][]bigquery.Value{{"EU"}},
		env.asUser(t, "alice@example.com").rows(t, "SELECT region FROM rls.sales"))
	assert.Empty(t, env.asUser(t, "zed@example.com").rows(t, "SELECT region FROM rls.sales"))

	got, err := svc.RowAccessPolicies.Get(sdkProject, "rls", "sales", "eu").Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, created.Etag, got.Etag)

	list, err := svc.RowAccessPolicies.List(sdkProject, "rls", "sales").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.RowAccessPolicies, 2)
	assert.Equal(t, "eu", list.RowAccessPolicies[0].RowAccessPolicyReference.PolicyId)

	upd, err := svc.RowAccessPolicies.Update(sdkProject, "rls", "sales", "eu",
		pol("eu", "region = 'APAC'")).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "region = 'APAC'", upd.FilterPredicate)
	assert.NotEqual(t, created.Etag, upd.Etag)
	assert.Equal(t, [][]bigquery.Value{{"APAC"}},
		env.asUser(t, "alice@example.com").rows(t, "SELECT region FROM rls.sales"))

	var gerr *googleapi.Error

	_, err = svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales", pol("eu", "TRUE", "allUsers")).Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusConflict, gerr.Code)

	_, err = svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales",
		pol("bad", "nope = 1", "allUsers")).Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	_, err = svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales",
		pol("bad-id", "TRUE", "allUsers")).Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	_, err = svc.RowAccessPolicies.Get(sdkProject, "rls", "sales", "missing").Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusNotFound, gerr.Code)

	require.NoError(t, svc.RowAccessPolicies.Delete(sdkProject, "rls", "sales", "us").Context(ctx).Do())

	err = svc.RowAccessPolicies.Delete(sdkProject, "rls", "sales", "eu").Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)
	assert.Equal(t, http.StatusBadRequest, gerr.Code)

	require.NoError(t, svc.RowAccessPolicies.Delete(sdkProject, "rls", "sales", "eu").Force(true).Context(ctx).Do())
	assert.Len(t, env.asUser(t, "zed@example.com").rows(t, "SELECT region FROM rls.sales"), 3)

	for _, id := range []string{"a", "b"} {
		_, err = svc.RowAccessPolicies.Insert(sdkProject, "rls", "sales", pol(id, "TRUE", "allUsers")).Context(ctx).Do()
		require.NoError(t, err)
	}

	err = svc.RowAccessPolicies.BatchDelete(sdkProject, "rls", "sales",
		&bqv2.BatchDeleteRowAccessPoliciesRequest{PolicyIds: []string{"a", "b"}}).Context(ctx).Do()
	require.ErrorAs(t, err, &gerr)

	require.NoError(t, svc.RowAccessPolicies.BatchDelete(sdkProject, "rls", "sales",
		&bqv2.BatchDeleteRowAccessPoliciesRequest{PolicyIds: []string{"a", "b"}, Force: true}).Context(ctx).Do())

	list, err = svc.RowAccessPolicies.List(sdkProject, "rls", "sales").Context(ctx).Do()
	require.NoError(t, err)
	assert.Empty(t, list.RowAccessPolicies)
}

func TestRowAccessPolicyGuards(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)
	env.exec(t, "CREATE ROW ACCESS POLICY p ON rls.sales GRANT TO ('allUsers') FILTER USING (amount > 1)")

	var gerr *googleapi.Error

	t.Run("tabledata", func(t *testing.T) {
		t.Parallel()

		it := env.client.Dataset("rls").Table("sales").Read(t.Context())

		var row []bigquery.Value

		require.ErrorAs(t, it.Next(&row), &gerr)
		assert.Equal(t, http.StatusForbidden, gerr.Code)
	})

	t.Run("dropcolumn", func(t *testing.T) {
		t.Parallel()

		_, err := env.client.Query("ALTER TABLE rls.sales DROP COLUMN amount").Read(t.Context())
		require.ErrorContains(t, err, "row access policy p references it")
	})

	t.Run("view", func(t *testing.T) {
		t.Parallel()

		env.exec(t, "CREATE VIEW rls.v AS SELECT 1 AS x")

		_, err := env.client.Query("CREATE ROW ACCESS POLICY q ON rls.v GRANT TO ('allUsers') FILTER USING (TRUE)").
			Read(t.Context())
		require.ErrorContains(t, err, "not on views")
	})
}

func TestRowAccessPolicyPersistence(t *testing.T) {
	t.Parallel()

	env := newSDK(t)
	seedSales(t, env)
	env.exec(t, "CREATE ROW ACCESS POLICY p ON rls.sales GRANT TO ('user:alice@example.com') "+
		"FILTER USING (region = 'EU')")

	other := newSDK(t)
	require.NoError(t, other.backend.Restore(t.Context(), env.backend.Snapshot(t.Context())))

	assert.Equal(t, [][]bigquery.Value{{"EU"}},
		other.asUser(t, "alice@example.com").rows(t, "SELECT region FROM rls.sales"))
	assert.Empty(t, other.asUser(t, "bob@example.org").rows(t, "SELECT region FROM rls.sales"))
}
