package gcpsql_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestFlagsAndTiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		has     string
		hasNot  string
	}{
		{"postgres", "POSTGRES_16", "work_mem", "innodb_buffer_pool_size"},
		{"mysql", "MYSQL_8_0", "innodb_buffer_pool_size", "work_mem"},
		{"all", "", "work_mem", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			call := e.svc.Flags.List()
			if tt.version != "" {
				call = call.DatabaseVersion(tt.version)
			}

			res, err := call.Context(t.Context()).Do()
			require.NoError(t, err)

			names := map[string]bool{}
			for _, f := range res.Items {
				names[f.Name] = true
			}

			assert.True(t, names[tt.has])

			if tt.hasNot != "" {
				assert.False(t, names[tt.hasNot])
			}
		})
	}

	e := newEnv(t)
	res, err := e.svc.Tiers.List(project).Context(t.Context()).Do()
	require.NoError(t, err)

	var micro *sqladmin.Tier

	for _, tr := range res.Items {
		if tr.Tier == "db-f1-micro" {
			micro = tr
		}
	}

	require.NotNil(t, micro)
	assert.Positive(t, micro.RAM)
}

func TestOperationsListGetCancel(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "o1", "POSTGRES_16", nil)

	op, err := e.svc.Databases.Insert(project, "o1", &sqladmin.Database{Name: "a"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	list, err := e.svc.Operations.List(project).Instance("o1").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, "CREATE_DATABASE", list.Items[0].OperationType)
	assert.Equal(t, "CREATE", list.Items[1].OperationType)

	paged, err := e.svc.Operations.List(project).Instance("o1").MaxResults(1).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, paged.Items, 1)
	assert.NotEmpty(t, paged.NextPageToken)

	got, err := e.svc.Operations.Get(project, op.Name).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "DONE", got.Status)
	assert.Equal(t, "o1", got.TargetId)
	assert.NotEmpty(t, got.EndTime)

	_, err = e.svc.Operations.Cancel(project, op.Name).Context(ctx).Do()
	require.NoError(t, err)

	_, err = e.svc.Operations.Get(project, "nope").Context(ctx).Do()
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestUnknownRouteIs404(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, e.url+"/v1/projects/p/nonsense", nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
