package dashboard_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/datastore/admin/apiv1/adminpb"
	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpdatastore"
)

func TestGCPDatastoreProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/gcp/datastore/v1/projects/p"

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		d := gcpdatastore.NewHandler(gcpdatastore.NewInMemoryBackend())
		key := &datastorepb.Key{
			PartitionId: &datastorepb.PartitionId{ProjectId: "p"},
			Path: []*datastorepb.Key_PathElement{
				{Kind: "Task", IdType: &datastorepb.Key_PathElement_Name{Name: "t1"}},
			},
		}
		_, err := d.Backend.Commit(&datastorepb.CommitRequest{
			ProjectId: "p", Mode: datastorepb.CommitRequest_NON_TRANSACTIONAL,
			Mutations: []*datastorepb.Mutation{{Operation: &datastorepb.Mutation_Insert{Insert: &datastorepb.Entity{
				Key: key,
				Properties: map[string]*datastorepb.Value{
					"title": {ValueType: &datastorepb.Value_StringValue{StringValue: "write tests"}},
					"note": {
						ValueType:          &datastorepb.Value_StringValue{StringValue: "n"},
						ExcludeFromIndexes: true,
					},
				},
			}}}},
		})
		require.NoError(t, err)

		_, err = d.Backend.CreateIndex(&adminpb.CreateIndexRequest{
			ProjectId: "p",
			Index: &adminpb.Index{
				Kind: "Task", Ancestor: adminpb.Index_NONE,
				Properties: []*adminpb.Index_IndexedProperty{{Name: "title", Direction: adminpb.Index_ASCENDING}},
			},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPDatastoreOps: d})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"kinds", []gcpStep{
			{
				http.MethodPost,
				base + ":runQuery",
				`{"query":{"kind":[{"name":"__kind__"}],"projection":[]}}`,
				`"Task"`,
				200,
			},
		}},
		{"namespaces", []gcpStep{
			{http.MethodPost, base + ":runQuery", `{"query":{"kind":[{"name":"__namespace__"}]}}`, `"batch"`, 200},
		}},
		{"entities", []gcpStep{
			{http.MethodPost, base + ":runQuery", `{"query":{"kind":[{"name":"Task"}]}}`, `"write tests"`, 200},
			{
				http.MethodPost,
				base + ":runQuery",
				`{"query":{"kind":[{"name":"Task"}]}}`,
				`"excludeFromIndexes":true`,
				200,
			},
		}},
		{"gql", []gcpStep{
			{http.MethodPost, base + ":runQuery",
				`{"gqlQuery":{"allowLiterals":true,"queryString":"SELECT * FROM Task WHERE title = 'write tests'"}}`,
				`"write tests"`, 200},
			{http.MethodPost, base + ":runQuery",
				`{"gqlQuery":{"allowLiterals":true,"queryString":"SELEKT"}}`, "INVALID_ARGUMENT", 400},
		}},
		{"lookup", []gcpStep{
			{http.MethodPost, base + ":lookup",
				`{"keys":[{"path":[{"kind":"Task","name":"t1"}]}]}`, `"found"`, 200},
		}},
		{"indexes", []gcpStep{
			{http.MethodGet, base + "/indexes", "", `"kind":"Task"`, 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodPost, base + ":commit", `{}`, "unsupported", 404},
			{http.MethodPost, base + ":beginTransaction", `{}`, "unsupported", 404},
			{http.MethodDelete, base + "/indexes/1", "", "unsupported", 404},
			{http.MethodGet, base + "/other", "", "unsupported", 404},
			{http.MethodPost, base + ":runQuery", `{bad`, "invalid request body", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, seed(t), tt.steps)
		})
	}

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, base + "/indexes", "", "ServiceUnavailable", http.StatusServiceUnavailable},
		})
	})
}
