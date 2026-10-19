package dashboard_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpfirestore"
)

func TestGCPFirestoreProxy(t *testing.T) {
	t.Parallel()

	const (
		root    = "/dashboard/api/v1/gcp/firestore"
		docs    = root + "/v1/projects/p/databases/(default)/documents"
		db      = "projects/p/databases/(default)/documents"
		users   = docs + "/users"
		alice   = users + "/alice"
		fieldsA = `{"fields":{"name":{"stringValue":"Alice"},"age":{"integerValue":"30"}}}`
		fieldsB = `{"fields":{"name":{"stringValue":"Bob"},"age":{"integerValue":"40"}}}`
		query   = `{"structuredQuery":{"from":[{"collectionId":"users"}],` +
			`"where":{"fieldFilter":{"field":{"fieldPath":"age"},"op":"GREATER_THAN","value":{"integerValue":"35"}}}}}`
		agg = `{"structuredAggregationQuery":{"structuredQuery":{"from":[{"collectionId":"users"}]},` +
			`"aggregations":[{"count":{},"alias":"n"}]}}`
		commit = `{"writes":[{"update":{"name":"` + db + `/users/carol","fields":{"name":{"stringValue":"Carol"}}}}]}`
		batch  = `{"documents":["` + db + `/users/alice","` + db + `/users/zed"]}`
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"document lifecycle", []gcpStep{
			{http.MethodPost, users + "?documentId=alice", fieldsA, `"stringValue":"Alice"`, 200},
			{
				http.MethodPost,
				users + "?documentId=alice",
				fieldsA,
				"ALREADY_EXISTS",
				http.StatusConflict,
			},
			{http.MethodPost, users + "?documentId=bob", fieldsB, db + "/users/bob", 200},
			{http.MethodGet, users, "", `"documents"`, 200},
			{http.MethodGet, alice, "", `"integerValue":"30"`, 200},
			{
				http.MethodPatch,
				alice + "?updateMask.fieldPaths=age",
				`{"fields":{"age":{"integerValue":"31"}}}`,
				`"31"`,
				200,
			},
			{http.MethodGet, alice, "", `"Alice"`, 200},
			{http.MethodPatch, alice, `{"fields":{"only":{"booleanValue":true}}}`, `"only"`, 200},
			{http.MethodGet, alice, "", `"only"`, 200},
			{
				http.MethodPatch,
				users + "/ghost?currentDocument.exists=true",
				fieldsA,
				"NOT_FOUND",
				http.StatusNotFound,
			},
			{http.MethodDelete, alice, "", "{}", 200},
			{http.MethodGet, alice, "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"queries and collections", []gcpStep{
			{http.MethodPost, users + "?documentId=alice", fieldsA, "alice", 200},
			{http.MethodPost, users + "?documentId=bob", fieldsB, "bob", 200},
			{http.MethodPost, alice + "/notes?documentId=n1", `{"fields":{}}`, "notes/n1", 200},
			{http.MethodPost, docs + ":runQuery", query, `"stringValue":"Bob"`, 200},
			{http.MethodPost, docs + ":runAggregationQuery", agg, `"n"`, 200},
			{http.MethodPost, docs + ":listCollectionIds", `{}`, `"users"`, 200},
			{http.MethodPost, alice + ":listCollectionIds", `{}`, `"notes"`, 200},
			{http.MethodPost, docs + ":batchGet", batch, `"missing"`, 200},
			{http.MethodPost, docs + ":commit", commit, "writeResults", 200},
			{http.MethodGet, users + "/carol", "", `"Carol"`, 200},
		}},
		{"rejects", []gcpStep{
			{
				http.MethodGet,
				root + "/v1/projects/p/databases/(default)",
				"",
				"unsupported",
				http.StatusNotFound,
			},
			{http.MethodGet, docs, "", "unsupported", http.StatusNotFound},
			{http.MethodPut, alice, "", "unsupported", http.StatusNotFound},
			{http.MethodGet, docs + ":runQuery", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, users + ":commit", "{}", "unsupported", http.StatusNotFound},
			{
				http.MethodPost,
				docs + ":runQuery",
				`{bad`,
				"invalid request body",
				http.StatusBadRequest,
			},
			{http.MethodPost, docs + ":runQuery", `{}`, "structured_query", http.StatusBadRequest},
			{
				http.MethodPatch,
				alice + "?currentDocument.updateTime=nope",
				fieldsA,
				"updateTime",
				http.StatusBadRequest,
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPFirestoreOps: gcpfirestore.NewHandler(gcpfirestore.NewInMemoryBackend()),
			})
			runGCPSteps(t, h, tt.steps)
		})
	}

	t.Run("not served", func(t *testing.T) {
		t.Parallel()

		runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
			{http.MethodGet, users, "", "", http.StatusServiceUnavailable},
		})
	})
}
