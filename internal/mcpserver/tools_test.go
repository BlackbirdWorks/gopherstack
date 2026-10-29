package mcpserver_test

import (
	"net/http"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/mcpserver"
)

func TestQueryTracesRedactsAndFilters(t *testing.T) {
	t.Parallel()

	admin := http.NewServeMux()
	admin.HandleFunc("/_gopherstack/requests", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id":"1","method":"POST","path":"/","service":"dynamodb","status":200,"duration_ms":2000000,
			 "headers":{"X-Amz-Target":"DynamoDB_20120810.PutItem","Authorization":"AWS4 Signature=hdrsecret"}},
			{"id":"2","method":"GET","path":"/b/k","query":"X-Amz-Signature=qsecret&versionId=3","service":"s3","status":403,
			 "headers":{}},
			{"id":"3","method":"POST","path":"/","query":"Action=ListQueues","service":"sqs","status":200,"headers":{}}
		]`))
	})

	srv := mcpserver.New(mcpserver.InProcessOptions(admin, "test"))
	ct, st := mcp.NewInMemoryTransports()

	ss, err := srv.Connect(t.Context(), st, nil)
	require.NoError(t, err)

	t.Cleanup(func() { _ = ss.Close() })

	cs, err := mcp.NewClient(&mcp.Implementation{Name: "t", Version: "0"}, nil).Connect(t.Context(), ct, nil)
	require.NoError(t, err)

	t.Cleanup(func() { _ = cs.Close() })

	tests := []struct {
		args    map[string]any
		name    string
		wantIDs []string
	}{
		{name: "all", args: map[string]any{}, wantIDs: []string{"1", "2", "3"}},
		{name: "op_header", args: map[string]any{"operation": "putitem"}, wantIDs: []string{"1"}},
		{name: "op_query_action", args: map[string]any{"operation": "ListQueues"}, wantIDs: []string{"3"}},
		{name: "limit", args: map[string]any{"limit": 2}, wantIDs: []string{"1", "2"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, callErr := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "query_traces", Arguments: tt.args})
			require.NoError(t, callErr)
			require.False(t, res.IsError)

			out, _ := res.StructuredContent.(map[string]any)
			traces, _ := out["traces"].([]any)

			var ids []string
			for _, tr := range traces {
				ids = append(ids, tr.(map[string]any)["id"].(string))
			}

			assert.Equal(t, tt.wantIDs, ids)

			assert.NotContains(t, out, "headers")

			for _, tr := range traces {
				assert.NotContains(t, tr, "headers")
				q, _ := tr.(map[string]any)["query"].(string)
				assert.NotContains(t, q, "qsecret")
			}
		})
	}
}
