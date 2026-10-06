package athena_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ClientRequestTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		idKey      string
		first      string
		second     string
		wantStatus int
		wantSame   bool
	}{
		{
			name: "start_query_replays", action: "StartQueryExecution", idKey: "QueryExecutionId",
			first:    `{"QueryString":"SELECT 1","WorkGroup":"primary","ClientRequestToken":"t1"}`,
			second:   `{"QueryString":"SELECT 1","WorkGroup":"primary","ClientRequestToken":"t1"}`,
			wantSame: true, wantStatus: http.StatusOK,
		},
		{
			name: "start_query_changed_param", action: "StartQueryExecution", idKey: "QueryExecutionId",
			first:      `{"QueryString":"SELECT 1","WorkGroup":"primary","ClientRequestToken":"t1"}`,
			second:     `{"QueryString":"SELECT 2","WorkGroup":"primary","ClientRequestToken":"t1"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "start_query_new_token", action: "StartQueryExecution", idKey: "QueryExecutionId",
			first:      `{"QueryString":"SELECT 1","WorkGroup":"primary","ClientRequestToken":"t1"}`,
			second:     `{"QueryString":"SELECT 1","WorkGroup":"primary","ClientRequestToken":"t2"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "named_query_replays", action: "CreateNamedQuery", idKey: "NamedQueryId",
			first:    `{"Name":"q","Database":"d","QueryString":"SELECT 1","ClientRequestToken":"t1"}`,
			second:   `{"Name":"q","Database":"d","QueryString":"SELECT 1","ClientRequestToken":"t1"}`,
			wantSame: true, wantStatus: http.StatusOK,
		},
		{
			name: "notebook_replays", action: "CreateNotebook", idKey: "NotebookId",
			first:    `{"WorkGroup":"primary","Name":"nb","ClientRequestToken":"t1"}`,
			second:   `{"WorkGroup":"primary","Name":"nb","ClientRequestToken":"t1"}`,
			wantSame: true, wantStatus: http.StatusOK,
		},
		{
			name: "import_notebook_replays", action: "ImportNotebook", idKey: "NotebookId",
			first:    `{"WorkGroup":"primary","Name":"nb","Type":"IPYNB","Payload":"{}","ClientRequestToken":"t1"}`,
			second:   `{"WorkGroup":"primary","Name":"nb","Type":"IPYNB","Payload":"{}","ClientRequestToken":"t1"}`,
			wantSame: true, wantStatus: http.StatusOK,
		},
		{
			name: "no_token_conflicts", action: "CreateNotebook", idKey: "NotebookId",
			first:      `{"WorkGroup":"primary","Name":"nb"}`,
			second:     `{"WorkGroup":"primary","Name":"nb"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "session_replays", action: "StartSession", idKey: "SessionId",
			first:    `{"WorkGroup":"primary","ClientRequestToken":"t1"}`,
			second:   `{"WorkGroup":"primary","ClientRequestToken":"t1"}`,
			wantSame: true, wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			first := doRequest(t, h, tt.action, tt.first)
			require.Equal(t, http.StatusOK, first.Code, first.Body.String())

			second := doRequest(t, h, tt.action, tt.second)
			require.Equal(t, tt.wantStatus, second.Code, second.Body.String())

			if tt.wantStatus != http.StatusOK {
				return
			}

			same := jsonField(t, first.Body.Bytes(), tt.idKey) == jsonField(t, second.Body.Bytes(), tt.idKey)
			assert.Equal(t, tt.wantSame, same)
		})
	}
}

func TestHandler_StartCalculationExecutionReplay(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sessionID := startSession(t, h)
	body := `{"SessionId":"` + sessionID + `","CodeBlock":"print(1)","ClientRequestToken":"t1"}`

	first := doRequest(t, h, "StartCalculationExecution", body)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())

	replay := doRequest(t, h, "StartCalculationExecution", body)
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t,
		jsonField(t, first.Body.Bytes(), "CalculationExecutionId"),
		jsonField(t, replay.Body.Bytes(), "CalculationExecutionId"))

	changed := doRequest(t, h, "StartCalculationExecution",
		`{"SessionId":"`+sessionID+`","CodeBlock":"print(2)","ClientRequestToken":"t1"}`)
	assert.Equal(t, http.StatusBadRequest, changed.Code)
}

type tagList struct {
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func TestHandler_StartSessionTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string]string
		name string
		body string
	}{
		{
			name: "explicit_tags",
			body: `{"WorkGroup":"primary","Tags":[{"Key":"env","Value":"dev"}]}`,
			want: map[string]string{"env": "dev"},
		},
		{
			name: "no_tags",
			body: `{"WorkGroup":"primary"}`,
			want: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "StartSession", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			arn := "arn:aws:athena:us-east-1:000000000000:session/" + jsonField(t, rec.Body.Bytes(), "SessionId")
			tagged := doRequest(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
			require.Equal(t, http.StatusOK, tagged.Code, tagged.Body.String())

			var out tagList
			require.NoError(t, json.Unmarshal(tagged.Body.Bytes(), &out))

			got := map[string]string{}
			for _, tg := range out.Tags {
				got[tg.Key] = tg.Value
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_StartSessionCopyWorkGroupTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	created := doRequest(t, h, "CreateWorkGroup",
		`{"Name":"wg","Tags":[{"Key":"team","Value":"a"},{"Key":"env","Value":"wg"}]}`)
	require.Equal(t, http.StatusOK, created.Code, created.Body.String())

	rec := doRequest(t, h, "StartSession",
		`{"WorkGroup":"wg","CopyWorkGroupTags":true,"Tags":[{"Key":"env","Value":"session"}]}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	arn := "arn:aws:athena:us-east-1:000000000000:session/" + jsonField(t, rec.Body.Bytes(), "SessionId")
	tagged := doRequest(t, h, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)
	require.Equal(t, http.StatusOK, tagged.Code, tagged.Body.String())

	var out tagList
	require.NoError(t, json.Unmarshal(tagged.Body.Bytes(), &out))

	got := map[string]string{}
	for _, tg := range out.Tags {
		got[tg.Key] = tg.Value
	}

	assert.Equal(t, map[string]string{"team": "a", "env": "session"}, got)
}
