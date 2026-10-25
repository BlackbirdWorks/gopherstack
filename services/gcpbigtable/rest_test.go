package gcpbigtable_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest/resttest"
)

func TestRESTFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	const inst = "/v2/projects/p/instances/rest-inst"

	resttest.Run(t, e.handler.Addr(), []resttest.Step{
		{
			Name: "create instance", Method: http.MethodPost, Path: "/v2/projects/p/instances",
			Body: `{"instanceId":"rest-inst","instance":{"displayName":"REST","type":"PRODUCTION"},` +
				`"clusters":{"rc":{"location":"projects/p/locations/us-central1-b","serveNodes":1}}}`,
			Status: 200, Contains: []string{`"done":true`, `rest-inst`},
		},
		{
			Name: "get instance", Method: http.MethodGet, Path: inst,
			Status: 200, Contains: []string{`"displayName":"REST"`, `"state":"READY"`},
		},
		{
			Name: "create table", Method: http.MethodPost, Path: inst + "/tables",
			Body:   `{"tableId":"t","table":{"columnFamilies":{"cf":{"gcRule":{"maxNumVersions":1}}}}}`,
			Status: 200, Contains: []string{`"cf"`, `"maxNumVersions":1`},
		},
		{
			Name: "mutate row", Method: http.MethodPost, Path: inst + "/tables/t:mutateRow",
			Body: `{"rowKey":"cm93MQ==","mutations":[{"setCell":{"familyName":"cf","columnQualifier":"cQ==",` +
				`"timestampMicros":"1000","value":"dmFs"}}]}`,
			Status: 200,
		},
		{
			Name: "read rows streams an array", Method: http.MethodPost, Path: inst + "/tables/t:readRows",
			Body: `{}`, Status: 200, Contains: []string{`"rowKey":"cm93MQ=="`, `"value":"dmFs"`, `"commitRow":true`, `[`},
		},
		{
			Name: "check and mutate", Method: http.MethodPost, Path: inst + "/tables/t:checkAndMutateRow",
			Body: `{"rowKey":"cm93MQ==","trueMutations":[{"deleteFromRow":{}}]}`, Status: 200,
			Contains: []string{`"predicateMatched":true`},
		},
		{
			Name: "read missing table", Method: http.MethodPost, Path: inst + "/tables/ghost:readRows",
			Body: `{}`, Status: 404, Contains: []string{`"status":"NOT_FOUND"`},
		},
		{
			Name: "list tables", Method: http.MethodGet, Path: inst + "/tables",
			Status: 200, Contains: []string{`inst/tables/t`},
		},
	})
}
