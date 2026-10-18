package iot_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`

func policyDoc(statement string) string {
	return `{"Version":"2012-10-17","Statement":` + statement + `}`
}

func TestPolicyDocumentStructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		doc  string
		bad  bool
	}{
		{name: "valid_array", doc: policyDoc(`[{"Effect":"Allow","Action":["iot:Connect"],"Resource":"*"}]`)},
		{name: "valid_single_statement", doc: policyDoc(`{"Effect":"Deny","Action":"iot:*","Resource":"*"}`)},
		{name: "empty_object", doc: `{}`, bad: true},
		{name: "no_version", doc: `{"Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`, bad: true},
		{name: "no_statement", doc: `{"Version":"2012-10-17"}`, bad: true},
		{name: "statement_not_object", doc: policyDoc(`["x"]`), bad: true},
		{name: "bad_effect", doc: policyDoc(`[{"Effect":"Maybe","Action":"iot:*","Resource":"*"}]`), bad: true},
		{name: "no_effect", doc: policyDoc(`[{"Action":"iot:*","Resource":"*"}]`), bad: true},
		{name: "no_action", doc: policyDoc(`[{"Effect":"Allow","Resource":"*"}]`), bad: true},
		{name: "no_resource", doc: policyDoc(`[{"Effect":"Allow","Action":"iot:*"}]`), bad: true},
		{name: "action_wrong_type", doc: policyDoc(`[{"Effect":"Allow","Action":5,"Resource":"*"}]`), bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newIoTHandlerBatch1(t)
			create := iotRequest(
				t,
				h,
				http.MethodPost,
				"/policies/p-"+tt.name,
				map[string]any{"policyDocument": tt.doc},
			)

			okDoc := validPolicyDoc
			seed := iotRequest(t, h, http.MethodPost, "/policies/base", map[string]any{"policyDocument": okDoc})
			require.Equal(t, http.StatusOK, seed.Code)

			version := iotRequest(
				t,
				h,
				http.MethodPost,
				"/policies/base/version",
				map[string]any{"policyDocument": tt.doc},
			)

			if !tt.bad {
				assert.Equal(t, http.StatusOK, create.Code, create.Body.String())
				assert.Equal(t, http.StatusOK, version.Code, version.Body.String())

				return
			}

			for _, rec := range []struct {
				body []byte
				code int
			}{{create.Body.Bytes(), create.Code}, {version.Body.Bytes(), version.Code}} {
				assert.Equal(t, http.StatusBadRequest, rec.code)
				code, _ := iotErr(t, rec.body)
				assert.Equal(t, "MalformedPolicyException", code)
			}
		})
	}
}
