package ssoadmin_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplicationLists_NextTokenCursor resumes ListApplicationGrants and
// ListTagsForResource from a NextToken cursor (NextToken-only inputs, api_op_ListApplicationGrants.go).
func TestApplicationLists_NextTokenCursor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		op    string
		field string
		token string
		want  int
	}{
		{name: "grants_after_first", op: "ListApplicationGrants", field: "Grants", token: "refresh_token", want: 1},
		{name: "grants_past_end", op: "ListApplicationGrants", field: "Grants", token: "zzz", want: 0},
		{name: "tags_after_first", op: "ListTagsForResource", field: "Tags", token: "b", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "cursor-instance")
			rec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "Cursor",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			appArn, _ := parseResponse(t, rec)["ApplicationArn"].(string)

			for _, g := range []string{"authorization_code", "refresh_token"} {
				require.Equal(t, http.StatusOK, doRequest(t, h, "PutApplicationGrant", map[string]any{
					"ApplicationArn": appArn, "GrantType": g,
				}).Code)
			}

			require.Equal(t, http.StatusOK, doRequest(t, h, "TagResource", map[string]any{
				"InstanceArn": instanceArn, "ResourceArn": appArn,
				"Tags": []map[string]string{{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}},
			}).Code)

			body := map[string]any{
				"ApplicationArn": appArn, "InstanceArn": instanceArn, "ResourceArn": appArn,
				"NextToken": base64.StdEncoding.EncodeToString([]byte(tt.token)),
			}
			out := parseResponse(t, doRequest(t, h, tt.op, body))

			items, _ := out[tt.field].([]any)
			require.Len(t, items, tt.want)
		})
	}
}
