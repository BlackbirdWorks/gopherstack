package stepfunctions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOverWideListResponses asserts on the raw response body that every
// stepfunctions List op emits only the fields the real AWS *ListItem type
// declares (types.go, sfn@v1.45.4) -- not the fields its sibling Get/Describe
// op returns. A typed aws-sdk-go-v2 client cannot see this class of bug: it
// silently discards any key it does not model, so a typed-client assertion
// would pass whether or not the fix is applied. Only inspecting the raw
// serialized JSON proves the leaked fields are actually absent from the
// wire, which is why every subtest here decodes into map[string]json.RawMessage
// rather than a struct.
func TestOverWideListResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw       func(t *testing.T) []byte
		name      string
		itemsKey  string
		required  []string
		forbidden []string
	}{
		{
			name:     "list state machines omits definition roleArn status config",
			itemsKey: "stateMachines",
			required: []string{"creationDate", "name", "stateMachineArn", "type"},
			// StateMachineListItem (types.go) declares only the four
			// required fields above; DescribeStateMachineOutput/StateMachine
			// additionally carries all of these.
			forbidden: []string{
				"definition", "roleArn", "status", "revisionId", "updatedDate",
				"encryptionConfiguration", "tracingConfiguration", "loggingConfiguration",
			},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := t.Context()
				h, e := newSFNHandler(t)

				createBody, err := json.Marshal(map[string]any{
					"name":                    "wide-sm",
					"definition":              validPassDef,
					"roleArn":                 "arn:aws:iam::123456789012:role/test",
					"type":                    "STANDARD",
					"tracingConfiguration":    map[string]any{"enabled": true},
					"loggingConfiguration":    map[string]any{"level": "ALL"},
					"encryptionConfiguration": map[string]any{"type": "AWS_OWNED_KEY"},
				})
				require.NoError(t, err)

				rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				smARN, _ := created["stateMachineArn"].(string)
				require.NotEmpty(t, smARN)

				// Populate revisionId/updatedDate too (only set after an Update).
				updateBody, err := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"definition":      validPassDef,
					"roleArn":         "arn:aws:iam::123456789012:role/test",
				})
				require.NoError(t, err)

				updRec := sfnPost(ctx, t, h, e, "UpdateStateMachine", string(updateBody))
				require.Equal(t, http.StatusOK, updRec.Code)

				listRec := sfnPost(ctx, t, h, e, "ListStateMachines", `{}`)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:      "list activities omits encryptionConfiguration",
			itemsKey:  "activities",
			required:  []string{"activityArn", "creationDate", "name"},
			forbidden: []string{"encryptionConfiguration"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := t.Context()
				h, e := newSFNHandler(t)

				createBody, err := json.Marshal(map[string]any{
					"name": "wide-activity",
					"encryptionConfiguration": map[string]any{
						"type":     "CUSTOMER_MANAGED_KMS_KEY",
						"kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
					},
				})
				require.NoError(t, err)

				rec := sfnPost(ctx, t, h, e, "CreateActivity", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				listRec := sfnPost(ctx, t, h, e, "ListActivities", `{}`)
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:      "list executions omits input output error cause",
			itemsKey:  "executions",
			required:  []string{"executionArn", "name", "startDate", "stateMachineArn", "status"},
			forbidden: []string{"input", "output", "error", "cause"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := context.Background()
				h, e := newSFNHandler(t)
				smARN := createSM(ctx, t, h, e, "wide-exec-sm")

				execBody, err := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"name":            "wide-exec",
					"input":           `{"secret":"do-not-leak"}`,
				})
				require.NoError(t, err)

				rec := sfnPost(ctx, t, h, e, "StartExecution", string(execBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var started map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))
				execARN, _ := started["executionArn"].(string)
				require.NotEmpty(t, execARN)

				descBody, err := json.Marshal(map[string]string{"executionArn": execARN})
				require.NoError(t, err)

				require.Eventuallyf(t, func() bool {
					descRec := sfnPost(ctx, t, h, e, "DescribeExecution", string(descBody))
					if descRec.Code != http.StatusOK {
						return false
					}

					var desc map[string]any
					if unmarshalErr := json.Unmarshal(descRec.Body.Bytes(), &desc); unmarshalErr != nil {
						return false
					}

					return desc["status"] != "RUNNING"
				}, 5*time.Second, 20*time.Millisecond, "execution should finish")

				listBody, err := json.Marshal(map[string]string{"stateMachineArn": smARN})
				require.NoError(t, err)

				listRec := sfnPost(ctx, t, h, e, "ListExecutions", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list map runs omits status itemCounts and update-only fields",
			itemsKey: "mapRuns",
			required: []string{"executionArn", "mapRunArn", "startDate", "stateMachineArn"},
			// MapRunListItem (types.go) declares only the four/five fields
			// above (StopDate is optional too); DescribeMapRunOutput/MapRun
			// additionally carries all of these.
			forbidden: []string{
				"status", "itemCounts", "toleratedFailurePercentage",
				"maxConcurrency", "toleratedFailureCount",
			},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := context.Background()
				h, e := newSFNHandler(t)

				createBody, err := json.Marshal(map[string]any{
					"name":       "wide-maprun-sm",
					"definition": mapIterStateDef,
					"roleArn":    "arn:aws:iam::123456789012:role/test",
					"type":       "EXPRESS",
				})
				require.NoError(t, err)

				rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(createBody))
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				smARN, _ := created["stateMachineArn"].(string)
				require.NotEmpty(t, smARN)

				syncBody, err := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"name":            "wide-maprun-exec",
					"input":           `[1,2,3]`,
				})
				require.NoError(t, err)

				syncRec := sfnPost(ctx, t, h, e, "StartSyncExecution", string(syncBody))
				require.Equal(t, http.StatusOK, syncRec.Code)

				var synced map[string]any
				require.NoError(t, json.Unmarshal(syncRec.Body.Bytes(), &synced))
				execARN, _ := synced["executionArn"].(string)
				require.NotEmpty(t, execARN)

				listBody, err := json.Marshal(map[string]any{"executionArn": execARN})
				require.NoError(t, err)

				listRec := sfnPost(ctx, t, h, e, "ListMapRuns", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				var listed map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listed))
				runs, _ := listed["mapRuns"].([]any)
				require.NotEmpty(t, runs, "expected at least one MapRun")
				first, _ := runs[0].(map[string]any)
				mapRunARN, _ := first["mapRunArn"].(string)
				require.NotEmpty(t, mapRunARN)

				// Populate maxConcurrency/toleratedFailureCount, which are
				// otherwise zero-valued and omitempty on the domain struct.
				updateBody, err := json.Marshal(map[string]any{
					"mapRunArn":                  mapRunARN,
					"maxConcurrency":             5,
					"toleratedFailureCount":      1,
					"toleratedFailurePercentage": 10.0,
				})
				require.NoError(t, err)

				updRec := sfnPost(ctx, t, h, e, "UpdateMapRun", string(updateBody))
				require.Equal(t, http.StatusOK, updRec.Code)

				listRec = sfnPost(ctx, t, h, e, "ListMapRuns", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list state machine aliases omits name description routing updateDate",
			itemsKey: "stateMachineAliases",
			required: []string{"creationDate", "stateMachineAliasArn"},
			// StateMachineAliasListItem (types.go) declares only the two
			// fields above; DescribeStateMachineAliasOutput additionally
			// carries all of these.
			forbidden: []string{"name", "description", "routingConfiguration", "updateDate"},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := context.Background()
				h, e := newSFNHandler(t)
				smARN := createSM(ctx, t, h, e, "wide-alias-sm")

				pubBody, err := json.Marshal(map[string]any{"stateMachineArn": smARN})
				require.NoError(t, err)

				pubRec := sfnPost(ctx, t, h, e, "PublishStateMachineVersion", string(pubBody))
				require.Equal(t, http.StatusOK, pubRec.Code)

				var published map[string]any
				require.NoError(t, json.Unmarshal(pubRec.Body.Bytes(), &published))
				versionARN, _ := published["stateMachineVersionArn"].(string)
				require.NotEmpty(t, versionARN)

				createBody, err := json.Marshal(map[string]any{
					"name":            "wide-alias",
					"stateMachineArn": smARN,
					"description":     "leaks if present",
					"routingConfiguration": []map[string]any{
						{"stateMachineVersionArn": versionARN, "weight": 100},
					},
				})
				require.NoError(t, err)

				aliasRec := sfnPost(ctx, t, h, e, "CreateStateMachineAlias", string(createBody))
				require.Equal(t, http.StatusOK, aliasRec.Code)

				var alias map[string]any
				require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &alias))
				aliasARN, _ := alias["stateMachineAliasArn"].(string)
				require.NotEmpty(t, aliasARN)

				// Update so updateDate is populated too.
				updateBody, err := json.Marshal(map[string]any{
					"stateMachineAliasArn": aliasARN,
					"description":          "updated",
				})
				require.NoError(t, err)

				updRec := sfnPost(ctx, t, h, e, "UpdateStateMachineAlias", string(updateBody))
				require.Equal(t, http.StatusOK, updRec.Code)

				listBody, err := json.Marshal(map[string]string{"stateMachineArn": smARN})
				require.NoError(t, err)

				listRec := sfnPost(ctx, t, h, e, "ListStateMachineAliases", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
		{
			name:     "list state machine versions omits definition roleArn status",
			itemsKey: "stateMachineVersions",
			required: []string{"creationDate", "stateMachineVersionArn"},
			// StateMachineVersionListItem (types.go) declares only the two
			// fields above; PublishStateMachineVersion's own domain struct
			// additionally carries all of these.
			forbidden: []string{
				"stateMachineArn", "name", "definition", "roleArn",
				"type", "status", "description", "revisionId",
			},
			raw: func(t *testing.T) []byte {
				t.Helper()

				ctx := context.Background()
				h, e := newSFNHandler(t)
				smARN := createSM(ctx, t, h, e, "wide-version-sm")

				pubBody, err := json.Marshal(map[string]any{
					"stateMachineArn": smARN,
					"description":     "leaks if present",
					"revisionId":      "leaks-if-present",
				})
				require.NoError(t, err)

				pubRec := sfnPost(ctx, t, h, e, "PublishStateMachineVersion", string(pubBody))
				require.Equal(t, http.StatusOK, pubRec.Code)

				listBody, err := json.Marshal(map[string]string{"stateMachineArn": smARN})
				require.NoError(t, err)

				listRec := sfnPost(ctx, t, h, e, "ListStateMachineVersions", string(listBody))
				require.Equal(t, http.StatusOK, listRec.Code)

				return listRec.Body.Bytes()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := tt.raw(t)

			var resp map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &resp))

			itemsRaw, ok := resp[tt.itemsKey]
			require.True(t, ok, "response missing %q key", tt.itemsKey)

			var items []map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(itemsRaw, &items))
			require.NotEmpty(t, items, "expected at least one item in %q", tt.itemsKey)

			for _, item := range items {
				for _, key := range tt.required {
					_, present := item[key]
					assert.True(t, present, "expected required key %q in %s item, got %s", key, tt.itemsKey, item)
				}

				for _, key := range tt.forbidden {
					_, present := item[key]
					assert.False(
						t,
						present,
						"over-wide: key %q must not appear in %s item (real AWS Summary/ListItem type does not declare it), got %s",
						key,
						tt.itemsKey,
						item,
					)
				}
			}
		})
	}
}
