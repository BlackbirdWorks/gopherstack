package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func TestAppStream_Stacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateStack returns stack with ARN",
			action:   "CreateStack",
			body:     map[string]any{"Name": "my-stack", "DisplayName": "My Stack"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				stack, ok := resp["Stack"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-stack", stack["Name"])
				assert.Contains(t, stack["Arn"], "arn:aws:appstream:")
				assert.Contains(t, stack["Arn"], ":stack/my-stack")
			},
		},
		{
			name:   "CreateStack duplicate returns error",
			action: "CreateStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "dup-stack")
			},
			body:     map[string]any{"Name": "dup-stack"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeStacks returns created stack",
			action: "DescribeStacks",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "list-stack")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				stacks, ok := resp["Stacks"].([]any)
				require.True(t, ok)
				assert.Len(t, stacks, 1)
			},
		},
		{
			name:   "DescribeStacks filtered by name",
			action: "DescribeStacks",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "stack-a")
				createStack(t, h, "stack-b")
			},
			body:     map[string]any{"Names": []string{"stack-a"}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				stacks := resp["Stacks"].([]any)
				assert.Len(t, stacks, 1)
				s := stacks[0].(map[string]any)
				assert.Equal(t, "stack-a", s["Name"])
			},
		},
		{
			name:     "DescribeStacks unknown name returns error",
			action:   "DescribeStacks",
			body:     map[string]any{"Names": []string{"no-such-stack"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UpdateStack changes display name",
			action: "UpdateStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "upd-stack")
			},
			body:     map[string]any{"Name": "upd-stack", "DisplayName": "Updated"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				stack := resp["Stack"].(map[string]any)
				assert.Equal(t, "Updated", stack["DisplayName"])
			},
		},
		{
			name:     "UpdateStack unknown name returns error",
			action:   "UpdateStack",
			body:     map[string]any{"Name": "no-such-stack"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteStack removes stack",
			action: "DeleteStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "del-stack")
			},
			body:     map[string]any{"Name": "del-stack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteStack unknown name returns error",
			action:   "DeleteStack",
			body:     map[string]any{"Name": "no-such-stack"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestAppStream_StackErrorCodes covers AWS-accuracy gaps in Stack error
// __type values (as corrected by an audit against the real aws-sdk-go-v2
// operation-scoped error deserializers, which only recognize a subset of
// exception shapes per operation):
//  1. ErrAlreadyExists __type: ResourceAlreadyExistsException (not
//     InvalidParameterCombinationException)
//  2. DeleteStack with associated fleets → ResourceInUseException.
func TestAppStream_StackErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		body     any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name:   "CreateStack duplicate returns ResourceAlreadyExistsException",
			action: "CreateStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "dup-stack")
			},
			body:     map[string]any{"Name": "dup-stack"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceAlreadyExistsException",
		},
		{
			name:   "DeleteStack with associated fleet returns ResourceInUseException",
			action: "DeleteStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "in-use-stack")
				createFleet(t, h, "in-use-fleet")
				rec := doRequest(t, h, "AssociateFleet", map[string]any{
					"FleetName": "in-use-fleet",
					"StackName": "in-use-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "in-use-stack"},
			wantCode: http.StatusBadRequest,
			wantType: "ResourceInUseException",
		},
		{
			name:   "DeleteStack succeeds after fleet disassociation",
			action: "DeleteStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "free-stack")
				createFleet(t, h, "free-fleet")
				rec := doRequest(t, h, "AssociateFleet", map[string]any{
					"FleetName": "free-fleet",
					"StackName": "free-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				rec = doRequest(t, h, "DisassociateFleet", map[string]any{
					"FleetName": "free-fleet",
					"StackName": "free-stack",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "free-stack"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantType != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tc.wantType, resp["__type"], "wrong __type in error response")
			}
		})
	}
}

// TestAppStream_StackARNFormat verifies that stack ARNs match the AWS format
// arn:aws:appstream:<region>:<account>:stack/<name>.
func TestAppStream_StackARNFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStack", map[string]any{"Name": "arn-stack"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stack := resp["Stack"].(map[string]any)
	assert.Regexp(t, `^arn:aws:appstream:[a-z0-9-]+:\d+:stack/arn-stack$`, stack["Arn"])
}
