package appstream_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func newTestHandler(t *testing.T) *appstream.Handler {
	t.Helper()
	backend := appstream.NewInMemoryBackend("000000000000", "us-east-1")

	return appstream.NewHandler(backend)
}

func doRequest(t *testing.T, h *appstream.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "PhotonAdminProxyService."+action)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func createStack(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateStack", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, rec.Code)
}

func createFleet(t *testing.T, h *appstream.Handler, name string) {
	t.Helper()
	rec := doRequest(t, h, "CreateFleet", map[string]any{
		"Name":         name,
		"InstanceType": "stream.standard.medium",
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

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

func TestAppStream_Fleets(t *testing.T) {
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
			name:   "CreateFleet returns fleet with ARN and STOPPED state",
			action: "CreateFleet",
			body: map[string]any{
				"Name":         "my-fleet",
				"InstanceType": "stream.standard.medium",
				"FleetType":    "ON_DEMAND",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				fleet := resp["Fleet"].(map[string]any)
				assert.Equal(t, "my-fleet", fleet["Name"])
				assert.Equal(t, "STOPPED", fleet["State"])
				assert.Contains(t, fleet["Arn"], ":fleet/my-fleet")
			},
		},
		{
			name:   "CreateFleet duplicate returns error",
			action: "CreateFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "dup-fleet")
			},
			body: map[string]any{
				"Name":         "dup-fleet",
				"InstanceType": "stream.standard.medium",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeFleets returns created fleet",
			action: "DescribeFleets",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "list-fleet")
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				fleets := resp["Fleets"].([]any)
				assert.Len(t, fleets, 1)
			},
		},
		{
			name:   "StartFleet transitions to RUNNING",
			action: "StartFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "start-fleet")
			},
			body:     map[string]any{"Name": "start-fleet"},
			wantCode: http.StatusOK,
		},
		{
			name:   "StopFleet transitions to STOPPED",
			action: "StopFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "stop-fleet")
				doRequest(t, h, "StartFleet", map[string]any{"Name": "stop-fleet"})
			},
			body:     map[string]any{"Name": "stop-fleet"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteFleet while RUNNING returns error",
			action: "DeleteFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "running-fleet")
				doRequest(t, h, "StartFleet", map[string]any{"Name": "running-fleet"})
			},
			body:     map[string]any{"Name": "running-fleet"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteFleet while STOPPED succeeds",
			action: "DeleteFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "del-fleet")
			},
			body:     map[string]any{"Name": "del-fleet"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteFleet unknown name returns error",
			action:   "DeleteFleet",
			body:     map[string]any{"Name": "no-such-fleet"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UpdateFleet changes instance type",
			action: "UpdateFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "upd-fleet")
			},
			body:     map[string]any{"Name": "upd-fleet", "InstanceType": "stream.compute.large"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				fleet := resp["Fleet"].(map[string]any)
				assert.Equal(t, "stream.compute.large", fleet["InstanceType"])
			},
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

func TestAppStream_Associations(t *testing.T) {
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
			name:   "AssociateFleet links fleet and stack",
			action: "AssociateFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "fleet-1")
				createStack(t, h, "stack-1")
			},
			body:     map[string]any{"FleetName": "fleet-1", "StackName": "stack-1"},
			wantCode: http.StatusOK,
		},
		{
			name:   "AssociateFleet unknown fleet returns error",
			action: "AssociateFleet",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "stack-x")
			},
			body:     map[string]any{"FleetName": "no-fleet", "StackName": "stack-x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "ListAssociatedFleets returns associated fleet",
			action: "ListAssociatedFleets",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "fleet-2")
				createStack(t, h, "stack-2")
				doRequest(t, h, "AssociateFleet", map[string]any{"FleetName": "fleet-2", "StackName": "stack-2"})
			},
			body:     map[string]any{"StackName": "stack-2"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				names := resp["Names"].([]any)
				assert.Len(t, names, 1)
				assert.Equal(t, "fleet-2", names[0])
			},
		},
		{
			name:   "ListAssociatedStacks returns associated stack",
			action: "ListAssociatedStacks",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "fleet-3")
				createStack(t, h, "stack-3")
				doRequest(t, h, "AssociateFleet", map[string]any{"FleetName": "fleet-3", "StackName": "stack-3"})
			},
			body:     map[string]any{"FleetName": "fleet-3"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				names := resp["Names"].([]any)
				assert.Len(t, names, 1)
				assert.Equal(t, "stack-3", names[0])
			},
		},
		{
			name:   "DisassociateFleet removes link",
			action: "DisassociateFleet",
			setup: func(h *appstream.Handler) {
				createFleet(t, h, "fleet-4")
				createStack(t, h, "stack-4")
				doRequest(t, h, "AssociateFleet", map[string]any{"FleetName": "fleet-4", "StackName": "stack-4"})
			},
			body:     map[string]any{"FleetName": "fleet-4", "StackName": "stack-4"},
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
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAppStream_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler) string
		check    func(t *testing.T, body []byte)
		body     func(arn string) any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "TagResource applies tags to stack",
			action: "TagResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "tag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"tag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "prod"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			action: "ListTagsForResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "listtag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"listtag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
				doRequest(
					t,
					h,
					"TagResource",
					map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "staging"}},
				)

				return arn
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "staging", tags["env"])
			},
		},
		{
			name:   "UntagResource removes tags",
			action: "UntagResource",
			setup: func(h *appstream.Handler) string {
				createStack(t, h, "untag-stack")
				rec := doRequest(t, h, "DescribeStacks", map[string]any{"Names": []string{"untag-stack"}})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["Stacks"].([]any)[0].(map[string]any)["Arn"].(string)
				doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v"}})

				return arn
			},
			body: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "TagKeys": []string{"k"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource unknown ARN returns error",
			action: "ListTagsForResource",
			setup:  func(_ *appstream.Handler) string { return "" },
			body: func(_ string) any {
				return map[string]any{"ResourceArn": "arn:aws:appstream:us-east-1:000000000000:stack/no-such"}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			arn := tc.setup(h)
			rec := doRequest(t, h, tc.action, tc.body(arn))
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
