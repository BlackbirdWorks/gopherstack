package azurearm_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (s *logicStack) postType(t *testing.T, name, contentType, body string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, s.callback(t, name), bytes.NewBufferString(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())
	s.waitRuns(t)
}

func (s *logicStack) runWorkflow(t *testing.T, name string, actions la) {
	t.Helper()

	_, err := s.factory.NewWorkflowsClient().CreateOrUpdate(t.Context(), logicRG, name, armlogic.Workflow{
		Location:   new("westus"),
		Properties: &armlogic.WorkflowProperties{Definition: logicDef(requestTrigger(nil), actions)},
	}, nil)
	require.NoError(t, err)
}

func TestLogicScopeFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		actions la
		check   func(t *testing.T, got any)
		name    string
	}{
		{
			name: "result of a scope lists its first-level actions",
			actions: la{
				"S": la{"type": "Scope", "runAfter": la{}, "actions": la{
					"A": compose("one", nil),
					"B": compose("two", after("A", "Succeeded")),
				}},
				"Out": compose("@result('S')", after("S", "Succeeded")),
			},
			check: func(t *testing.T, got any) {
				t.Helper()

				list, ok := got.([]any)
				require.True(t, ok)
				require.Len(t, list, 2)

				first, _ := list[0].(map[string]any)
				second, _ := list[1].(map[string]any)

				assert.Equal(t, "A", first["name"])
				assert.Equal(t, "Succeeded", first["status"])
				assert.Equal(t, map[string]any{"body": "one"}, first["outputs"])
				assert.Equal(t, "B", second["name"])
			},
		},
		{
			name: "result of a foreach lists every iteration",
			actions: la{
				"F": la{
					"type": "Foreach", "foreach": []any{"x", "y"}, "runAfter": la{},
					"actions": la{"In": compose("@item()", nil)},
				},
				"Out": compose("@result('F')", after("F", "Succeeded")),
			},
			check: func(t *testing.T, got any) {
				t.Helper()

				list, ok := got.([]any)
				require.True(t, ok)
				require.Len(t, list, 2)

				values := map[any]bool{}

				for _, e := range list {
					m, _ := e.(map[string]any)
					assert.Equal(t, "In", m["name"])

					out, _ := m["outputs"].(map[string]any)
					values[out["body"]] = true
				}

				assert.Equal(t, map[any]bool{"x": true, "y": true}, values)
			},
		},
		{
			name: "result of a non scope fails",
			actions: la{
				"A":   compose("x", nil),
				"Out": compose("@result('A')", after("A", "Succeeded")),
			},
			check: nil,
		},
		{
			name: "iterationIndexes inside until",
			actions: la{
				"Init": initVar("i", "Integer", 0),
				"Arr":  initVar("seen", "Array", []any{}),
				"Loop": la{
					"type":       "Until",
					"expression": "@equals(variables('i'), 3)",
					"runAfter":   after("Init", "Succeeded", "Arr", "Succeeded"),
					"limit":      la{"count": 10, "timeout": "PT1H"},
					"actions": la{
						"Inc": varOp("IncrementVariable", "i", 1, la{}),
						"Rec": varOp(
							"AppendToArrayVariable",
							"seen",
							"@iterationIndexes('Loop')",
							after("Inc", "Succeeded"),
						),
					},
				},
				"Out": compose("@variables('seen')", after("Loop", "Succeeded")),
			},
			check: func(t *testing.T, got any) {
				t.Helper()
				assert.Equal(t, []any{float64(0), float64(1), float64(2)}, got)
			},
		},
		{
			name: "iterationIndexes outside until fails",
			actions: la{
				"Out": compose("@iterationIndexes('Loop')", nil),
			},
			check: nil,
		},
		{
			name: "listCallbackUrl outside a webhook fails",
			actions: la{
				"Out": compose("@listCallbackUrl()", nil),
			},
			check: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.runWorkflow(t, "wf", tc.actions)

			out := s.invoke(t, "wf", "{}")

			if tc.check == nil {
				assert.Equal(t, "Failed", string(*out.run.Properties.Status))
				assert.Equal(t, "Failed", string(*out.actions["Out"].Properties.Status))
				assert.Equal(t, "InvalidTemplate", *out.actions["Out"].Properties.Code)

				return
			}

			require.Equal(t, "Succeeded", string(*out.run.Properties.Status))
			tc.check(t, s.content(t, out.actions["Out"].Properties.OutputsLink))
		})
	}
}

func TestLogicFormDataFunctions(t *testing.T) {
	t.Parallel()

	const boundary = "XBOUNDARY"

	multipartBody := "--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"Subject\"\r\n\r\nHello\r\n" +
		"--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"Tag\"\r\n\r\na\r\n" +
		"--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"Tag\"\r\n\r\nb\r\n" +
		"--" + boundary + "--\r\n"

	mixedBody := "--" + boundary + "\r\n" +
		"Content-Type: application/json\r\n\r\n{\"n\":1}\r\n" +
		"--" + boundary + "\r\n" +
		"Content-Type: text/plain\r\n\r\nplain\r\n" +
		"--" + boundary + "--\r\n"

	tests := []struct {
		want        any
		name        string
		contentType string
		body        string
		expr        string
	}{
		{
			name: "urlencoded single value", contentType: "application/x-www-form-urlencoded",
			body: "Subject=Hello+World&k=v", expr: "@triggerFormDataValue('Subject')", want: "Hello World",
		},
		{
			name: "urlencoded multi values", contentType: "application/x-www-form-urlencoded",
			body: "t=a&t=b&u=c", expr: "@triggerFormDataMultiValues('t')", want: []any{"a", "b"},
		},
		{
			name: "urlencoded duplicate fails the single form", contentType: "application/x-www-form-urlencoded",
			body: "t=a&t=b", expr: "@triggerFormDataValue('t')",
		},
		{
			name: "urlencoded missing key fails", contentType: "application/x-www-form-urlencoded",
			body: "t=a", expr: "@triggerFormDataValue('zzz')",
		},
		{
			name: "multipart form data single", contentType: "multipart/form-data; boundary=" + boundary,
			body: multipartBody, expr: "@triggerFormDataValue('Subject')", want: "Hello",
		},
		{
			name: "multipart form data multi", contentType: "multipart/form-data; boundary=" + boundary,
			body: multipartBody, expr: "@triggerFormDataMultiValues('Tag')", want: []any{"a", "b"},
		},
		{
			name: "action form data value", contentType: "application/x-www-form-urlencoded",
			body: "k=v", expr: "@formDataValue('Src', 'k')", want: "v",
		},
		{
			name: "action form data multi values", contentType: "application/x-www-form-urlencoded",
			body: "k=v&k=w", expr: "@formDataMultiValues('Src', 'k')", want: []any{"v", "w"},
		},
		{
			name: "trigger multipart body json part", contentType: "multipart/mixed; boundary=" + boundary,
			body: mixedBody, expr: "@triggerMultipartBody(0)", want: map[string]any{"n": float64(1)},
		},
		{
			name: "trigger multipart body text part", contentType: "multipart/mixed; boundary=" + boundary,
			body: mixedBody, expr: "@triggerMultipartBody(1)", want: "plain",
		},
		{
			name: "action multipart body", contentType: "multipart/mixed; boundary=" + boundary,
			body: mixedBody, expr: "@multipartBody('Src', 1)", want: "plain",
		},
		{
			name: "multipart index out of range fails", contentType: "multipart/mixed; boundary=" + boundary,
			body: mixedBody, expr: "@triggerMultipartBody(5)",
		},
		{
			name: "json body is not form data", contentType: "application/json",
			body: `{"a":1}`, expr: "@triggerFormDataValue('a')",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.runWorkflow(t, "wf", la{
				"Src": compose("@triggerBody()", nil),
				"Out": compose(tc.expr, after("Src", "Succeeded")),
			})

			s.postType(t, "wf", tc.contentType, tc.body)

			actions := s.actions(t, "wf")
			require.Contains(t, actions, "Out")

			if tc.want == nil {
				assert.Equal(t, "Failed", string(*actions["Out"].Properties.Status))

				return
			}

			require.Equal(t, "Succeeded", string(*actions["Out"].Properties.Status))
			assert.Equal(t, tc.want, s.content(t, actions["Out"].Properties.OutputsLink))
		})
	}
}
