package amp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuleGroupsNamespaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws *string)
		name string
	}{
		{name: "create describe list", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			out, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: ws, Name: aws.String("alpha"), Data: b64(validRules), Tags: map[string]string{"k": "v"},
			})
			require.NoError(t, err)
			assert.Equal(t, types.RuleGroupsNamespaceStatusCodeCreating, out.Status.StatusCode)
			assert.Contains(t, aws.ToString(out.Arn), ":rulegroupsnamespace/"+aws.ToString(ws)+"/alpha")

			f.settle()

			d, err := f.client.DescribeRuleGroupsNamespace(
				t.Context(),
				&ampsdk.DescribeRuleGroupsNamespaceInput{WorkspaceId: ws, Name: aws.String("alpha")},
			)
			require.NoError(t, err)
			assert.Equal(t, validRules, string(d.RuleGroupsNamespace.Data))
			assert.Equal(t, types.RuleGroupsNamespaceStatusCodeActive, d.RuleGroupsNamespace.Status.StatusCode)
			assert.Equal(t, map[string]string{"k": "v"}, d.RuleGroupsNamespace.Tags)

			_, err = f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: ws, Name: aws.String("beta"), Data: b64(validRules),
			})
			require.NoError(t, err)

			l, err := f.client.ListRuleGroupsNamespaces(
				t.Context(),
				&ampsdk.ListRuleGroupsNamespacesInput{WorkspaceId: ws, Name: aws.String("al")},
			)
			require.NoError(t, err)
			require.Len(t, l.RuleGroupsNamespaces, 1)
			assert.Equal(t, "alpha", aws.ToString(l.RuleGroupsNamespaces[0].Name))
		}},
		{name: "duplicate conflicts", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			in := func() *ampsdk.CreateRuleGroupsNamespaceInput {
				return &ampsdk.CreateRuleGroupsNamespaceInput{
					WorkspaceId: ws,
					Name:        aws.String("dup"),
					Data:        b64(validRules),
				}
			}
			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), in())
			require.NoError(t, err)

			_, err = f.client.CreateRuleGroupsNamespace(t.Context(), in())
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "put replaces", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: ws, Name: aws.String("ns"), Data: b64(validRules),
			})
			require.NoError(t, err)
			f.settle()

			next := "groups:\n  - name: g2\n    rules:\n      - record: r\n        expr: vector(1)\n"
			out, err := f.client.PutRuleGroupsNamespace(t.Context(), &ampsdk.PutRuleGroupsNamespaceInput{
				WorkspaceId: ws, Name: aws.String("ns"), Data: b64(next),
			})
			require.NoError(t, err)
			assert.Equal(t, types.RuleGroupsNamespaceStatusCodeUpdating, out.Status.StatusCode)

			f.settle()

			d, err := f.client.DescribeRuleGroupsNamespace(
				t.Context(),
				&ampsdk.DescribeRuleGroupsNamespaceInput{WorkspaceId: ws, Name: aws.String("ns")},
			)
			require.NoError(t, err)
			assert.Equal(t, next, string(d.RuleGroupsNamespace.Data))
		}},
		{name: "delete", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: ws, Name: aws.String("ns"), Data: b64(validRules),
			})
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DeleteRuleGroupsNamespace(
				t.Context(),
				&ampsdk.DeleteRuleGroupsNamespaceInput{WorkspaceId: ws, Name: aws.String("ns")},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeRuleGroupsNamespace(
				t.Context(),
				&ampsdk.DescribeRuleGroupsNamespaceInput{WorkspaceId: ws, Name: aws.String("ns")},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "unknown workspace", run: func(t *testing.T, f *fixture, _ *string) {
			t.Helper()

			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: aws.String("ws-missing"), Name: aws.String("ns"), Data: b64(validRules),
			})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, aws.String(f.workspace(t)))
		})
	}
}

func TestRuleGroupsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data string
	}{
		{name: "not yaml", data: "groups: [unterminated"},
		{name: "no groups", data: "foo: bar\n"},
		{name: "scalar document", data: "just a string\n"},
		{name: "group without name", data: "groups:\n  - rules:\n      - record: r\n        expr: up\n"},
		{
			name: "duplicate group names",
			data: "groups:\n  - name: g\n    rules:\n      - record: r\n        expr: up\n" +
				"  - name: g\n    rules:\n      - record: r\n        expr: up\n",
		},
		{name: "empty rules", data: "groups:\n  - name: g\n    rules: []\n"},
		{
			name: "both record and alert",
			data: "groups:\n  - name: g\n    rules:\n      - record: r\n        alert: a\n        expr: up\n",
		},
		{name: "neither record nor alert", data: "groups:\n  - name: g\n    rules:\n      - expr: up\n"},
		{name: "unbalanced expr", data: "groups:\n  - name: g\n    rules:\n      - record: r\n        expr: sum(up\n"},
		{name: "missing expr", data: "groups:\n  - name: g\n    rules:\n      - record: r\n"},
		{
			name: "bad for",
			data: "groups:\n  - name: g\n    rules:\n      - alert: a\n        expr: up\n        for: soon\n",
		},
		{name: "empty data", data: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: aws.String(f.workspace(t)), Name: aws.String("ns"), Data: b64(tt.data),
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		})
	}

	t.Run("bad names", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		id := f.workspace(t)

		for _, name := range []string{"", "-lead", "has space", string(make([]byte, 65))} {
			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: aws.String(id), Name: aws.String(name), Data: b64(validRules),
			})
			require.Error(t, err, name)
		}
	})
}
