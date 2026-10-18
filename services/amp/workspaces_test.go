package amp_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "create then active", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{
				Alias: aws.String("  prod  "), Tags: map[string]string{"env": "prod"},
			})
			require.NoError(t, err)
			assert.Equal(t, types.WorkspaceStatusCodeCreating, out.Status.StatusCode)
			assert.Regexp(t, `^arn:aws:aps:us-east-1:\d{12}:workspace/ws-[0-9a-f-]{36}$`, aws.ToString(out.Arn))

			d, err := f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: out.WorkspaceId},
			)
			require.NoError(t, err)
			assert.Equal(t, "prod", aws.ToString(d.Workspace.Alias))
			assert.Equal(t, types.WorkspaceStatusCodeCreating, d.Workspace.Status.StatusCode)
			assert.Equal(t, map[string]string{"env": "prod"}, d.Workspace.Tags)
			assert.Contains(
				t,
				aws.ToString(d.Workspace.PrometheusEndpoint),
				"/workspaces/"+aws.ToString(out.WorkspaceId)+"/",
			)
			assert.WithinDuration(t, f.clock.now(), aws.ToTime(d.Workspace.CreatedAt), time.Second)

			f.settle()

			d, err = f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: out.WorkspaceId},
			)
			require.NoError(t, err)
			assert.Equal(t, types.WorkspaceStatusCodeActive, d.Workspace.Status.StatusCode)
		}},
		{name: "describe missing", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: aws.String("ws-nope")},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "client token replays", run: func(t *testing.T, f *fixture) {
			t.Helper()

			in := &ampsdk.CreateWorkspaceInput{Alias: aws.String("a"), ClientToken: aws.String("tok")}
			a, err := f.client.CreateWorkspace(t.Context(), in)
			require.NoError(t, err)
			b, err := f.client.CreateWorkspace(t.Context(), in)
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(a.WorkspaceId), aws.ToString(b.WorkspaceId))

			_, err = f.client.CreateWorkspace(
				t.Context(),
				&ampsdk.CreateWorkspaceInput{Alias: aws.String("other"), ClientToken: aws.String("tok")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "bad input", run: func(t *testing.T, f *fixture) {
			t.Helper()

			_, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{KmsKeyArn: aws.String("nope")})
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = f.client.CreateWorkspace(
				t.Context(),
				&ampsdk.CreateWorkspaceInput{Tags: map[string]string{"aws:x": "y"}},
			)
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "list filter and pagination", run: func(t *testing.T, f *fixture) {
			t.Helper()

			for i := range 5 {
				alias := fmt.Sprintf("team-%d", i)
				if i == 4 {
					alias = "other"
				}

				_, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{Alias: aws.String(alias)})
				require.NoError(t, err)
			}

			var seen int

			p := ampsdk.NewListWorkspacesPaginator(f.client, &ampsdk.ListWorkspacesInput{
				Alias: aws.String("team-"), MaxResults: aws.Int32(3),
			})
			for p.HasMorePages() {
				pg, err := p.NextPage(t.Context())
				require.NoError(t, err)

				seen += len(pg.Workspaces)
			}

			assert.Equal(t, 4, seen)

			_, err := f.client.ListWorkspaces(t.Context(), &ampsdk.ListWorkspacesInput{MaxResults: aws.Int32(5000)})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
		{name: "update alias", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.workspace(t)
			_, err := f.client.UpdateWorkspaceAlias(t.Context(), &ampsdk.UpdateWorkspaceAliasInput{
				WorkspaceId: aws.String(id), Alias: aws.String("renamed"),
			})
			require.NoError(t, err)

			d, err := f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)},
			)
			require.NoError(t, err)
			assert.Equal(t, "renamed", aws.ToString(d.Workspace.Alias))
		}},
		{name: "mutation while creating conflicts", run: func(t *testing.T, f *fixture) {
			t.Helper()

			out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
			require.NoError(t, err)

			_, err = f.client.UpdateWorkspaceAlias(t.Context(), &ampsdk.UpdateWorkspaceAliasInput{
				WorkspaceId: out.WorkspaceId, Alias: aws.String("x"),
			})
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "delete removes workspace and children", run: func(t *testing.T, f *fixture) {
			t.Helper()

			id := f.workspace(t)
			_, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: aws.String(id), Name: aws.String("ns"), Data: b64(validRules),
			})
			require.NoError(t, err)

			_, err = f.client.DeleteWorkspace(t.Context(), &ampsdk.DeleteWorkspaceInput{WorkspaceId: aws.String(id)})
			require.NoError(t, err)

			d, err := f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.WorkspaceStatusCodeDeleting, d.Workspace.Status.StatusCode)

			f.settle()

			_, err = f.client.DescribeWorkspace(
				t.Context(),
				&ampsdk.DescribeWorkspaceInput{WorkspaceId: aws.String(id)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			_, err = f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
			require.NoError(t, err)
			assert.Equal(t, 1, f.backend.WorkspaceCount())
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newFixture(t))
		})
	}
}

func TestWorkspaceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, id string)
		name string
	}{
		{name: "defaults", run: func(t *testing.T, f *fixture, id string) {
			t.Helper()

			out, err := f.client.DescribeWorkspaceConfiguration(
				t.Context(),
				&ampsdk.DescribeWorkspaceConfigurationInput{WorkspaceId: aws.String(id)},
			)
			require.NoError(t, err)
			assert.Equal(t, int32(150), aws.ToInt32(out.WorkspaceConfiguration.RetentionPeriodInDays))
			assert.Equal(t, types.WorkspaceConfigurationStatusCodeActive, out.WorkspaceConfiguration.Status.StatusCode)
			assert.Empty(t, out.WorkspaceConfiguration.LimitsPerLabelSet)
		}},
		{name: "update round trips", run: func(t *testing.T, f *fixture, id string) {
			t.Helper()

			up, err := f.client.UpdateWorkspaceConfiguration(t.Context(), &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(
					id,
				),
				RetentionPeriodInDays:         aws.Int32(30),
				OutOfOrderTimeWindowInSeconds: aws.Int32(600),
				LimitsPerLabelSet: []types.LimitsPerLabelSet{
					{
						LabelSet: map[string]string{
							"team": "a",
						},
						Limits: &types.LimitsPerLabelSetEntry{MaxSeries: aws.Int64(100)},
					},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, types.WorkspaceConfigurationStatusCodeUpdating, up.Status.StatusCode)

			f.settle()

			out, err := f.client.DescribeWorkspaceConfiguration(
				t.Context(),
				&ampsdk.DescribeWorkspaceConfigurationInput{WorkspaceId: aws.String(id)},
			)
			require.NoError(t, err)

			c := out.WorkspaceConfiguration
			assert.Equal(t, int32(30), aws.ToInt32(c.RetentionPeriodInDays))
			assert.Equal(t, int32(600), aws.ToInt32(c.OutOfOrderTimeWindowInSeconds))
			require.Len(t, c.LimitsPerLabelSet, 1)
			assert.Equal(t, int64(100), aws.ToInt64(c.LimitsPerLabelSet[0].Limits.MaxSeries))
		}},
		{name: "validation", run: func(t *testing.T, f *fixture, id string) {
			t.Helper()

			_, err := f.client.UpdateWorkspaceConfiguration(t.Context(), &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(id), RetentionPeriodInDays: aws.Int32(0),
			})
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = f.client.UpdateWorkspaceConfiguration(t.Context(), &ampsdk.UpdateWorkspaceConfigurationInput{
				WorkspaceId: aws.String(id),
				LimitsPerLabelSet: []types.LimitsPerLabelSet{
					{
						LabelSet: map[string]string{"a": "b"},
						Limits:   &types.LimitsPerLabelSetEntry{MaxSeries: aws.Int64(1)},
					},
					{
						LabelSet: map[string]string{"a": "b"},
						Limits:   &types.LimitsPerLabelSetEntry{MaxSeries: aws.Int64(2)},
					},
				},
			})
			assert.Equal(t, "ValidationException", apiCode(err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			tt.run(t, f, f.workspace(t))
		})
	}
}

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arn  func(ws, ns *string) string
		name string
	}{
		{name: "workspace", arn: func(ws, _ *string) string { return *ws }},
		{name: "namespace", arn: func(_, ns *string) string { return *ns }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			ws, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
			require.NoError(t, err)
			f.settle()

			ns, err := f.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
				WorkspaceId: ws.WorkspaceId, Name: aws.String("ns"), Data: b64(validRules),
			})
			require.NoError(t, err)

			arn := tt.arn(ws.Arn, ns.Arn)

			_, err = f.client.TagResource(t.Context(), &ampsdk.TagResourceInput{
				ResourceArn: aws.String(arn), Tags: map[string]string{"a": "1", "b": "2"},
			})
			require.NoError(t, err)

			_, err = f.client.UntagResource(
				t.Context(),
				&ampsdk.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"a"}},
			)
			require.NoError(t, err)

			out, err := f.client.ListTagsForResource(
				t.Context(),
				&ampsdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"b": "2"}, out.Tags)
		})
	}

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		f := newFixture(t)
		ws, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
		require.NoError(t, err)

		_, err = f.client.TagResource(t.Context(), &ampsdk.TagResourceInput{
			ResourceArn: aws.String(
				strings.Replace(aws.ToString(ws.Arn), "ws-", "ws-gone-", 1),
			),
			Tags: map[string]string{"a": "b"},
		})
		assert.Equal(t, "ResourceNotFoundException", apiCode(err))

		_, err = f.client.TagResource(t.Context(), &ampsdk.TagResourceInput{
			ResourceArn: aws.String("arn:aws:s3:::bucket"), Tags: map[string]string{"a": "b"},
		})
		assert.Equal(t, "ValidationException", apiCode(err))
	})
}
