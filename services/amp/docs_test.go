package amp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const logGroup = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/prometheus/ws"

func TestAlertManagerDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws *string)
		name string
	}{
		{name: "create describe put delete", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			c, err := f.client.CreateAlertManagerDefinition(
				t.Context(),
				&ampsdk.CreateAlertManagerDefinitionInput{WorkspaceId: ws, Data: b64(validAlertManager)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.AlertManagerDefinitionStatusCodeCreating, c.Status.StatusCode)
			f.settle()

			d, err := f.client.DescribeAlertManagerDefinition(
				t.Context(),
				&ampsdk.DescribeAlertManagerDefinitionInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			assert.Equal(t, validAlertManager, string(d.AlertManagerDefinition.Data))
			assert.Equal(t, types.AlertManagerDefinitionStatusCodeActive, d.AlertManagerDefinition.Status.StatusCode)

			next := "alertmanager_config: |\n  route:\n    receiver: other\n  receivers:\n    - name: other\n"
			p, err := f.client.PutAlertManagerDefinition(
				t.Context(),
				&ampsdk.PutAlertManagerDefinitionInput{WorkspaceId: ws, Data: b64(next)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.AlertManagerDefinitionStatusCodeUpdating, p.Status.StatusCode)
			f.settle()

			_, err = f.client.DeleteAlertManagerDefinition(
				t.Context(),
				&ampsdk.DeleteAlertManagerDefinitionInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeAlertManagerDefinition(
				t.Context(),
				&ampsdk.DescribeAlertManagerDefinitionInput{WorkspaceId: ws},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "duplicate create and missing put", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.PutAlertManagerDefinition(
				t.Context(),
				&ampsdk.PutAlertManagerDefinitionInput{WorkspaceId: ws, Data: b64(validAlertManager)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			create := func() *ampsdk.CreateAlertManagerDefinitionInput {
				return &ampsdk.CreateAlertManagerDefinitionInput{WorkspaceId: ws, Data: b64(validAlertManager)}
			}
			_, err = f.client.CreateAlertManagerDefinition(t.Context(), create())
			require.NoError(t, err)

			_, err = f.client.CreateAlertManagerDefinition(t.Context(), create())
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "client token replays", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			in := &ampsdk.CreateAlertManagerDefinitionInput{
				WorkspaceId: ws,
				Data:        b64(validAlertManager),
				ClientToken: aws.String("t1"),
			}
			_, err := f.client.CreateAlertManagerDefinition(t.Context(), in)
			require.NoError(t, err)

			_, err = f.client.CreateAlertManagerDefinition(t.Context(), in)
			require.NoError(t, err)
		}},
		{name: "invalid definitions", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			for _, bad := range []string{
				"not: [valid", "foo: bar\n", "alertmanager_config: 5\n",
				"alertmanager_config: |\n  receivers:\n    - name: a\n",
				"alertmanager_config: |\n  route:\n    receiver: missing\n  receivers:\n    - name: a\n",
			} {
				_, err := f.client.CreateAlertManagerDefinition(
					t.Context(),
					&ampsdk.CreateAlertManagerDefinitionInput{WorkspaceId: ws, Data: b64(bad)},
				)
				assert.Equal(t, "ValidationException", apiCode(err), bad)
			}
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

func TestLoggingConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws *string)
		name string
	}{
		{name: "rules logging lifecycle", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.DescribeLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeLoggingConfigurationInput{WorkspaceId: ws},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			c, err := f.client.CreateLoggingConfiguration(
				t.Context(),
				&ampsdk.CreateLoggingConfigurationInput{WorkspaceId: ws, LogGroupArn: aws.String(logGroup)},
			)
			require.NoError(t, err)
			assert.Equal(t, types.LoggingConfigurationStatusCodeCreating, c.Status.StatusCode)
			f.settle()

			u, err := f.client.UpdateLoggingConfiguration(
				t.Context(),
				&ampsdk.UpdateLoggingConfigurationInput{WorkspaceId: ws, LogGroupArn: aws.String(logGroup + "2")},
			)
			require.NoError(t, err)
			assert.Equal(t, types.LoggingConfigurationStatusCodeUpdating, u.Status.StatusCode)
			f.settle()

			d, err := f.client.DescribeLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeLoggingConfigurationInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			assert.Equal(t, logGroup+"2", aws.ToString(d.LoggingConfiguration.LogGroupArn))
			assert.Equal(t, aws.ToString(ws), aws.ToString(d.LoggingConfiguration.Workspace))

			_, err = f.client.DeleteLoggingConfiguration(
				t.Context(),
				&ampsdk.DeleteLoggingConfigurationInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeLoggingConfigurationInput{WorkspaceId: ws},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "rules logging errors", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.CreateLoggingConfiguration(
				t.Context(),
				&ampsdk.CreateLoggingConfigurationInput{WorkspaceId: ws, LogGroupArn: aws.String("bad")},
			)
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = f.client.UpdateLoggingConfiguration(
				t.Context(),
				&ampsdk.UpdateLoggingConfigurationInput{WorkspaceId: ws, LogGroupArn: aws.String(logGroup)},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			create := func() *ampsdk.CreateLoggingConfigurationInput {
				return &ampsdk.CreateLoggingConfigurationInput{WorkspaceId: ws, LogGroupArn: aws.String(logGroup)}
			}
			_, err = f.client.CreateLoggingConfiguration(t.Context(), create())
			require.NoError(t, err)

			_, err = f.client.CreateLoggingConfiguration(t.Context(), create())
			assert.Equal(t, "ConflictException", apiCode(err))
		}},
		{name: "query logging lifecycle", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			dest := []types.LoggingDestination{{
				CloudWatchLogs: &types.CloudWatchLogDestination{LogGroupArn: aws.String(logGroup)},
				Filters:        &types.LoggingFilter{QspThreshold: aws.Int64(1000)},
			}}

			_, err := f.client.CreateQueryLoggingConfiguration(
				t.Context(),
				&ampsdk.CreateQueryLoggingConfigurationInput{WorkspaceId: ws, Destinations: dest},
			)
			require.NoError(t, err)
			f.settle()

			dest[0].Filters.QspThreshold = aws.Int64(5)
			_, err = f.client.UpdateQueryLoggingConfiguration(
				t.Context(),
				&ampsdk.UpdateQueryLoggingConfigurationInput{WorkspaceId: ws, Destinations: dest},
			)
			require.NoError(t, err)
			f.settle()

			d, err := f.client.DescribeQueryLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeQueryLoggingConfigurationInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			require.Len(t, d.QueryLoggingConfiguration.Destinations, 1)
			assert.Equal(t, int64(5), aws.ToInt64(d.QueryLoggingConfiguration.Destinations[0].Filters.QspThreshold))

			_, err = f.client.DeleteQueryLoggingConfiguration(
				t.Context(),
				&ampsdk.DeleteQueryLoggingConfigurationInput{WorkspaceId: ws},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeQueryLoggingConfiguration(
				t.Context(),
				&ampsdk.DescribeQueryLoggingConfigurationInput{WorkspaceId: ws},
			)
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "query logging validation", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			one := types.LoggingDestination{
				CloudWatchLogs: &types.CloudWatchLogDestination{LogGroupArn: aws.String(logGroup)},
				Filters:        &types.LoggingFilter{QspThreshold: aws.Int64(1)},
			}
			bad := one
			bad.CloudWatchLogs = &types.CloudWatchLogDestination{LogGroupArn: aws.String("nope")}

			for _, dests := range [][]types.LoggingDestination{{}, {one, one}, {bad}} {
				_, err := f.client.CreateQueryLoggingConfiguration(
					t.Context(),
					&ampsdk.CreateQueryLoggingConfigurationInput{
						WorkspaceId: ws, Destinations: dests,
					},
				)
				assert.Equal(t, "ValidationException", apiCode(err))
			}
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

func TestResourcePolicy(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"aps:RemoteWrite","Resource":"*"}]}`

	tests := []struct {
		run  func(t *testing.T, f *fixture, ws *string)
		name string
	}{
		{name: "put describe update delete", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.DescribeResourcePolicy(t.Context(), &ampsdk.DescribeResourcePolicyInput{WorkspaceId: ws})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))

			p, err := f.client.PutResourcePolicy(
				t.Context(),
				&ampsdk.PutResourcePolicyInput{
					WorkspaceId:    ws,
					PolicyDocument: aws.String(doc),
					RevisionId:     aws.String("NO_POLICY"),
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.WorkspacePolicyStatusCodeCreating, p.PolicyStatus)
			f.settle()

			d, err := f.client.DescribeResourcePolicy(t.Context(), &ampsdk.DescribeResourcePolicyInput{WorkspaceId: ws})
			require.NoError(t, err)
			assert.JSONEq(t, doc, aws.ToString(d.PolicyDocument))
			assert.Equal(t, types.WorkspacePolicyStatusCodeActive, d.PolicyStatus)
			assert.Equal(t, aws.ToString(p.RevisionId), aws.ToString(d.RevisionId))

			_, err = f.client.PutResourcePolicy(
				t.Context(),
				&ampsdk.PutResourcePolicyInput{
					WorkspaceId:    ws,
					PolicyDocument: aws.String(doc),
					RevisionId:     aws.String("stale"),
				},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			p2, err := f.client.PutResourcePolicy(
				t.Context(),
				&ampsdk.PutResourcePolicyInput{
					WorkspaceId:    ws,
					PolicyDocument: aws.String(doc),
					RevisionId:     p.RevisionId,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, types.WorkspacePolicyStatusCodeUpdating, p2.PolicyStatus)
			assert.NotEqual(t, aws.ToString(p.RevisionId), aws.ToString(p2.RevisionId))
			f.settle()

			_, err = f.client.DeleteResourcePolicy(
				t.Context(),
				&ampsdk.DeleteResourcePolicyInput{WorkspaceId: ws, RevisionId: aws.String("stale")},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.DeleteResourcePolicy(
				t.Context(),
				&ampsdk.DeleteResourcePolicyInput{WorkspaceId: ws, RevisionId: p2.RevisionId},
			)
			require.NoError(t, err)
			f.settle()

			_, err = f.client.DescribeResourcePolicy(t.Context(), &ampsdk.DescribeResourcePolicyInput{WorkspaceId: ws})
			assert.Equal(t, "ResourceNotFoundException", apiCode(err))
		}},
		{name: "invalid policy and stale first revision", run: func(t *testing.T, f *fixture, ws *string) {
			t.Helper()

			_, err := f.client.PutResourcePolicy(
				t.Context(),
				&ampsdk.PutResourcePolicyInput{WorkspaceId: ws, PolicyDocument: aws.String("not json")},
			)
			assert.Equal(t, "ValidationException", apiCode(err))

			_, err = f.client.PutResourcePolicy(
				t.Context(),
				&ampsdk.PutResourcePolicyInput{
					WorkspaceId:    ws,
					PolicyDocument: aws.String(doc),
					RevisionId:     aws.String("rev"),
				},
			)
			assert.Equal(t, "ConflictException", apiCode(err))

			_, err = f.client.DeleteResourcePolicy(t.Context(), &ampsdk.DeleteResourcePolicyInput{WorkspaceId: ws})
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
