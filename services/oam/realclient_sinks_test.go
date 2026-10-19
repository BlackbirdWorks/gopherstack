package oam_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	oamsdk "github.com/aws/aws-sdk-go-v2/service/oam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createSink(t *testing.T, f *fixture) *oamsdk.CreateSinkOutput {
	t.Helper()

	out, err := f.monitoring.CreateSink(t.Context(), &oamsdk.CreateSinkInput{Name: aws.String("central")})
	require.NoError(t, err)

	return out
}

func TestSinks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "create_get_list_delete",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.monitoring.CreateSink(t.Context(), &oamsdk.CreateSinkInput{
					Name: aws.String("central"), Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)
				assert.Regexp(t, `^arn:aws:oam:us-east-1:000000000000:sink/[0-9a-f-]{36}$`, aws.ToString(out.Arn))
				assert.Equal(t, "central", aws.ToString(out.Name))
				assert.Equal(t, "prod", out.Tags["env"])

				byARN, err := f.monitoring.GetSink(t.Context(), &oamsdk.GetSinkInput{Identifier: out.Arn})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(out.Id), aws.ToString(byARN.Id))
				assert.Empty(t, byARN.Tags)

				byID, err := f.monitoring.GetSink(t.Context(), &oamsdk.GetSinkInput{
					Identifier: out.Id, IncludeTags: aws.Bool(true),
				})
				require.NoError(t, err)
				assert.Equal(t, "prod", byID.Tags["env"])

				list, err := f.monitoring.ListSinks(t.Context(), &oamsdk.ListSinksInput{})
				require.NoError(t, err)
				require.Len(t, list.Items, 1)
				assert.Equal(t, "central", aws.ToString(list.Items[0].Name))

				_, err = f.monitoring.DeleteSink(t.Context(), &oamsdk.DeleteSinkInput{Identifier: out.Arn})
				require.NoError(t, err)

				_, err = f.monitoring.GetSink(t.Context(), &oamsdk.GetSinkInput{Identifier: out.Arn})
				assert.Equal(t, "ResourceNotFoundException", apiCode(err))
			},
		},
		{
			name: "duplicate_name_conflicts",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				createSink(t, f)

				_, err := f.monitoring.CreateSink(t.Context(), &oamsdk.CreateSinkInput{Name: aws.String("central")})
				assert.Equal(t, "ConflictException", apiCode(err))
			},
		},
		{
			name: "one_sink_per_region",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				createSink(t, f)

				_, err := f.monitoring.CreateSink(t.Context(), &oamsdk.CreateSinkInput{Name: aws.String("second")})
				assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))
			},
		},
		{
			name: "sinks_are_per_account",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				sink := createSink(t, f)

				_, err := f.source.CreateSink(t.Context(), &oamsdk.CreateSinkInput{Name: aws.String("central")})
				require.NoError(t, err)

				_, err = f.source.GetSink(t.Context(), &oamsdk.GetSinkInput{Identifier: sink.Arn})
				assert.Equal(t, "ResourceNotFoundException", apiCode(err))
			},
		},
		{
			name: "invalid_name",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.monitoring.CreateSink(t.Context(), &oamsdk.CreateSinkInput{Name: aws.String("has space")})
				assert.Equal(t, "InvalidParameterException", apiCode(err))
			},
		},
		{
			name: "delete_blocked_by_links",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				sink := createSink(t, f)
				allowAll(t, f, sink.Arn)
				createLink(t, f, sink.Arn, "label", []string{"AWS::CloudWatch::Metric"})

				_, err := f.monitoring.DeleteSink(t.Context(), &oamsdk.DeleteSinkInput{Identifier: sink.Arn})
				assert.Equal(t, "ConflictException", apiCode(err))
			},
		},
		{
			name: "pagination_validation",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.monitoring.ListSinks(t.Context(), &oamsdk.ListSinksInput{MaxResults: aws.Int32(51)})
				assert.Equal(t, "InvalidParameterException", apiCode(err))

				_, err = f.monitoring.ListSinks(t.Context(), &oamsdk.ListSinksInput{NextToken: aws.String("%%%")})
				assert.Equal(t, "InvalidParameterException", apiCode(err))
			},
		},
		{
			name: "policy_roundtrip",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				sink := createSink(t, f)

				_, err := f.monitoring.GetSinkPolicy(t.Context(), &oamsdk.GetSinkPolicyInput{SinkIdentifier: sink.Arn})
				assert.Equal(t, "ResourceNotFoundException", apiCode(err))

				policy := allowPolicy(sourceAccount, "")
				put, err := f.monitoring.PutSinkPolicy(t.Context(), &oamsdk.PutSinkPolicyInput{
					SinkIdentifier: sink.Id, Policy: aws.String(policy),
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(sink.Arn), aws.ToString(put.SinkArn))
				assert.JSONEq(t, policy, aws.ToString(put.Policy))

				got, err := f.monitoring.GetSinkPolicy(
					t.Context(),
					&oamsdk.GetSinkPolicyInput{SinkIdentifier: sink.Arn},
				)
				require.NoError(t, err)
				assert.JSONEq(t, policy, aws.ToString(got.Policy))
				assert.Equal(t, aws.ToString(sink.Id), aws.ToString(got.SinkId))
			},
		},
		{
			name: "invalid_policies",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				sink := createSink(t, f)

				for _, bad := range []string{
					`not json`,
					`{"Version":"2012-10-17"}`,
					`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"oam:CreateLink","Resource":"*"}]}`,
					`{"Statement":[{"Effect":"Allow","Action":"oam:CreateLink","Resource":"*"}]}`,
					`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}]}`,
				} {
					_, err := f.monitoring.PutSinkPolicy(t.Context(), &oamsdk.PutSinkPolicyInput{
						SinkIdentifier: sink.Arn, Policy: aws.String(bad),
					})
					assert.Equal(t, "InvalidParameterException", apiCode(err), bad)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}
