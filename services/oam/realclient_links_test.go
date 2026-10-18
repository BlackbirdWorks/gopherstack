package oam_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	oamsdk "github.com/aws/aws-sdk-go-v2/service/oam"
	oamtypes "github.com/aws/aws-sdk-go-v2/service/oam/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/oam"
)

func allowPolicy(account, condition string) string {
	stmt := map[string]any{
		"Effect":    "Allow",
		"Principal": map[string]any{"AWS": account},
		"Action":    []string{"oam:CreateLink", "oam:UpdateLink"},
		"Resource":  "*",
	}

	if condition != "" {
		var c any
		_ = json.Unmarshal([]byte(condition), &c)
		stmt["Condition"] = c
	}

	b, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{stmt}})

	return string(b)
}

func allowAll(t *testing.T, f *fixture, sinkARN *string) {
	t.Helper()

	_, err := f.monitoring.PutSinkPolicy(t.Context(), &oamsdk.PutSinkPolicyInput{
		SinkIdentifier: sinkARN, Policy: aws.String(allowPolicy(sourceAccount, "")),
	})
	require.NoError(t, err)
}

func toTypes(in []string) []oamtypes.ResourceType {
	out := make([]oamtypes.ResourceType, len(in))
	for i, s := range in {
		out[i] = oamtypes.ResourceType(s)
	}

	return out
}

func createLink(t *testing.T, f *fixture, sinkARN *string, label string, types []string) *oamsdk.CreateLinkOutput {
	t.Helper()

	out, err := f.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
		LabelTemplate: aws.String(label), ResourceTypes: toTypes(types), SinkIdentifier: sinkARN,
	})
	require.NoError(t, err)

	return out
}

func TestLinks_Lifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sink := createSink(t, f)
	allowAll(t, f, sink.Arn)

	link, err := f.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
		LabelTemplate: aws.String("prod-$AccountName"), SinkIdentifier: sink.Arn, Tags: map[string]string{"a": "b"},
		ResourceTypes: toTypes([]string{"AWS::CloudWatch::Metric", "AWS::Logs::LogGroup", "AWS::CloudWatch::Metric"}),
		LinkConfiguration: &oamtypes.LinkConfiguration{
			LogGroupConfiguration: &oamtypes.LogGroupConfiguration{
				Filter: aws.String("LogGroupName LIKE 'aws/lambda/%' AND LogGroupName NOT IN ('x', 'y')"),
			},
			MetricConfiguration: &oamtypes.MetricConfiguration{
				Filter: aws.String("Namespace IN ('AWS/EC2', 'AWS/ELB')"),
			},
		},
	})
	require.NoError(t, err)
	assert.Regexp(t, `^arn:aws:oam:us-east-1:111122223333:link/[0-9a-f-]{36}$`, aws.ToString(link.Arn))
	assert.Equal(t, "prod-"+sourceAccount, aws.ToString(link.Label))
	assert.Equal(t, "prod-$AccountName", aws.ToString(link.LabelTemplate))
	assert.Equal(t, aws.ToString(sink.Arn), aws.ToString(link.SinkArn))
	assert.Equal(t, []string{"AWS::CloudWatch::Metric", "AWS::Logs::LogGroup"}, link.ResourceTypes)
	assert.Equal(t, "b", link.Tags["a"])

	got, err := f.source.GetLink(t.Context(), &oamsdk.GetLinkInput{Identifier: link.Id, IncludeTags: aws.Bool(true)})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(got.LinkConfiguration.LogGroupConfiguration.Filter), "aws/lambda")
	assert.Equal(t, "b", got.Tags["a"])

	list, err := f.source.ListLinks(t.Context(), &oamsdk.ListLinksInput{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, aws.ToString(sink.Arn), aws.ToString(list.Items[0].SinkArn))

	empty, err := f.monitoring.ListLinks(t.Context(), &oamsdk.ListLinksInput{})
	require.NoError(t, err)
	assert.Empty(t, empty.Items)

	attached, err := f.monitoring.ListAttachedLinks(
		t.Context(),
		&oamsdk.ListAttachedLinksInput{SinkIdentifier: sink.Arn},
	)
	require.NoError(t, err)
	require.Len(t, attached.Items, 1)
	assert.Equal(t, aws.ToString(link.Arn), aws.ToString(attached.Items[0].LinkArn))

	upd, err := f.source.UpdateLink(t.Context(), &oamsdk.UpdateLinkInput{
		Identifier: link.Arn, ResourceTypes: toTypes([]string{"AWS::CloudWatch::Metric"}),
		LinkConfiguration: &oamtypes.LinkConfiguration{
			MetricConfiguration: &oamtypes.MetricConfiguration{Filter: aws.String("Namespace NOT LIKE 'AWS/%'")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"AWS::CloudWatch::Metric"}, upd.ResourceTypes)
	assert.Nil(t, upd.LinkConfiguration.LogGroupConfiguration)

	_, err = f.source.DeleteLink(t.Context(), &oamsdk.DeleteLinkInput{Identifier: link.Arn})
	require.NoError(t, err)

	_, err = f.source.GetLink(t.Context(), &oamsdk.GetLinkInput{Identifier: link.Arn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.monitoring.DeleteSink(t.Context(), &oamsdk.DeleteSinkInput{Identifier: sink.Arn})
	require.NoError(t, err)
}

func TestLinks_Validation(t *testing.T) {
	t.Parallel()

	metric := []string{"AWS::CloudWatch::Metric"}

	tests := []struct {
		input *oamsdk.CreateLinkInput
		name  string
		code  string
	}{
		{
			name: "unsupported_resource_type",
			input: &oamsdk.CreateLinkInput{
				LabelTemplate: aws.String("l"),
				ResourceTypes: toTypes([]string{"AWS::S3::Bucket"}),
			},
			code: "InvalidParameterException",
		},
		{
			name:  "unknown_label_variable",
			input: &oamsdk.CreateLinkInput{LabelTemplate: aws.String("$Nope"), ResourceTypes: toTypes(metric)},
			code:  "InvalidParameterException",
		},
		{
			name: "log_config_without_log_type",
			input: &oamsdk.CreateLinkInput{
				LabelTemplate: aws.String("l"), ResourceTypes: toTypes(metric),
				LinkConfiguration: &oamtypes.LinkConfiguration{
					LogGroupConfiguration: &oamtypes.LogGroupConfiguration{
						Filter: aws.String("LogGroupName LIKE 'a%'"),
					},
				},
			},
			code: "InvalidParameterException",
		},
		{
			name: "bad_metric_filter",
			input: &oamsdk.CreateLinkInput{
				LabelTemplate: aws.String("l"), ResourceTypes: toTypes(metric),
				LinkConfiguration: &oamtypes.LinkConfiguration{
					MetricConfiguration: &oamtypes.MetricConfiguration{Filter: aws.String("LogGroupName IN ('a')")},
				},
			},
			code: "InvalidParameterException",
		},
		{
			name: "unterminated_filter",
			input: &oamsdk.CreateLinkInput{
				LabelTemplate: aws.String("l"), ResourceTypes: toTypes(metric),
				LinkConfiguration: &oamtypes.LinkConfiguration{
					MetricConfiguration: &oamtypes.MetricConfiguration{Filter: aws.String("Namespace IN ('a'")},
				},
			},
			code: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			sink := createSink(t, f)
			allowAll(t, f, sink.Arn)

			tt.input.SinkIdentifier = sink.Arn

			_, err := f.source.CreateLink(t.Context(), tt.input)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}
}

func TestLinks_SinkPolicyEnforcement(t *testing.T) {
	t.Parallel()

	metric := []string{"AWS::CloudWatch::Metric"}
	both := []string{"AWS::CloudWatch::Metric", "AWS::Logs::LogGroup"}
	onlyMetric := `{"ForAllValues:StringEquals":{"oam:ResourceTypes":["AWS::CloudWatch::Metric"]}}`

	tests := []struct {
		directory fakeDirectory
		name      string
		policy    string
		types     []string
		allowed   bool
	}{
		{name: "no_policy", policy: "", types: metric, allowed: false},
		{name: "account_listed", policy: allowPolicy(sourceAccount, ""), types: metric, allowed: true},
		{name: "other_account_listed", policy: allowPolicy("999999999999", ""), types: metric, allowed: false},
		{
			name:    "root_arn_principal",
			policy:  allowPolicy("arn:aws:iam::"+sourceAccount+":root", ""),
			types:   metric,
			allowed: true,
		},
		{name: "wildcard_principal", policy: allowPolicy("*", ""), types: metric, allowed: true},
		{name: "type_condition_met", policy: allowPolicy(sourceAccount, onlyMetric), types: metric, allowed: true},
		{name: "type_condition_violated", policy: allowPolicy(sourceAccount, onlyMetric), types: both, allowed: false},
		{
			name:      "org_condition_met",
			policy:    allowPolicy("*", `{"StringEquals":{"aws:PrincipalOrgID":"o-abc123"}}`),
			types:     metric,
			directory: fakeDirectory{sourceAccount: {OrgID: "o-abc123"}},
			allowed:   true,
		},
		{
			name:      "org_condition_other_org",
			policy:    allowPolicy("*", `{"StringEquals":{"aws:PrincipalOrgID":"o-abc123"}}`),
			types:     metric,
			directory: fakeDirectory{sourceAccount: {OrgID: "o-zzz"}},
			allowed:   false,
		},
		{
			name:    "org_condition_no_directory",
			policy:  allowPolicy("*", `{"StringEquals":{"aws:PrincipalOrgID":"o-abc123"}}`),
			types:   metric,
			allowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			if tt.directory != nil {
				f.backend.SetAccountDirectory(tt.directory)
			}

			sink := createSink(t, f)

			if tt.policy != "" {
				_, err := f.monitoring.PutSinkPolicy(t.Context(), &oamsdk.PutSinkPolicyInput{
					SinkIdentifier: sink.Arn, Policy: aws.String(tt.policy),
				})
				require.NoError(t, err)
			}

			_, err := f.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
				LabelTemplate: aws.String("l"), ResourceTypes: toTypes(tt.types), SinkIdentifier: sink.Arn,
			})

			if tt.allowed {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, "InvalidParameterException", apiCode(err))
		})
	}
}

func TestLinks_UpdateRechecksPolicy(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sink := createSink(t, f)

	_, err := f.monitoring.PutSinkPolicy(t.Context(), &oamsdk.PutSinkPolicyInput{
		SinkIdentifier: sink.Arn,
		Policy: aws.String(allowPolicy(sourceAccount,
			`{"ForAllValues:StringEquals":{"oam:ResourceTypes":["AWS::CloudWatch::Metric"]}}`)),
	})
	require.NoError(t, err)

	link := createLink(t, f, sink.Arn, "l", []string{"AWS::CloudWatch::Metric"})

	_, err = f.source.UpdateLink(t.Context(), &oamsdk.UpdateLinkInput{
		Identifier: link.Arn, ResourceTypes: toTypes([]string{"AWS::CloudWatch::Metric", "AWS::XRay::Trace"}),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestLinks_Conflicts(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sink := createSink(t, f)
	allowAll(t, f, sink.Arn)
	createLink(t, f, sink.Arn, "one", []string{"AWS::CloudWatch::Metric"})

	_, err := f.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
		LabelTemplate: aws.String(
			"two",
		),
		ResourceTypes:  toTypes([]string{"AWS::CloudWatch::Metric"}),
		SinkIdentifier: sink.Arn,
	})
	assert.Equal(t, "ConflictException", apiCode(err))

	_, err = f.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
		LabelTemplate: aws.String("x"), ResourceTypes: toTypes([]string{"AWS::CloudWatch::Metric"}),
		SinkIdentifier: aws.String("arn:aws:oam:us-east-1:000000000000:sink/00000000-0000-0000-0000-000000000000"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.monitoring.ListAttachedLinks(
		t.Context(),
		&oamsdk.ListAttachedLinksInput{SinkIdentifier: aws.String("nope")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestLinks_LabelFromDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		template string
		want     string
	}{
		{"$AccountName", "Prod Account"},
		{"$AccountEmail", "ops@example.com"},
		{"$AccountEmailNoDomain", "ops"},
		{"$AccountName/$AccountEmailNoDomain", "Prod Account/ops"},
		{"plain", "plain"},
	}

	for _, tt := range tests {
		t.Run(tt.template, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetAccountDirectory(
				fakeDirectory{sourceAccount: {Name: "Prod Account", Email: "ops@example.com"}},
			)

			sink := createSink(t, f)
			allowAll(t, f, sink.Arn)

			link := createLink(t, f, sink.Arn, tt.template, []string{"AWS::CloudWatch::Metric"})
			assert.Equal(t, tt.want, aws.ToString(link.Label))
		})
	}
}

func TestTags(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sink := createSink(t, f)
	allowAll(t, f, sink.Arn)
	link := createLink(t, f, sink.Arn, "l", []string{"AWS::CloudWatch::Metric"})

	tests := []struct {
		client *oamsdk.Client
		arn    *string
		name   string
	}{
		{name: "sink", client: f.monitoring, arn: sink.Arn},
		{name: "link", client: f.source, arn: link.Arn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.client.TagResource(t.Context(), &oamsdk.TagResourceInput{
				ResourceArn: tt.arn, Tags: map[string]string{"k1": "v1", "k2": "v2"},
			})
			require.NoError(t, err)

			out, err := tt.client.ListTagsForResource(
				t.Context(),
				&oamsdk.ListTagsForResourceInput{ResourceArn: tt.arn},
			)
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, out.Tags)

			_, err = tt.client.UntagResource(t.Context(), &oamsdk.UntagResourceInput{
				ResourceArn: tt.arn, TagKeys: []string{"k1"},
			})
			require.NoError(t, err)

			out, err = tt.client.ListTagsForResource(t.Context(), &oamsdk.ListTagsForResourceInput{ResourceArn: tt.arn})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"k2": "v2"}, out.Tags)
		})
	}

	_, err := f.monitoring.ListTagsForResource(t.Context(), &oamsdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:oam:us-east-1:000000000000:sink/none"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.monitoring.ListTagsForResource(
		t.Context(),
		&oamsdk.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:s3:::b")},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.source.ListTagsForResource(t.Context(), &oamsdk.ListTagsForResourceInput{ResourceArn: sink.Arn})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	many := make(map[string]string, 51)
	for i := range 51 {
		many[string(rune('A'+i/26))+string(rune('a'+i%26))] = "v"
	}

	_, err = f.monitoring.TagResource(t.Context(), &oamsdk.TagResourceInput{ResourceArn: sink.Arn, Tags: many})
	assert.Equal(t, "TooManyTagsException", apiCode(err))
}

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sink := createSink(t, f)
	allowAll(t, f, sink.Arn)
	link := createLink(t, f, sink.Arn, "l", []string{"AWS::CloudWatch::Metric"})

	_, err := f.monitoring.TagResource(
		t.Context(),
		&oamsdk.TagResourceInput{ResourceArn: sink.Arn, Tags: map[string]string{"k": "v"}},
	)
	require.NoError(t, err)

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := oam.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	g := newFixtureOn(t, restored, serveBackend(t, restored))

	gotSink, err := g.monitoring.GetSink(
		t.Context(),
		&oamsdk.GetSinkInput{Identifier: sink.Arn, IncludeTags: aws.Bool(true)},
	)
	require.NoError(t, err)
	assert.Equal(t, "v", gotSink.Tags["k"])

	pol, err := g.monitoring.GetSinkPolicy(t.Context(), &oamsdk.GetSinkPolicyInput{SinkIdentifier: sink.Arn})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(pol.Policy))

	gotLink, err := g.source.GetLink(t.Context(), &oamsdk.GetLinkInput{Identifier: link.Arn})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(link.Label), aws.ToString(gotLink.Label))

	_, err = g.source.CreateLink(t.Context(), &oamsdk.CreateLinkInput{
		LabelTemplate: aws.String(
			"dup",
		),
		ResourceTypes:  toTypes([]string{"AWS::CloudWatch::Metric"}),
		SinkIdentifier: sink.Arn,
	})
	assert.Equal(t, "ConflictException", apiCode(err))
}
