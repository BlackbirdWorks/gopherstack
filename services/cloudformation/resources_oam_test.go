package cloudformation_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	oamsdk "github.com/aws/aws-sdk-go-v2/service/oam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	oambackend "github.com/blackbirdworks/gopherstack/services/oam"
)

type oamStackEnv struct {
	cfn *cfnsdk.Client
	oam *oamsdk.Client
}

func newOAMStackEnv(t *testing.T) oamStackEnv {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	handler := oambackend.NewHandler(oambackend.NewInMemoryBackend("000000000000", "us-east-1"))
	backends.OAM = handler

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	url := sdktest.Serve(t, handler)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("000000000000", "test", "")),
	)
	require.NoError(t, err)

	return oamStackEnv{
		cfn: newTestClientForBackend(t, backend),
		oam: oamsdk.NewFromConfig(cfg, func(o *oamsdk.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func (e oamStackEnv) getSink(t *testing.T, arn string) (*oamsdk.GetSinkOutput, error) {
	t.Helper()

	return e.oam.GetSink(t.Context(), &oamsdk.GetSinkInput{Identifier: aws.String(arn), IncludeTags: aws.Bool(true)})
}

func (e oamStackEnv) getLink(t *testing.T, arn string) (*oamsdk.GetLinkOutput, error) {
	t.Helper()

	return e.oam.GetLink(t.Context(), &oamsdk.GetLinkInput{Identifier: aws.String(arn), IncludeTags: aws.Bool(true)})
}

func oamTemplate(t *testing.T, sinkName string, linkTypes []string, linkFilter, team string) string {
	t.Helper()

	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{map[string]any{
			"Effect": "Allow", "Principal": map[string]any{"AWS": "000000000000"},
			"Action": []string{"oam:CreateLink", "oam:UpdateLink"}, "Resource": "*",
			"Condition": map[string]any{"ForAllValues:StringEquals": map[string]any{
				"oam:ResourceTypes": []string{"AWS::CloudWatch::Metric", "AWS::Logs::LogGroup"},
			}},
		}},
	}

	doc, err := json.Marshal(map[string]any{
		"Resources": map[string]any{
			"Sink": map[string]any{"Type": "AWS::Oam::Sink", "Properties": map[string]any{
				"Name": sinkName, "Policy": policy, "Tags": map[string]string{"Team": team},
			}},
			"Link": map[string]any{"Type": "AWS::Oam::Link", "Properties": map[string]any{
				"SinkIdentifier": map[string]any{"Ref": "Sink"},
				"LabelTemplate":  "$AccountName",
				"ResourceTypes":  linkTypes,
				"LinkConfiguration": map[string]any{
					"MetricConfiguration": map[string]any{"Filter": linkFilter},
				},
				"Tags": map[string]string{"Team": team},
			}},
		},
		"Outputs": map[string]any{
			"SinkRef":  map[string]any{"Value": map[string]any{"Ref": "Sink"}},
			"SinkArn":  map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Sink", "Arn"}}},
			"LinkRef":  map[string]any{"Value": map[string]any{"Ref": "Link"}},
			"LinkArn":  map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Link", "Arn"}}},
			"LinkLabl": map[string]any{"Value": map[string]any{"Fn::GetAtt": []string{"Link", "Label"}}},
		},
	})
	require.NoError(t, err)

	return string(doc)
}

func updateOAMStack(t *testing.T, env oamStackEnv, name, template string) {
	t.Helper()

	_, err := env.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	desc, err := env.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)

	stack := desc.Stacks[0]
	require.Equal(t, cfntypes.StackStatusUpdateComplete, stack.StackStatus, aws.ToString(stack.StackStatusReason))
}

func TestCreateStack_OAMSinkAndLink(t *testing.T) {
	t.Parallel()

	both := []string{"AWS::CloudWatch::Metric", "AWS::Logs::LogGroup"}
	ec2Filter := "Namespace IN ('AWS/EC2')"
	s3Filter := "Namespace IN ('AWS/S3')"

	tests := []struct {
		run  func(t *testing.T, env oamStackEnv)
		name string
	}{
		{name: "create ref and getatt", run: func(t *testing.T, env oamStackEnv) {
			t.Helper()

			out := createStackAndGetOutputs(t, env.cfn, "oam-create", oamTemplate(t, "sink-a", both, ec2Filter, "emu"))
			assert.Equal(t, out["SinkRef"], out["SinkArn"])
			assert.Equal(t, out["LinkRef"], out["LinkArn"])
			assert.NotEmpty(t, out["LinkLabl"])

			sink, err := env.getSink(t, out["SinkArn"])
			require.NoError(t, err)
			assert.Equal(t, "sink-a", aws.ToString(sink.Name))
			assert.Equal(t, map[string]string{"Team": "emu"}, sink.Tags)

			link, err := env.getLink(t, out["LinkArn"])
			require.NoError(t, err)
			assert.Equal(t, out["SinkArn"], aws.ToString(link.SinkArn))
			assert.Equal(t, out["LinkLabl"], aws.ToString(link.Label))
			assert.ElementsMatch(t, both, link.ResourceTypes)
			assert.Equal(t, ec2Filter, aws.ToString(link.LinkConfiguration.MetricConfiguration.Filter))
		}},
		{name: "update link and tags in place", run: func(t *testing.T, env oamStackEnv) {
			t.Helper()

			out := createStackAndGetOutputs(t, env.cfn, "oam-update", oamTemplate(t, "sink-u", both, ec2Filter, "emu"))

			updateOAMStack(t, env, "oam-update", oamTemplate(t, "sink-u", both[:1], s3Filter, "dev"))

			link, err := env.getLink(t, out["LinkArn"])
			require.NoError(t, err)
			assert.Equal(t, both[:1], link.ResourceTypes)
			assert.Equal(t, s3Filter, aws.ToString(link.LinkConfiguration.MetricConfiguration.Filter))
			assert.Equal(t, map[string]string{"Team": "dev"}, link.Tags)

			sink, err := env.getSink(t, out["SinkArn"])
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"Team": "dev"}, sink.Tags)
		}},
		{name: "delete stack", run: func(t *testing.T, env oamStackEnv) {
			t.Helper()

			createStackAndGetOutputs(t, env.cfn, "oam-delete", oamTemplate(t, "sink-d", both, ec2Filter, "emu"))

			_, err := env.cfn.DeleteStack(t.Context(), &cfnsdk.DeleteStackInput{StackName: aws.String("oam-delete")})
			require.NoError(t, err)

			sinks, err := env.oam.ListSinks(t.Context(), &oamsdk.ListSinksInput{})
			require.NoError(t, err)
			assert.Empty(t, sinks.Items)

			links, err := env.oam.ListLinks(t.Context(), &oamsdk.ListLinksInput{})
			require.NoError(t, err)
			assert.Empty(t, links.Items)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newOAMStackEnv(t))
		})
	}
}
