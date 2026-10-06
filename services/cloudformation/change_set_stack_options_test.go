package cloudformation_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const changeSetOptionsTemplate = `{"Parameters":{"P":{"Type":"String","Default":"d"}},` +
	`"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`

func TestChangeSet_StackOptionsRoundTripAndExecute(t *testing.T) {
	t.Parallel()

	const topic = "arn:aws:sns:us-east-1:123456789012:t"

	tests := []struct {
		name       string
		in         cfnsdk.CreateChangeSetInput
		wantTags   map[string]string
		wantNotify []string
		wantMon    int32
	}{
		{
			name: "all_options",
			in: cfnsdk.CreateChangeSetInput{
				Tags:             []cfntypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
				NotificationARNs: []string{topic},
				RollbackConfiguration: &cfntypes.RollbackConfiguration{
					MonitoringTimeInMinutes: aws.Int32(5),
					RollbackTriggers: []cfntypes.RollbackTrigger{
						{
							Arn:  aws.String("arn:aws:cloudwatch:us-east-1:123456789012:alarm:a"),
							Type: aws.String("AWS::CloudWatch::Alarm"),
						},
					},
				},
				Parameters: []cfntypes.Parameter{{ParameterKey: aws.String("P"), ParameterValue: aws.String("v")}},
			},
			wantTags:   map[string]string{"env": "dev"},
			wantNotify: []string{topic},
			wantMon:    5,
		},
		{name: "none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			in := tt.in
			in.StackName = aws.String("s")
			in.ChangeSetName = aws.String("cs")
			in.ChangeSetType = cfntypes.ChangeSetTypeCreate
			in.TemplateBody = aws.String(changeSetOptionsTemplate)

			_, err := client.CreateChangeSet(t.Context(), &in)
			require.NoError(t, err)

			desc, err := client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("s"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantNotify, desc.NotificationARNs)

			if tt.wantMon > 0 {
				require.NotNil(t, desc.RollbackConfiguration)
				assert.Equal(t, tt.wantMon, aws.ToInt32(desc.RollbackConfiguration.MonitoringTimeInMinutes))
				require.Len(t, desc.Parameters, 1)
				assert.Equal(t, "v", aws.ToString(desc.Parameters[0].ParameterValue))
			}

			_, err = client.ExecuteChangeSet(t.Context(), &cfnsdk.ExecuteChangeSetInput{
				StackName: aws.String("s"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)

			stacks, err := client.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String("s")})
			require.NoError(t, err)
			require.Len(t, stacks.Stacks, 1)

			gotTags := map[string]string{}
			for _, tg := range stacks.Stacks[0].Tags {
				gotTags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
			}

			wantTags := tt.wantTags
			if wantTags == nil {
				wantTags = map[string]string{}
			}

			assert.Equal(t, wantTags, gotTags)
			assert.ElementsMatch(t, tt.wantNotify, stacks.Stacks[0].NotificationARNs)
		})
	}
}

func TestGetTemplate_ChangeSetName(t *testing.T) {
	t.Parallel()

	const (
		stackTemplate = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`
		csTemplate    = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"},"R":{"Type":"AWS::SQS::Queue"}}}`
	)

	tests := []struct {
		stackArg *string
		name     string
		useARN   bool
		wantR    bool
		wantErr  bool
	}{
		{name: "by_name", stackArg: aws.String("s"), wantR: true},
		{name: "by_arn_without_stack", useARN: true, wantR: true},
		{name: "unknown_change_set", stackArg: aws.String("s"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			_, err := client.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("s"), TemplateBody: aws.String(stackTemplate),
			})
			require.NoError(t, err)

			created, err := client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
				StackName: aws.String("s"), ChangeSetName: aws.String("cs"), TemplateBody: aws.String(csTemplate),
			})
			require.NoError(t, err)

			csArg := aws.String("cs")
			if tt.useARN {
				csArg = created.Id
			}

			if tt.wantErr {
				csArg = aws.String("missing")
			}

			out, err := client.GetTemplate(
				t.Context(),
				&cfnsdk.GetTemplateInput{StackName: tt.stackArg, ChangeSetName: csArg},
			)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantR, strings.Contains(aws.ToString(out.TemplateBody), `"R"`))
		})
	}
}

func TestListTypes_TypeCategoryAndDeprecatedFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   cfnsdk.ListTypesInput
		want []string
	}{
		{
			name: "registered_category",
			in:   cfnsdk.ListTypesInput{Filters: &cfntypes.TypeFilters{Category: cfntypes.CategoryRegistered}},
			want: []string{"Acme::Reg::Res"},
		},
		{
			name: "activated_category",
			in:   cfnsdk.ListTypesInput{Filters: &cfntypes.TypeFilters{Category: cfntypes.CategoryActivated}},
			want: []string{"Acme::Act::Res"},
		},
		{
			name: "aws_types_category",
			in:   cfnsdk.ListTypesInput{Filters: &cfntypes.TypeFilters{Category: cfntypes.CategoryAwsTypes}},
			want: []string{},
		},
		{name: "type_resource", in: cfnsdk.ListTypesInput{Type: cfntypes.RegistryTypeResource},
			want: []string{"Acme::Act::Res", "Acme::Reg::Res"}},
		{name: "deprecated", in: cfnsdk.ListTypesInput{DeprecatedStatus: cfntypes.DeprecatedStatusDeprecated},
			want: []string{"Acme::Reg::Res2"}},
		{name: "live_default", in: cfnsdk.ListTypesInput{},
			want: []string{"Acme::Act::Res", "Acme::Reg::Res"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			for _, n := range []string{"Acme::Reg::Res", "Acme::Reg::Res2"} {
				_, err := client.RegisterType(ctx, &cfnsdk.RegisterTypeInput{
					TypeName: aws.String(n), SchemaHandlerPackage: aws.String("s3://b/s.zip"),
				})
				require.NoError(t, err)
			}

			_, err := client.ActivateType(ctx, &cfnsdk.ActivateTypeInput{
				TypeName:      aws.String("Acme::Act::Res"),
				PublicTypeArn: aws.String("arn:aws:cloudformation:us-east-1::type/resource/Acme-Act-Res"),
			})
			require.NoError(t, err)

			_, err = client.DeregisterType(ctx, &cfnsdk.DeregisterTypeInput{
				TypeName: aws.String("Acme::Reg::Res2"), Type: cfntypes.RegistryTypeResource,
			})
			require.NoError(t, err)

			out, err := client.ListTypes(ctx, &tt.in)
			require.NoError(t, err)

			got := make([]string, 0)
			for _, s := range out.TypeSummaries {
				got = append(got, aws.ToString(s.TypeName))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
