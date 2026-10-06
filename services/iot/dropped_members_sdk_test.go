package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

type iotDroppedEnv struct {
	backend *iot.InMemoryBackend
	client  *iotsdk.Client
}

func newIoTDroppedEnv(t *testing.T) iotDroppedEnv {
	t.Helper()

	backend := iot.NewInMemoryBackend()

	return iotDroppedEnv{backend: backend, client: newTestIoTClient(t, iot.NewHandler(backend, nil))}
}

const droppedThingARN = "arn:aws:iot:us-east-1:000000000000:thing/t1"

func TestDroppedMembers_JobsAndCommands(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e iotDroppedEnv)
		name string
	}{
		{
			name: "cancel job keeps comment, reason code and force flag",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId: aws.String("j1"), Targets: []string{droppedThingARN}, Document: aws.String(`{}`),
				})
				require.NoError(t, err)

				_, err = e.client.CancelJob(t.Context(), &iotsdk.CancelJobInput{
					JobId: aws.String("j1"), Comment: aws.String("why"), ReasonCode: aws.String("R1"), Force: true,
				})
				require.NoError(t, err)

				got, err := e.client.DescribeJob(t.Context(), &iotsdk.DescribeJobInput{JobId: aws.String("j1")})
				require.NoError(t, err)
				assert.Equal(t, "why", aws.ToString(got.Job.Comment))
				assert.Equal(t, "R1", aws.ToString(got.Job.ReasonCode))
				assert.True(t, aws.ToBool(got.Job.ForceCanceled))
			},
		},
		{
			name: "list jobs applies status, target selection and thing group filters",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
					ThingGroupName: aws.String("g1"),
				})
				require.NoError(t, err)

				group, err := e.client.DescribeThingGroup(t.Context(), &iotsdk.DescribeThingGroupInput{
					ThingGroupName: aws.String("g1"),
				})
				require.NoError(t, err)

				for id, in := range map[string]*iotsdk.CreateJobInput{
					"snap":  {TargetSelection: types.TargetSelectionSnapshot, Targets: []string{droppedThingARN}},
					"cont":  {TargetSelection: types.TargetSelectionContinuous, Targets: []string{droppedThingARN}},
					"group": {TargetSelection: types.TargetSelectionSnapshot, Targets: []string{aws.ToString(group.ThingGroupArn)}},
				} {
					in.JobId = aws.String(id)
					in.Document = aws.String(`{}`)
					_, err = e.client.CreateJob(t.Context(), in)
					require.NoError(t, err)
				}

				_, err = e.client.CancelJob(t.Context(), &iotsdk.CancelJobInput{JobId: aws.String("snap")})
				require.NoError(t, err)

				ids := func(in *iotsdk.ListJobsInput) []string {
					out, listErr := e.client.ListJobs(t.Context(), in)
					require.NoError(t, listErr)

					got := make([]string, 0, len(out.Jobs))
					for _, j := range out.Jobs {
						got = append(got, aws.ToString(j.JobId))
					}

					return got
				}

				assert.ElementsMatch(t, []string{"snap"}, ids(&iotsdk.ListJobsInput{Status: types.JobStatusCanceled}))
				assert.ElementsMatch(
					t,
					[]string{"cont"},
					ids(&iotsdk.ListJobsInput{TargetSelection: types.TargetSelectionContinuous}),
				)
				assert.ElementsMatch(t, []string{"group"}, ids(&iotsdk.ListJobsInput{ThingGroupName: aws.String("g1")}))
				assert.ElementsMatch(t, []string{"group"}, ids(&iotsdk.ListJobsInput{ThingGroupId: group.ThingGroupId}))
				assert.Empty(t, ids(&iotsdk.ListJobsInput{ThingGroupId: aws.String("nope")}))
				assert.Len(t, ids(&iotsdk.ListJobsInput{}), 3)
			},
		},
		{
			name: "dynamic command members round trip and the parameter name filter applies",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
					CommandId:       aws.String("dyn"),
					Namespace:       types.CommandNamespaceAWSIoT,
					PayloadTemplate: aws.String(`{"v":"${p}"}`),
					RoleArn:         aws.String("arn:aws:iam::000000000000:role/cmd"),
					Preprocessor: &types.CommandPreprocessor{
						AwsJsonSubstitution: &types.AwsJsonSubstitutionCommandPreprocessorConfig{
							OutputFormat: types.OutputFormatJson,
						},
					},
					MandatoryParameters: []types.CommandParameter{{Name: aws.String("p")}},
				})
				require.NoError(t, err)

				_, err = e.client.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
					CommandId: aws.String("plain"), Namespace: types.CommandNamespaceAWSIoT,
					MandatoryParameters: []types.CommandParameter{{Name: aws.String("other")}},
				})
				require.NoError(t, err)

				got, err := e.client.GetCommand(t.Context(), &iotsdk.GetCommandInput{CommandId: aws.String("dyn")})
				require.NoError(t, err)
				assert.JSONEq(t, `{"v":"${p}"}`, aws.ToString(got.PayloadTemplate))
				assert.Equal(t, "arn:aws:iam::000000000000:role/cmd", aws.ToString(got.RoleArn))
				require.NotNil(t, got.Preprocessor)
				assert.Equal(t, types.OutputFormatJson, got.Preprocessor.AwsJsonSubstitution.OutputFormat)

				listed, err := e.client.ListCommands(
					t.Context(),
					&iotsdk.ListCommandsInput{CommandParameterName: aws.String("p")},
				)
				require.NoError(t, err)
				require.Len(t, listed.Commands, 1)
				assert.Equal(t, "dyn", aws.ToString(listed.Commands[0].CommandId))
			},
		},
		{
			name: "ota update options echo and reach the job",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateOTAUpdate(t.Context(), &iotsdk.CreateOTAUpdateInput{
					OtaUpdateId:          aws.String("ota1"),
					RoleArn:              aws.String("arn:aws:iam::000000000000:role/ota"),
					Targets:              []string{droppedThingARN},
					Files:                []types.OTAUpdateFile{{FileName: aws.String("fw.bin")}},
					TargetSelection:      types.TargetSelectionContinuous,
					Protocols:            []types.Protocol{types.ProtocolHttp},
					AdditionalParameters: map[string]string{"k": "v"},
					AwsJobTimeoutConfig:  &types.AwsJobTimeoutConfig{InProgressTimeoutInMinutes: aws.Int64(30)},
					AwsJobPresignedUrlConfig: &types.AwsJobPresignedUrlConfig{
						ExpiresInSec: aws.Int64(1800),
					},
					AwsJobExecutionsRolloutConfig: &types.AwsJobExecutionsRolloutConfig{MaximumPerMinute: aws.Int32(5)},
					AwsJobAbortConfig: &types.AwsJobAbortConfig{AbortCriteriaList: []types.AwsJobAbortCriteria{{
						Action:                    types.AwsJobAbortCriteriaAbortActionCancel,
						FailureType:               types.AwsJobAbortCriteriaFailureTypeFailed,
						MinNumberOfExecutedThings: aws.Int32(3),
						ThresholdPercentage:       aws.Float64(50),
					}}},
				})
				require.NoError(t, err)

				got, err := e.client.GetOTAUpdate(
					t.Context(),
					&iotsdk.GetOTAUpdateInput{OtaUpdateId: aws.String("ota1")},
				)
				require.NoError(t, err)

				info := got.OtaUpdateInfo
				assert.Equal(t, types.TargetSelectionContinuous, info.TargetSelection)
				assert.Equal(t, []types.Protocol{types.ProtocolHttp}, info.Protocols)
				assert.Equal(t, map[string]string{"k": "v"}, info.AdditionalParameters)
				assert.EqualValues(t, 5, aws.ToInt32(info.AwsJobExecutionsRolloutConfig.MaximumPerMinute))
				assert.EqualValues(t, 1800, aws.ToInt64(info.AwsJobPresignedUrlConfig.ExpiresInSec))

				job, err := e.client.DescribeJob(t.Context(), &iotsdk.DescribeJobInput{JobId: info.AwsIotJobId})
				require.NoError(t, err)
				assert.Equal(t, types.TargetSelectionContinuous, job.Job.TargetSelection)
				assert.EqualValues(t, 30, aws.ToInt64(job.Job.TimeoutConfig.InProgressTimeoutInMinutes))
				assert.EqualValues(t, 5, aws.ToInt32(job.Job.JobExecutionsRolloutConfig.MaximumPerMinute))
				assert.EqualValues(t, 1800, aws.ToInt64(job.Job.PresignedUrlConfig.ExpiresInSec))
				require.Len(t, job.Job.AbortConfig.CriteriaList, 1)
				assert.EqualValues(t, 3, aws.ToInt32(job.Job.AbortConfig.CriteriaList[0].MinNumberOfExecutedThings))

				_, err = e.client.CreateOTAUpdate(t.Context(), &iotsdk.CreateOTAUpdateInput{
					OtaUpdateId: aws.String("ota-bad"), RoleArn: aws.String("arn:aws:iam::000000000000:role/ota"),
					Targets: []string{droppedThingARN}, Files: []types.OTAUpdateFile{{FileName: aws.String("fw.bin")}},
					TargetSelection: types.TargetSelection("SOMETIMES"),
				})
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newIoTDroppedEnv(t))
		})
	}
}

func TestDroppedMembers_ConfigAndTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e iotDroppedEnv)
		name string
	}{
		{
			name: "domain configuration sub-configs round trip, update and remove",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateDomainConfiguration(t.Context(), &iotsdk.CreateDomainConfigurationInput{
					DomainConfigurationName: aws.String("dc1"),
					AuthorizerConfig: &types.AuthorizerConfig{
						DefaultAuthorizerName: aws.String("auth"), AllowAuthorizerOverride: aws.Bool(true),
					},
					ClientCertificateConfig: &types.ClientCertificateConfig{
						ClientCertificateCallbackArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:cb"),
					},
					ServerCertificateConfig: &types.ServerCertificateConfig{
						EnableOCSPCheck: aws.Bool(true),
						OcspLambdaArn:   aws.String("arn:aws:lambda:us-east-1:000000000000:function:ocsp"),
					},
					TlsConfig: &types.TlsConfig{SecurityPolicy: aws.String("IoTSecurityPolicy_TLS13_1_2_2022_10")},
				})
				require.NoError(t, err)

				describe := &iotsdk.DescribeDomainConfigurationInput{DomainConfigurationName: aws.String("dc1")}
				got, err := e.client.DescribeDomainConfiguration(t.Context(), describe)
				require.NoError(t, err)
				assert.Equal(t, "auth", aws.ToString(got.AuthorizerConfig.DefaultAuthorizerName))
				assert.True(t, aws.ToBool(got.AuthorizerConfig.AllowAuthorizerOverride))
				assert.Contains(
					t,
					aws.ToString(got.ClientCertificateConfig.ClientCertificateCallbackArn),
					"function:cb",
				)
				assert.True(t, aws.ToBool(got.ServerCertificateConfig.EnableOCSPCheck))
				assert.Equal(t, "IoTSecurityPolicy_TLS13_1_2_2022_10", aws.ToString(got.TlsConfig.SecurityPolicy))

				_, err = e.client.UpdateDomainConfiguration(t.Context(), &iotsdk.UpdateDomainConfigurationInput{
					DomainConfigurationName: aws.String("dc1"),
					TlsConfig: &types.TlsConfig{
						SecurityPolicy: aws.String("IoTSecurityPolicy_TLS12_1_2_2022_10"),
					},
					RemoveAuthorizerConfig: true,
				})
				require.NoError(t, err)

				got, err = e.client.DescribeDomainConfiguration(t.Context(), describe)
				require.NoError(t, err)
				assert.Nil(t, got.AuthorizerConfig)
				assert.Equal(t, "IoTSecurityPolicy_TLS12_1_2_2022_10", aws.ToString(got.TlsConfig.SecurityPolicy))

				_, err = e.client.UpdateDomainConfiguration(t.Context(), &iotsdk.UpdateDomainConfigurationInput{
					DomainConfigurationName: aws.String("dc1"),
					RemoveAuthorizerConfig:  true,
					AuthorizerConfig:        &types.AuthorizerConfig{DefaultAuthorizerName: aws.String("x")},
				})
				require.Error(t, err)
			},
		},
		{
			name: "package and provider create tokens replay and conflict across resources",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				in := &iotsdk.CreatePackageInput{PackageName: aws.String("pkg"), ClientToken: aws.String("tok-1")}
				first, err := e.client.CreatePackage(t.Context(), in)
				require.NoError(t, err)

				again, err := e.client.CreatePackage(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(first.PackageArn), aws.ToString(again.PackageArn))

				_, err = e.client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{
					PackageName: aws.String("pkg-2"), ClientToken: aws.String("tok-1"),
				})
				var conflict *types.ConflictException
				require.ErrorAs(t, err, &conflict)

				_, err = e.client.CreatePackage(
					t.Context(),
					&iotsdk.CreatePackageInput{PackageName: aws.String("pkg"), ClientToken: aws.String("tok-other")},
				)
				require.ErrorAs(t, err, &conflict)

				vin := &iotsdk.CreatePackageVersionInput{
					PackageName: aws.String("pkg"), VersionName: aws.String("1.0"), ClientToken: aws.String("tok-v"),
				}
				_, err = e.client.CreatePackageVersion(t.Context(), vin)
				require.NoError(t, err)
				_, err = e.client.CreatePackageVersion(t.Context(), vin)
				require.NoError(t, err)

				pin := &iotsdk.CreateCertificateProviderInput{
					CertificateProviderName: aws.String("prov"),
					LambdaFunctionArn:       aws.String("arn:aws:lambda:us-east-1:000000000000:function:p"),
					AccountDefaultForOperations: []types.CertificateProviderOperation{
						types.CertificateProviderOperationCreateCertificateFromCsr,
					},
					ClientToken: aws.String("tok-p"),
				}
				p1, err := e.client.CreateCertificateProvider(t.Context(), pin)
				require.NoError(t, err)
				p2, err := e.client.CreateCertificateProvider(t.Context(), pin)
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(p1.CertificateProviderArn), aws.ToString(p2.CertificateProviderArn))

				_, err = e.client.DeletePackageVersion(
					t.Context(),
					&iotsdk.DeletePackageVersionInput{PackageName: aws.String("pkg"), VersionName: aws.String("1.0")},
				)
				require.NoError(t, err)

				_, err = e.client.CreatePackageVersion(t.Context(), &iotsdk.CreatePackageVersionInput{
					PackageName: aws.String("pkg"), VersionName: aws.String("2.0"), ClientToken: aws.String("tok-v"),
				})
				require.NoError(t, err, "a deleted version releases its token")
			},
		},
		{
			name: "delete account audit configuration can drop scheduled audits",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateScheduledAudit(t.Context(), &iotsdk.CreateScheduledAuditInput{
					ScheduledAuditName: aws.String("sa"), Frequency: types.AuditFrequencyDaily,
					TargetCheckNames: []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
				})
				require.NoError(t, err)

				_, err = e.client.DeleteAccountAuditConfiguration(
					t.Context(),
					&iotsdk.DeleteAccountAuditConfigurationInput{},
				)
				require.NoError(t, err)
				listed, err := e.client.ListScheduledAudits(t.Context(), &iotsdk.ListScheduledAuditsInput{})
				require.NoError(t, err)
				assert.Len(t, listed.ScheduledAudits, 1)

				_, err = e.client.DeleteAccountAuditConfiguration(
					t.Context(),
					&iotsdk.DeleteAccountAuditConfigurationInput{DeleteScheduledAudits: true},
				)
				require.NoError(t, err)
				listed, err = e.client.ListScheduledAudits(t.Context(), &iotsdk.ListScheduledAuditsInput{})
				require.NoError(t, err)
				assert.Empty(t, listed.ScheduledAudits)
			},
		},
		{
			name: "update authorizer applies token key name, public keys and caching",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateAuthorizer(t.Context(), &iotsdk.CreateAuthorizerInput{
					AuthorizerName: aws.String(
						"a1",
					), AuthorizerFunctionArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:f"),
					TokenKeyName: aws.String("tok"), TokenSigningPublicKeys: map[string]string{"k1": "pem1"},
				})
				require.NoError(t, err)

				_, err = e.client.UpdateAuthorizer(t.Context(), &iotsdk.UpdateAuthorizerInput{
					AuthorizerName: aws.String("a1"), TokenKeyName: aws.String("tok2"),
					TokenSigningPublicKeys: map[string]string{"k2": "pem2"}, EnableCachingForHttp: aws.Bool(true),
				})
				require.NoError(t, err)

				got, err := e.client.DescribeAuthorizer(
					t.Context(),
					&iotsdk.DescribeAuthorizerInput{AuthorizerName: aws.String("a1")},
				)
				require.NoError(t, err)
				assert.Equal(t, "tok2", aws.ToString(got.AuthorizerDescription.TokenKeyName))
				assert.Equal(t, map[string]string{"k2": "pem2"}, got.AuthorizerDescription.TokenSigningPublicKeys)
				assert.True(t, aws.ToBool(got.AuthorizerDescription.EnableCachingForHttp))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newIoTDroppedEnv(t))
		})
	}
}

func TestDroppedMembers_ListFiltersAndCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e iotDroppedEnv)
		name string
	}{
		{
			name: "list things filters by type and attribute",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				_, err := e.client.CreateThingType(
					t.Context(),
					&iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("sensor")},
				)
				require.NoError(t, err)
				_, err = e.client.CreateThingType(
					t.Context(),
					&iotsdk.CreateThingTypeInput{ThingTypeName: aws.String("lamp")},
				)
				require.NoError(t, err)

				for name, spec := range map[string]struct{ typ, attr string }{
					"a": {"sensor", "red-1"}, "b": {"sensor", "blue"}, "c": {"lamp", "red-2"},
				} {
					_, err = e.client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
						ThingName: aws.String(name), ThingTypeName: aws.String(spec.typ),
						AttributePayload: &types.AttributePayload{Attributes: map[string]string{"color": spec.attr}},
					})
					require.NoError(t, err)
				}

				names := func(in *iotsdk.ListThingsInput) []string {
					out, listErr := e.client.ListThings(t.Context(), in)
					require.NoError(t, listErr)

					got := make([]string, 0, len(out.Things))
					for _, th := range out.Things {
						got = append(got, aws.ToString(th.ThingName))
					}

					return got
				}

				assert.ElementsMatch(
					t,
					[]string{"a", "b"},
					names(&iotsdk.ListThingsInput{ThingTypeName: aws.String("sensor")}),
				)
				assert.ElementsMatch(t, []string{"b"}, names(&iotsdk.ListThingsInput{
					AttributeName: aws.String("color"), AttributeValue: aws.String("blue"),
				}))
				assert.ElementsMatch(t, []string{"a", "c"}, names(&iotsdk.ListThingsInput{
					AttributeName: aws.String(
						"color",
					), AttributeValue: aws.String("red"), UsePrefixAttributeValue: true,
				}))
				assert.ElementsMatch(
					t,
					[]string{"a", "b", "c"},
					names(&iotsdk.ListThingsInput{AttributeName: aws.String("color")}),
				)

				types1, err := e.client.ListThingTypes(
					t.Context(),
					&iotsdk.ListThingTypesInput{ThingTypeName: aws.String("lamp")},
				)
				require.NoError(t, err)
				require.Len(t, types1.ThingTypes, 1)
				assert.Equal(t, "lamp", aws.ToString(types1.ThingTypes[0].ThingTypeName))
			},
		},
		{
			name: "list topic rules filters by disabled state and topic",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				for name, topic := range map[string]string{"r1": "a/b", "r2": "c/d"} {
					_, err := e.client.CreateTopicRule(t.Context(), &iotsdk.CreateTopicRuleInput{
						RuleName: aws.String(name),
						TopicRulePayload: &types.TopicRulePayload{
							Sql: aws.String("SELECT * FROM '" + topic + "'"),
							Actions: []types.Action{{Republish: &types.RepublishAction{
								RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), Topic: aws.String("out/x"),
							}}},
						},
					})
					require.NoError(t, err)
				}

				_, err := e.client.DisableTopicRule(
					t.Context(),
					&iotsdk.DisableTopicRuleInput{RuleName: aws.String("r2")},
				)
				require.NoError(t, err)

				disabled, err := e.client.ListTopicRules(
					t.Context(),
					&iotsdk.ListTopicRulesInput{RuleDisabled: aws.Bool(true)},
				)
				require.NoError(t, err)
				require.Len(t, disabled.Rules, 1)
				assert.Equal(t, "r2", aws.ToString(disabled.Rules[0].RuleName))

				enabled, err := e.client.ListTopicRules(
					t.Context(),
					&iotsdk.ListTopicRulesInput{RuleDisabled: aws.Bool(false)},
				)
				require.NoError(t, err)
				require.Len(t, enabled.Rules, 1)
				assert.Equal(t, "r1", aws.ToString(enabled.Rules[0].RuleName))

				byTopic, err := e.client.ListTopicRules(
					t.Context(),
					&iotsdk.ListTopicRulesInput{Topic: aws.String("a/b")},
				)
				require.NoError(t, err)
				require.Len(t, byTopic.Rules, 1)
				assert.Equal(t, "r1", aws.ToString(byTopic.Rules[0].RuleName))
			},
		},
		{
			name: "list thing groups honours parent group and recursive",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				mk := func(name, parent string) {
					in := &iotsdk.CreateThingGroupInput{ThingGroupName: aws.String(name)}
					if parent != "" {
						in.ParentGroupName = aws.String(parent)
					}

					_, err := e.client.CreateThingGroup(t.Context(), in)
					require.NoError(t, err)
				}
				mk("root", "")
				mk("child", "root")
				mk("grandchild", "child")

				names := func(in *iotsdk.ListThingGroupsInput) []string {
					out, err := e.client.ListThingGroups(t.Context(), in)
					require.NoError(t, err)

					got := make([]string, 0, len(out.ThingGroups))
					for _, g := range out.ThingGroups {
						got = append(got, aws.ToString(g.GroupName))
					}

					return got
				}

				assert.ElementsMatch(
					t,
					[]string{"child"},
					names(&iotsdk.ListThingGroupsInput{ParentGroup: aws.String("root")}),
				)
				assert.ElementsMatch(t, []string{"child", "grandchild"}, names(&iotsdk.ListThingGroupsInput{
					ParentGroup: aws.String("root"), Recursive: aws.Bool(true),
				}))
				assert.Len(t, names(&iotsdk.ListThingGroupsInput{}), 3)
			},
		},
		{
			name: "certificate delete is blocked by attached policies unless forced",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				cert, err := e.client.CreateKeysAndCertificate(
					t.Context(),
					&iotsdk.CreateKeysAndCertificateInput{SetAsActive: false},
				)
				require.NoError(t, err)

				_, err = e.client.CreatePolicy(t.Context(), &iotsdk.CreatePolicyInput{
					PolicyName: aws.String(
						"pol",
					), PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				})
				require.NoError(t, err)
				_, err = e.client.AttachPolicy(t.Context(), &iotsdk.AttachPolicyInput{
					PolicyName: aws.String("pol"), Target: cert.CertificateArn,
				})
				require.NoError(t, err)

				_, err = e.client.DeleteCertificate(
					t.Context(),
					&iotsdk.DeleteCertificateInput{CertificateId: cert.CertificateId},
				)
				var conflict *types.DeleteConflictException
				require.ErrorAs(t, err, &conflict)

				_, err = e.client.DeleteCertificate(
					t.Context(),
					&iotsdk.DeleteCertificateInput{CertificateId: cert.CertificateId, ForceDelete: true},
				)
				require.NoError(t, err)

				principals, err := e.client.ListTargetsForPolicy(t.Context(), &iotsdk.ListTargetsForPolicyInput{
					PolicyName: aws.String("pol"),
				})
				require.NoError(t, err)
				assert.Empty(t, principals.Targets)
			},
		},
		{
			name: "register certificate and ca certificate honour activation and auto registration",
			run: func(t *testing.T, e iotDroppedEnv) {
				t.Helper()

				ca, err := e.client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
					CaCertificate: aws.String("ca-pem-1"), VerificationCertificate: aws.String("verify"),
					SetAsActive: true, AllowAutoRegistration: true,
				})
				require.NoError(t, err)

				got, err := e.client.DescribeCACertificate(
					t.Context(),
					&iotsdk.DescribeCACertificateInput{CertificateId: ca.CertificateId},
				)
				require.NoError(t, err)
				assert.Equal(t, types.CACertificateStatusActive, got.CertificateDescription.Status)
				assert.Equal(t, types.AutoRegistrationStatusEnable, got.CertificateDescription.AutoRegistrationStatus)

				plain, err := e.client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
					CaCertificate: aws.String("ca-pem-2"), VerificationCertificate: aws.String("verify"),
				})
				require.NoError(t, err)

				gotPlain, err := e.client.DescribeCACertificate(
					t.Context(),
					&iotsdk.DescribeCACertificateInput{CertificateId: plain.CertificateId},
				)
				require.NoError(t, err)
				assert.Equal(t, types.CACertificateStatusInactive, gotPlain.CertificateDescription.Status)
				assert.Equal(
					t,
					types.AutoRegistrationStatusDisable,
					gotPlain.CertificateDescription.AutoRegistrationStatus,
				)

				dev, err := e.client.RegisterCertificate(t.Context(), &iotsdk.RegisterCertificateInput{
					CertificatePem:   aws.String("dev-pem"),
					CaCertificatePem: aws.String("ca-pem-1"),
					SetAsActive:      aws.Bool(true), //nolint:staticcheck // deprecated member under test
				})
				require.NoError(t, err)

				desc, err := e.client.DescribeCertificate(t.Context(), &iotsdk.DescribeCertificateInput{
					CertificateId: dev.CertificateId,
				})
				require.NoError(t, err)
				assert.Equal(t, types.CertificateStatusActive, desc.CertificateDescription.Status)
				assert.Equal(
					t,
					aws.ToString(ca.CertificateId),
					aws.ToString(desc.CertificateDescription.CaCertificateId),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newIoTDroppedEnv(t))
		})
	}
}
