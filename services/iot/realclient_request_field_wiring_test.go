package iot_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// newIoTTestClientWithBackend is newIoTTestClient (realclient_core_resources_test.go)
// but also returns the backend, for tests that need to seed state no SDK
// call can produce (e.g. SeedAuditFinding, AddJobExecutionInternal).
func newIoTTestClientWithBackend(t *testing.T) (*iotsdk.Client, *iot.InMemoryBackend) {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	return newTestIoTClient(t, h), backend
}

// TestRealClient_JobForce drives CancelJob/CancelJobExecution/DeleteJob's
// Force fields (gopherstack-xhu2t) through the real SDK client. All three
// bind Force to an HTTP query parameter, not the body (confirmed against
// aws-sdk-go-v2/service/iot@v1.83.0's schemas.go) -- CancelJobExecution
// previously read it from the body instead, a different bug class than a
// plain drop.
func TestRealClient_JobForce(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "cancel_job_force_cascades_to_in_progress_executions",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)

				_, err := client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId:    aws.String("force-job"),
					Targets:  []string{"arn:aws:iot:us-east-1:000000000000:thing/t1"},
					Document: aws.String(`{}`),
				})
				require.NoError(t, err)

				backend.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "force-job", ThingName: "queued-thing",
					Status: iot.JobExecQueued, VersionNumber: 1,
				})
				backend.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "force-job", ThingName: "inprogress-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})

				_, err = client.CancelJob(t.Context(), &iotsdk.CancelJobInput{
					JobId: aws.String("force-job"),
					Force: true,
				})
				require.NoError(t, err)

				job, err := client.DescribeJob(t.Context(), &iotsdk.DescribeJobInput{JobId: aws.String("force-job")})
				require.NoError(t, err)
				assert.Equal(t, types.JobStatusCanceled, job.Job.Status)

				queued, err := client.DescribeJobExecution(t.Context(), &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("force-job"), ThingName: aws.String("queued-thing"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.JobExecutionStatusCanceled, queued.Execution.Status)

				inProgress, err := client.DescribeJobExecution(t.Context(), &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("force-job"), ThingName: aws.String("inprogress-thing"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.JobExecutionStatusCanceled, inProgress.Execution.Status)
				assert.True(t, aws.ToBool(inProgress.Execution.ForceCanceled))
			},
		},
		{
			name: "cancel_job_without_force_leaves_in_progress_execution_alone",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)

				_, err := client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId:    aws.String("noforce-job"),
					Targets:  []string{"arn:aws:iot:us-east-1:000000000000:thing/t1"},
					Document: aws.String(`{}`),
				})
				require.NoError(t, err)

				backend.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "noforce-job", ThingName: "inprogress-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})

				_, err = client.CancelJob(t.Context(), &iotsdk.CancelJobInput{JobId: aws.String("noforce-job")})
				require.NoError(t, err)

				exec, err := client.DescribeJobExecution(t.Context(), &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("noforce-job"), ThingName: aws.String("inprogress-thing"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.JobExecutionStatusInProgress, exec.Execution.Status,
					"an IN_PROGRESS execution must survive an unforced CancelJob")
			},
		},
		{
			name: "cancel_job_execution_force_query_param_allows_canceling_in_progress",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)

				backend.AddJobExecutionInternal(&iot.JobExecution{
					JobID: "cje-job", ThingName: "cje-thing",
					Status: iot.JobExecInProgress, VersionNumber: 1,
				})

				_, err := client.CancelJobExecution(t.Context(), &iotsdk.CancelJobExecutionInput{
					JobId: aws.String("cje-job"), ThingName: aws.String("cje-thing"),
				})
				require.Error(t, err, "without force, canceling an IN_PROGRESS execution must fail")

				_, err = client.CancelJobExecution(t.Context(), &iotsdk.CancelJobExecutionInput{
					JobId: aws.String("cje-job"), ThingName: aws.String("cje-thing"),
					Force: true,
				})
				require.NoError(t, err)

				exec, err := client.DescribeJobExecution(t.Context(), &iotsdk.DescribeJobExecutionInput{
					JobId: aws.String("cje-job"), ThingName: aws.String("cje-thing"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.JobExecutionStatusCanceled, exec.Execution.Status)
				assert.True(t, aws.ToBool(exec.Execution.ForceCanceled))
			},
		},
		{
			name: "delete_job_force_query_param_allows_deleting_in_progress",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.CreateJob(t.Context(), &iotsdk.CreateJobInput{
					JobId:    aws.String("del-job"),
					Targets:  []string{"arn:aws:iot:us-east-1:000000000000:thing/t1"},
					Document: aws.String(`{}`),
				})
				require.NoError(t, err)

				_, err = client.DeleteJob(t.Context(), &iotsdk.DeleteJobInput{JobId: aws.String("del-job")})
				require.Error(t, err, "a freshly created (IN_PROGRESS) job must not delete without force")

				_, err = client.DeleteJob(t.Context(), &iotsdk.DeleteJobInput{
					JobId: aws.String("del-job"), Force: true,
				})
				require.NoError(t, err)

				_, err = client.DescribeJob(t.Context(), &iotsdk.DescribeJobInput{JobId: aws.String("del-job")})
				require.Error(t, err, "job must be gone after a forced delete")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_Provisioning drives CreateProvisioningTemplateVersion's
// setAsDefault (an HTTP query parameter, not a body field -- confirmed
// against schemas.go) and CreateDomainConfiguration/
// UpdateDomainConfiguration's applicationProtocol/authenticationType
// (previously entirely unmodeled) through the real SDK client.
func TestRealClient_Provisioning(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_provisioning_template_version_set_as_default_query_param",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.CreateProvisioningTemplate(t.Context(), &iotsdk.CreateProvisioningTemplateInput{
					TemplateName:        aws.String("rfd2-tmpl"),
					TemplateBody:        aws.String(`{"Parameters":{}}`),
					ProvisioningRoleArn: aws.String("arn:aws:iam::000000000000:role/provisioning"),
				})
				require.NoError(t, err)

				v, err := client.CreateProvisioningTemplateVersion(
					t.Context(), &iotsdk.CreateProvisioningTemplateVersionInput{
						TemplateName: aws.String("rfd2-tmpl"),
						TemplateBody: aws.String(`{"Parameters":{"v2":{}}}`),
						SetAsDefault: true,
					},
				)
				require.NoError(t, err)
				assert.True(t, v.IsDefaultVersion)

				desc, err := client.DescribeProvisioningTemplate(t.Context(), &iotsdk.DescribeProvisioningTemplateInput{
					TemplateName: aws.String("rfd2-tmpl"),
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToInt32(v.VersionId), aws.ToInt32(desc.DefaultVersionId))
			},
		},
		{
			name: "domain_configuration_protocol_and_auth_type_round_trip",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.CreateDomainConfiguration(t.Context(), &iotsdk.CreateDomainConfigurationInput{
					DomainConfigurationName: aws.String("rfd2-domain"),
					ApplicationProtocol:     types.ApplicationProtocolMqttWss,
					AuthenticationType:      types.AuthenticationTypeCustomAuth,
				})
				require.NoError(t, err)

				desc, err := client.DescribeDomainConfiguration(t.Context(), &iotsdk.DescribeDomainConfigurationInput{
					DomainConfigurationName: aws.String("rfd2-domain"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.ApplicationProtocolMqttWss, desc.ApplicationProtocol)
				assert.Equal(t, types.AuthenticationTypeCustomAuth, desc.AuthenticationType)

				_, err = client.UpdateDomainConfiguration(t.Context(), &iotsdk.UpdateDomainConfigurationInput{
					DomainConfigurationName: aws.String("rfd2-domain"),
					ApplicationProtocol:     types.ApplicationProtocolHttps,
					AuthenticationType:      types.AuthenticationTypeAwsX509,
				})
				require.NoError(t, err)

				desc2, err := client.DescribeDomainConfiguration(t.Context(), &iotsdk.DescribeDomainConfigurationInput{
					DomainConfigurationName: aws.String("rfd2-domain"),
				})
				require.NoError(t, err)
				assert.Equal(t, types.ApplicationProtocolHttps, desc2.ApplicationProtocol)
				assert.Equal(t, types.AuthenticationTypeAwsX509, desc2.AuthenticationType)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_CommandsFilterAndSort drives ListCommands'
// maxResults/namespace/sortOrder and ListCommandExecutions' sortOrder --
// all previously either unread (ListCommands had no pagination/filter/sort
// at all) or unread on this specific op (ListCommandExecutions' body-carried
// sortOrder).
func TestRealClient_CommandsFilterAndSort(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "list_commands_namespace_filter_and_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
					CommandId: aws.String("iot-cmd"),
					Namespace: types.CommandNamespaceAWSIoT,
				})
				require.NoError(t, err)

				_, err = client.CreateCommand(t.Context(), &iotsdk.CreateCommandInput{
					CommandId: aws.String("fleetwise-cmd"),
					Namespace: types.CommandNamespaceAWSIoTFleetWise,
				})
				require.NoError(t, err)

				out, err := client.ListCommands(t.Context(), &iotsdk.ListCommandsInput{
					Namespace: types.CommandNamespaceAWSIoT,
				})
				require.NoError(t, err)
				require.Len(t, out.Commands, 1)
				assert.Equal(t, "iot-cmd", aws.ToString(out.Commands[0].CommandId))

				limited, err := client.ListCommands(t.Context(), &iotsdk.ListCommandsInput{MaxResults: aws.Int32(1)})
				require.NoError(t, err)
				assert.Len(t, limited.Commands, 1)
				assert.NotEmpty(t, aws.ToString(limited.NextToken))
			},
		},
		{
			name: "list_commands_sort_order",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)

				// Explicit distinct CreationDate values (AddCommandInternal), not
				// two real CreateCommand calls: both would land in the same
				// wall-clock second (CreationDate has 1-second resolution), making
				// ascending/descending ties indistinguishable.
				backend.AddCommandInternal(iot.IoTCommand{CommandID: "cmd-a", CreationDate: 100})
				backend.AddCommandInternal(iot.IoTCommand{CommandID: "cmd-b", CreationDate: 200})

				ascending, err := client.ListCommands(t.Context(), &iotsdk.ListCommandsInput{
					SortOrder: types.SortOrderAscending,
				})
				require.NoError(t, err)
				require.Len(t, ascending.Commands, 2)
				assert.Equal(t, "cmd-a", aws.ToString(ascending.Commands[0].CommandId))

				descending, err := client.ListCommands(t.Context(), &iotsdk.ListCommandsInput{
					SortOrder: types.SortOrderDescending,
				})
				require.NoError(t, err)
				require.Len(t, descending.Commands, 2)
				assert.Equal(t, "cmd-b", aws.ToString(descending.Commands[0].CommandId))
			},
		},
		{
			name: "list_command_executions_sort_order",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)

				backend.AddCommandExecutionInternal("exec-cmd", "exec-a", iot.IoTCommandExecution{
					CommandARN: "arn:aws:iot:us-east-1:000000000000:command/exec-cmd",
					ThingARN:   "arn:aws:iot:us-east-1:000000000000:thing/t1",
					Status:     "SUCCEEDED", CreationDate: 100,
				})
				backend.AddCommandExecutionInternal("exec-cmd", "exec-b", iot.IoTCommandExecution{
					CommandARN: "arn:aws:iot:us-east-1:000000000000:command/exec-cmd",
					ThingARN:   "arn:aws:iot:us-east-1:000000000000:thing/t2",
					Status:     "SUCCEEDED", CreationDate: 200,
				})

				ascending, err := client.ListCommandExecutions(t.Context(), &iotsdk.ListCommandExecutionsInput{
					SortOrder: types.SortOrderAscending,
				})
				require.NoError(t, err)
				require.Len(t, ascending.CommandExecutions, 2)
				assert.Equal(t, "exec-a", aws.ToString(ascending.CommandExecutions[0].ExecutionId))

				descending, err := client.ListCommandExecutions(t.Context(), &iotsdk.ListCommandExecutionsInput{
					SortOrder: types.SortOrderDescending,
				})
				require.NoError(t, err)
				require.Len(t, descending.CommandExecutions, 2)
				assert.Equal(t, "exec-b", aws.ToString(descending.CommandExecutions[0].ExecutionId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_ListPagination drives the maxResults field on five List/Get
// ops that previously returned every item with no pagination at all
// (ListAuditTasks, ListMitigationActions, ListScheduledAudits,
// ListCustomMetrics) or decoded-but-never-applied it (ListAuditFindings).
func TestRealClient_ListPagination(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "list_audit_tasks_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.StartOnDemandAuditTask(t.Context(), &iotsdk.StartOnDemandAuditTaskInput{
					TargetCheckNames: []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
				})
				require.NoError(t, err)
				_, err = client.StartOnDemandAuditTask(t.Context(), &iotsdk.StartOnDemandAuditTaskInput{
					TargetCheckNames: []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
				})
				require.NoError(t, err)

				out, err := client.ListAuditTasks(t.Context(), &iotsdk.ListAuditTasksInput{
					StartTime:  aws.Time(mustTime(t, "2020-01-01")),
					EndTime:    aws.Time(mustTime(t, "2030-01-01")),
					MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				assert.Len(t, out.Tasks, 1)
				assert.NotEmpty(t, aws.ToString(out.NextToken))
			},
		},
		{
			name: "list_mitigation_actions_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)
				for _, name := range []string{"ma-1", "ma-2"} {
					_, err := client.CreateMitigationAction(t.Context(), &iotsdk.CreateMitigationActionInput{
						ActionName: aws.String(name),
						RoleArn:    aws.String("arn:aws:iam::000000000000:role/mit"),
						ActionParams: &types.MitigationActionParams{
							UpdateDeviceCertificateParams: &types.UpdateDeviceCertificateParams{
								Action: types.DeviceCertificateUpdateActionDeactivate,
							},
						},
					})
					require.NoError(t, err)
				}

				out, err := client.ListMitigationActions(t.Context(), &iotsdk.ListMitigationActionsInput{
					MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				assert.Len(t, out.ActionIdentifiers, 1)
				assert.NotEmpty(t, aws.ToString(out.NextToken))
			},
		},
		{
			name: "list_scheduled_audits_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)
				for _, name := range []string{"sa-1", "sa-2"} {
					_, err := client.CreateScheduledAudit(t.Context(), &iotsdk.CreateScheduledAuditInput{
						ScheduledAuditName: aws.String(name),
						Frequency:          types.AuditFrequencyWeekly,
						TargetCheckNames:   []string{"DEVICE_CERTIFICATE_EXPIRING_CHECK"},
					})
					require.NoError(t, err)
				}

				out, err := client.ListScheduledAudits(t.Context(), &iotsdk.ListScheduledAuditsInput{
					MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				assert.Len(t, out.ScheduledAudits, 1)
				assert.NotEmpty(t, aws.ToString(out.NextToken))
			},
		},
		{
			name: "list_custom_metrics_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)
				for _, name := range []string{"metric-1", "metric-2"} {
					_, err := client.CreateCustomMetric(t.Context(), &iotsdk.CreateCustomMetricInput{
						MetricName: aws.String(name),
						MetricType: types.CustomMetricTypeNumber,
					})
					require.NoError(t, err)
				}

				out, err := client.ListCustomMetrics(t.Context(), &iotsdk.ListCustomMetricsInput{
					MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				assert.Len(t, out.MetricNames, 1)
				assert.NotEmpty(t, aws.ToString(out.NextToken))
			},
		},
		{
			name: "list_audit_findings_max_results",
			run: func(t *testing.T) {
				t.Helper()

				client, backend := newIoTTestClientWithBackend(t)
				backend.SeedAuditFinding(&iot.AuditFinding{CheckName: "CHECK_A", Severity: "HIGH"})
				backend.SeedAuditFinding(&iot.AuditFinding{CheckName: "CHECK_B", Severity: "LOW"})

				out, err := client.ListAuditFindings(t.Context(), &iotsdk.ListAuditFindingsInput{
					MaxResults: aws.Int32(1),
				})
				require.NoError(t, err)
				assert.Len(t, out.Findings, 1)
				assert.NotEmpty(t, aws.ToString(out.NextToken))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_GetStatisticsIndexName drives GetStatistics' indexName
// (decoded but never read before this pass -- GetStatistics always
// aggregated the AWS_Things index regardless of what was requested) through
// the real SDK client, proving AWS_ThingGroups is now a genuinely distinct
// result set.
func TestRealClient_GetStatisticsIndexName(t *testing.T) {
	t.Parallel()

	client := newIoTTestClient(t)

	_, err := client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
		ThingName: aws.String("stats-thing"),
		AttributePayload: &types.AttributePayload{
			Attributes: map[string]string{"score": "10"},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
		ThingGroupName: aws.String("stats-group"),
		ThingGroupProperties: &types.ThingGroupProperties{
			AttributePayload: &types.AttributePayload{
				Attributes: map[string]string{"score": "20"},
			},
		},
	})
	require.NoError(t, err)

	thingsStats, err := client.GetStatistics(t.Context(), &iotsdk.GetStatisticsInput{
		QueryString:      aws.String(""),
		AggregationField: aws.String("score"),
	})
	require.NoError(t, err)
	require.NotNil(t, thingsStats.Statistics)
	assert.InDelta(t, 10, aws.ToFloat64(thingsStats.Statistics.Average), 0.001)

	groupStats, err := client.GetStatistics(t.Context(), &iotsdk.GetStatisticsInput{
		QueryString:      aws.String(""),
		IndexName:        aws.String("AWS_ThingGroups"),
		AggregationField: aws.String("score"),
	})
	require.NoError(t, err)
	require.NotNil(t, groupStats.Statistics)
	assert.InDelta(t, 20, aws.ToFloat64(groupStats.Statistics.Average), 0.001)
}

// TestRealClient_RegisterCACertificate drives RegisterCACertificate's
// certificateMode/verificationCertificate through the real SDK client,
// enforcing the documented rule verbatim (api_op_RegisterCACertificate.go:
// "If certificateMode is SNI_ONLY, the verificationCertificate field must
// be empty. If certificateMode is DEFAULT or not provided, the
// verificationCertificate field must not be empty").
func TestRealClient_RegisterCACertificate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "sni_only_with_verification_certificate_is_rejected",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
					CaCertificate:   aws.String("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----"),
					CertificateMode: types.CertificateModeSniOnly,
					VerificationCertificate: aws.String(
						"-----BEGIN CERTIFICATE-----\nverify\n-----END CERTIFICATE-----",
					),
				})
				require.Error(t, err)
			},
		},
		{
			name: "default_mode_without_verification_certificate_is_rejected",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				_, err := client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
					CaCertificate: aws.String("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----"),
				})
				require.Error(t, err)
			},
		},
		{
			name: "default_mode_with_verification_certificate_succeeds_and_describes_back",
			run: func(t *testing.T) {
				t.Helper()

				client := newIoTTestClient(t)

				registered, err := client.RegisterCACertificate(t.Context(), &iotsdk.RegisterCACertificateInput{
					CaCertificate: aws.String("-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----"),
					VerificationCertificate: aws.String(
						"-----BEGIN CERTIFICATE-----\nverify\n-----END CERTIFICATE-----",
					),
				})
				require.NoError(t, err)

				desc, err := client.DescribeCACertificate(t.Context(), &iotsdk.DescribeCACertificateInput{
					CertificateId: registered.CertificateId,
				})
				require.NoError(t, err)
				require.NotNil(t, desc.CertificateDescription)
				assert.Equal(t, types.CertificateModeDefault, desc.CertificateDescription.CertificateMode)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func mustTime(t *testing.T, layout string) time.Time {
	t.Helper()

	parsed, err := time.Parse("2006-01-02", layout)
	require.NoError(t, err)

	return parsed
}
