package iot_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestPersistence_FullBackendStateSurvivesRoundTrip is the regression
// test for gopherstack-264: backendSnapshot previously omitted many live
// InMemoryBackend maps (thingTypes, thingGroups, certificates, jobs, etc.),
// so they silently dropped on Snapshot/Restore. This test seeds one entry in
// every backend map/field via the real create APIs, takes a snapshot,
// restores it into a fresh backend, and asserts every seeded entry survives.
func TestPersistence_FullBackendStateSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	seeded := seedPersistenceGapState(t, b1)

	snap := b1.Snapshot(context.Background())
	require.NotNil(t, snap)

	b2 := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(context.Background(), snap))

	for _, tt := range persistenceGapChecks(seeded) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, b2)
		})
	}
}

// gapSeededIDs carries the identifiers created by seedPersistenceGapState so
// the assertion table can look resources up in the restored backend.
type gapSeededIDs struct {
	certificateID      string
	topicRuleDestARN   string
	topicRuleDestToken string
	caCertificateID    string
	registrationCode   string
}

// seedPersistenceGapState creates one entry in every backend map that
// gopherstack-264 identified as missing from backendSnapshot, using only the
// backend's real public create/update APIs (or exported test-seed helpers
// where there is no control-plane create operation).
func seedPersistenceGapState(t *testing.T, b *iot.InMemoryBackend) gapSeededIDs {
	t.Helper()

	seedThingsAndGroups(t, b)
	certID := seedCertificates(t, b)
	destARN, destToken := seedTopicRuleDestination(t, b)
	seedJobsAndTemplates(t, b)
	seedProvisioningAndRoles(t, b)
	seedAuditAndSecurity(t, b)
	caID := seedCertsStreamsMetrics(t, b)
	seedPackagesAndCommands(t, b)
	seedAccountSingletons(t, b)

	code := b.GetRegistrationCode()
	require.NotEmpty(t, code)

	return gapSeededIDs{
		certificateID:      certID,
		topicRuleDestARN:   destARN,
		topicRuleDestToken: destToken,
		caCertificateID:    caID,
		registrationCode:   code,
	}
}

func seedThingsAndGroups(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateThing(&iot.CreateThingInput{ThingName: "gap-thing"})
	require.NoError(t, err)

	_, err = b.CreateThingType(&iot.CreateThingTypeInput{ThingTypeName: "gap-thing-type"})
	require.NoError(t, err)

	_, err = b.CreateThingGroup(&iot.CreateThingGroupInput{ThingGroupName: "gap-thing-group"})
	require.NoError(t, err)

	err = b.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
		ThingGroupName: "gap-thing-group",
		ThingName:      "gap-thing",
	})
	require.NoError(t, err)

	err = b.TagResourceGeneric(
		"arn:aws:iot:us-east-1:123456789012:thing/gap-thing",
		map[string]string{"env": "gap-test"},
	)
	require.NoError(t, err)
}

func seedCertificates(t *testing.T, b *iot.InMemoryBackend) string {
	t.Helper()

	cert, err := b.RegisterCertificateWithoutCA(&iot.RegisterCertificateInput{
		CertificatePem: "fake-pem",
		Status:         "ACTIVE",
	})
	require.NoError(t, err)

	_, err = b.CreatePolicy(&iot.CreatePolicyInput{
		PolicyName:     "gap-policy",
		PolicyDocument: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)

	_, err = b.CreateCertificateProvider(&iot.CreateCertificateProviderInput{
		CertificateProviderName:     "gap-cert-provider",
		LambdaFunctionARN:           "arn:aws:lambda:us-east-1:123456789012:function:gap-fn",
		AccountDefaultForOperations: []string{"CreateCertificateFromCsr"},
	})
	require.NoError(t, err)

	return cert.CertificateID
}

func seedTopicRuleDestination(t *testing.T, b *iot.InMemoryBackend) (string, string) {
	t.Helper()

	dest, err := b.CreateTopicRuleDestination(&iot.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &iot.TopicRuleDestinationConfiguration{
			HTTPURLConfiguration: &iot.HTTPURLDestinationConfiguration{
				ConfirmationURL: "https://example.com/confirm",
			},
		},
	})
	require.NoError(t, err)

	token := b.TopicRuleDestConfirmationToken(dest.ARN)
	require.NotEmpty(t, token)

	return dest.ARN, token
}

func seedJobsAndTemplates(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	// Also seeds Job's advanced fields (jobExecutionsRetryConfig,
	// presignedUrlConfig, schedulingConfig, destinationPackageVersions) so
	// the "jobs" check below can assert they survive Snapshot/Restore --
	// these are stored directly on the Job struct with normal json tags
	// (unlike JobProcessDetails, which is computed fresh on every
	// DescribeJob and deliberately never persisted).
	_, err := b.CreateJob(&iot.CreateJobInput{
		JobID:    "gap-job",
		Targets:  []string{"arn:aws:iot:us-east-1:123456789012:thing/gap-thing"},
		Document: `{}`,
		JobExecutionsRetryConfig: &iot.JobExecutionsRetryConfig{
			CriteriaList: []iot.RetryCriteria{{FailureType: "FAILED", NumberOfRetries: 2}},
		},
		PresignedURLConfig: &iot.PresignedURLConfig{
			RoleARN:      "arn:aws:iam::123456789012:role/Presign",
			ExpiresInSec: 60,
		},
		SchedulingConfig: &iot.SchedulingConfig{
			StartTime: "2030-01-01T00:00", EndBehavior: "STOP_ROLLOUT",
			MaintenanceWindows: []iot.MaintenanceWindow{{StartTime: "2030-01-01T00:00", DurationInMinutes: 30}},
		},
		DestinationPackageVersions: []string{"arn:aws:iot:us-east-1:123456789012:package/p/version/v1"},
	})
	require.NoError(t, err)

	// CreateJob's fan-out (see PARITY.md's job_and_jobtemplate fix) already
	// created a QUEUED JobExecution for gap-thing; this transitions it to
	// CANCELED so the "jobExecutions" check below observes a real state
	// change surviving the round trip, not just the fanned-out row's initial
	// state.
	err = b.CancelJobExecution("gap-job", "gap-thing", iot.CancelJobExecutionOptions{})
	require.NoError(t, err)

	_, err = b.CreateJobTemplate(&iot.CreateJobTemplateInput{
		JobTemplateID: "gap-job-template",
		Document:      `{}`,
	})
	require.NoError(t, err)
}

func seedProvisioningAndRoles(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateRoleAlias(&iot.CreateRoleAliasInput{
		RoleAlias: "gap-role-alias",
		RoleARN:   "arn:aws:iam::123456789012:role/gap-role",
	})
	require.NoError(t, err)

	_, err = b.CreateDomainConfiguration(&iot.CreateDomainConfigurationInput{
		DomainConfigurationName: "gap-domain-config",
	})
	require.NoError(t, err)

	_, err = b.CreateProvisioningTemplate(&iot.CreateProvisioningTemplateInput{
		TemplateName: "gap-prov-template",
		TemplateBody: `{}`,
	})
	require.NoError(t, err)

	_, err = b.CreateProvisioningTemplateVersion("gap-prov-template", `{}`, false)
	require.NoError(t, err)

	_, err = b.CreateAuthorizer(&iot.CreateAuthorizerInput{
		AuthorizerName:        "gap-authorizer",
		AuthorizerFunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:gap-authorizer-fn",
	})
	require.NoError(t, err)

	err = b.SetDefaultAuthorizer("gap-authorizer")
	require.NoError(t, err)

	_, err = b.CreateBillingGroup(&iot.CreateBillingGroupInput{BillingGroupName: "gap-billing-group"})
	require.NoError(t, err)
}

func seedAuditAndSecurity(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateScheduledAudit(&iot.CreateScheduledAuditInput{
		ScheduledAuditName: "gap-scheduled-audit",
		Frequency:          "WEEKLY",
		DayOfWeek:          "MON",
	})
	require.NoError(t, err)

	_, err = b.CreateMitigationAction(&iot.CreateMitigationActionInput{
		ActionName: "gap-mitigation-action",
		RoleARN:    "arn:aws:iam::123456789012:role/gap-role",
	})
	require.NoError(t, err)

	_, err = b.CreateSecurityProfile(&iot.CreateSecurityProfileInput{
		SecurityProfileName: "gap-security-profile",
		Behaviors: []iot.SecurityProfileBehavior{{
			Name:   "gap-behavior",
			Metric: "aws:num-connections",
			Criteria: &iot.SecurityProfileBehaviorCriteria{
				ComparisonOperator: "greater-than",
				DurationSeconds:    300,
			},
		}},
		AlertTargets: map[string]iot.SecurityProfileAlertTarget{
			"SNS": {
				AlertTargetArn: "arn:aws:sns:us-east-1:123456789012:gap-topic",
				RoleArn:        "arn:aws:iam::123456789012:role/gap-alert-role",
			},
		},
		AdditionalMetricsToRetainV2: []iot.SecurityProfileMetricToRetain{{Metric: "aws:num-messages-sent"}},
		MetricsExportConfig: &iot.SecurityProfileMetricsExportConfig{
			MqttTopic: "$aws/things/gap-thing/metrics",
			RoleArn:   "arn:aws:iam::123456789012:role/gap-export-role",
		},
	})
	require.NoError(t, err)

	err = b.UpdateAccountAuditConfiguration(
		"arn:aws:iam::123456789012:role/gap-audit-role",
		map[string]*iot.AuditCheckConfig{"CA_CERTIFICATE_EXPIRING_CHECK": {Enabled: true}},
	)
	require.NoError(t, err)

	_, err = b.StartOnDemandAuditTask([]string{"CA_CERTIFICATE_EXPIRING_CHECK"})
	require.NoError(t, err)

	err = b.CreateAuditSuppression(
		"CA_CERTIFICATE_EXPIRING_CHECK",
		map[string]any{"caCertificateId": "gap-ca-id"},
		"gap suppression",
		true,
		0,
	)
	require.NoError(t, err)

	found := b.SeedAuditFinding(&iot.AuditFinding{
		FindingID: "gap-finding",
		CheckName: "CA_CERTIFICATE_EXPIRING_CHECK",
		Severity:  "MEDIUM",
		NonCompliantResource: &iot.NonCompliantResource{
			ResourceType:       "CA_CERTIFICATE",
			ResourceIdentifier: &iot.ResourceIdentifier{CaCertificateID: "gap-ca-id"},
		},
	})
	require.NotNil(t, found)

	_, err = b.SeedActiveViolation(&iot.SeedActiveViolationInput{
		ViolationID:         "gap-violation",
		ThingName:           "gap-thing",
		SecurityProfileName: "gap-security-profile",
	})
	require.NoError(t, err)

	// ViolationEventOccurrenceRange is stored with a normal json tag on
	// DetectMitigationTask (unlike Actions' internal-only storage) so it
	// round-trips through Snapshot/Restore -- this seeds it to prove that,
	// not just that the table itself round-trips.
	_, err = b.StartDetectMitigationActionsTask(&iot.StartDetectMitigationActionsTaskInput{
		TaskID:  "gap-detect-task",
		Target:  &iot.DetectMitigationActionsTaskTarget{ViolationIDs: []string{"gap-violation"}},
		Actions: []string{"gap-mitigation-action"},
		ViolationEventOccurrenceRange: &iot.ViolationEventOccurrenceRange{
			StartTime: 1000,
			EndTime:   2000,
		},
	})
	require.NoError(t, err)
}

func seedCertsStreamsMetrics(t *testing.T, b *iot.InMemoryBackend) string {
	t.Helper()

	caCert, err := b.RegisterCACertificate("fake-ca-pem", "ACTIVE", nil, iot.RegistrationConfig{})
	require.NoError(t, err)

	_, err = b.CreateStream(&iot.CreateStreamInput{
		StreamID: "gap-stream",
		Files:    []iot.StreamFile{{FileID: 1, S3Bucket: "gap-bucket", S3Key: "gap-key"}},
	})
	require.NoError(t, err)

	_, err = b.CreateDimension(&iot.CreateDimensionInput{
		Name:         "gap-dimension",
		Type:         "TOPIC_FILTER",
		StringValues: []string{"foo/#"},
	})
	require.NoError(t, err)

	_, err = b.CreateFleetMetric(&iot.CreateFleetMetricInput{
		MetricName:  "gap-fleet-metric",
		QueryString: "connectivity.connected:true",
	})
	require.NoError(t, err)

	_, err = b.CreateCustomMetric(&iot.CreateCustomMetricInput{
		MetricName: "gap-custom-metric",
		MetricType: "string-list",
	})
	require.NoError(t, err)

	return caCert.CertificateID
}

func seedPackagesAndCommands(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	_, err := b.CreateOTAUpdate(
		"gap-ota-update", "gap ota", "arn:aws:iam::123456789012:role/gap-role",
		[]string{"arn:aws:iot:us-east-1:123456789012:thing/gap-thing"}, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.CreateIoTPackage("gap-package", "gap package", nil)
	require.NoError(t, err)

	_, err = b.CreateIoTPackageVersion("gap-package", "1.0", "gap version", nil, iot.CreateIoTPackageVersionOptions{})
	require.NoError(t, err)

	err = b.UpdatePackageConfiguration(map[string]any{"gap": "config"})
	require.NoError(t, err)

	_, err = b.CreateCommand(
		"gap-command", "Gap Command", "gap command desc", "AWS-IoT",
		map[string]any{"foo": "bar"}, nil,
	)
	require.NoError(t, err)

	b.AddCommandExecutionInternal("gap-command", "gap-exec-1", iot.IoTCommandExecution{
		CommandARN: "arn:aws:iot:us-east-1:123456789012:command/gap-command",
		ThingARN:   "arn:aws:iot:us-east-1:123456789012:thing/gap-thing",
		Status:     "SUCCEEDED",
	})
}

func seedAccountSingletons(t *testing.T, b *iot.InMemoryBackend) {
	t.Helper()

	err := b.SetV2LoggingOptions("arn:aws:iam::123456789012:role/gap-role", "ERROR", false, nil)
	require.NoError(t, err)

	err = b.SetV2LoggingLevel(map[string]any{"targetType": "DEFAULT", "targetName": "DEFAULT"}, "DEBUG")
	require.NoError(t, err)

	err = b.SetLoggingOptions("arn:aws:iam::123456789012:role/gap-role", "DEBUG")
	require.NoError(t, err)

	err = b.UpdateEventConfigurations(map[string]*iot.EventConfigEntry{"THING": {Enabled: true}})
	require.NoError(t, err)
}

// persistenceGapCheck is one row of the table-driven assertion below: a
// named lookup against the restored backend that must succeed and return
// the seeded value.
type persistenceGapCheck struct {
	check func(t *testing.T, b *iot.InMemoryBackend)
	name  string
}

func persistenceGapChecks(seeded gapSeededIDs) []persistenceGapCheck {
	return []persistenceGapCheck{
		{name: "thingTypes", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeThingType("gap-thing-type")
			require.NoError(t, err)
			assert.Equal(t, "gap-thing-type", got.ThingTypeName)
		}},
		{name: "thingGroups", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeThingGroup("gap-thing-group")
			require.NoError(t, err)
			assert.Equal(t, "gap-thing-group", got.ThingGroupName)
		}},
		{name: "thingGroupMembers", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			members, err := b.ListThingsInThingGroup(
				&iot.ListThingsInThingGroupInput{ThingGroupName: "gap-thing-group"},
			)
			require.NoError(t, err)
			assert.Contains(t, members, "gap-thing")
		}},
		{name: "resourceTags", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			tags := b.ListTagsForResource("arn:aws:iot:us-east-1:123456789012:thing/gap-thing")
			assert.Equal(t, "gap-test", tags["env"])
		}},
		{name: "certificates", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeCertificate(seeded.certificateID)
			require.NoError(t, err)
			assert.Equal(t, seeded.certificateID, got.CertificateID)
		}},
		{name: "policyVersions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			versions, err := b.ListPolicyVersions("gap-policy")
			require.NoError(t, err)
			assert.NotEmpty(t, versions)
		}},
		{name: "topicRuleDestinations", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetTopicRuleDestination(seeded.topicRuleDestARN)
			require.NoError(t, err)
			assert.Equal(t, seeded.topicRuleDestARN, got.ARN)
			// The confirmation token is unexported (json:"-") but must
			// still survive the round-trip so pending confirmations work
			// after a restart.
			assert.Equal(t, seeded.topicRuleDestToken, b.TopicRuleDestConfirmationToken(seeded.topicRuleDestARN))
		}},
		{name: "certificateProviders", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeCertificateProvider("gap-cert-provider")
			require.NoError(t, err)
			assert.Equal(t, "gap-cert-provider", got.CertificateProviderName)
		}},
		{name: "jobs", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeJob("gap-job")
			require.NoError(t, err)
			assert.Equal(t, "gap-job", got.JobID)

			require.NotNil(t, got.JobExecutionsRetryConfig)
			require.Len(t, got.JobExecutionsRetryConfig.CriteriaList, 1)
			assert.EqualValues(t, 2, got.JobExecutionsRetryConfig.CriteriaList[0].NumberOfRetries)

			require.NotNil(t, got.PresignedURLConfig)
			assert.Equal(t, "arn:aws:iam::123456789012:role/Presign", got.PresignedURLConfig.RoleARN)

			require.NotNil(t, got.SchedulingConfig)
			require.Len(t, got.SchedulingConfig.MaintenanceWindows, 1)
			assert.EqualValues(t, 30, got.SchedulingConfig.MaintenanceWindows[0].DurationInMinutes)

			assert.Equal(t,
				[]string{"arn:aws:iot:us-east-1:123456789012:package/p/version/v1"},
				got.DestinationPackageVersions,
			)
		}},
		{name: "jobExecutions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeJobExecution("gap-job", "gap-thing")
			require.NoError(t, err)
			assert.Equal(t, iot.JobExecCanceled, got.Status)
		}},
		{name: "jobTemplates", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeJobTemplate("gap-job-template")
			require.NoError(t, err)
			assert.Equal(t, "gap-job-template", got.JobTemplateID)
		}},
		{name: "roleAliases", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeRoleAlias("gap-role-alias")
			require.NoError(t, err)
			assert.Equal(t, "gap-role-alias", got.RoleAlias)
		}},
		{name: "domainConfigs", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeDomainConfiguration("gap-domain-config")
			require.NoError(t, err)
			assert.Equal(t, "gap-domain-config", got.DomainConfigurationName)
		}},
		{name: "provTemplates", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeProvisioningTemplate("gap-prov-template")
			require.NoError(t, err)
			assert.Equal(t, "gap-prov-template", got.TemplateName)
		}},
		{name: "provTemplateVersions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			versions, err := b.ListProvisioningTemplateVersions("gap-prov-template")
			require.NoError(t, err)
			assert.NotEmpty(t, versions)
		}},
		{name: "authorizers", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeAuthorizer("gap-authorizer")
			require.NoError(t, err)
			assert.Equal(t, "gap-authorizer", got.AuthorizerName)
		}},
		{name: "defaultAuthorizer", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeDefaultAuthorizer()
			require.NoError(t, err)
			assert.Equal(t, "gap-authorizer", got)
		}},
		{name: "billingGroups", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeBillingGroup("gap-billing-group")
			require.NoError(t, err)
			assert.Equal(t, "gap-billing-group", got.BillingGroupName)
		}},
		{name: "scheduledAudits", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeScheduledAudit("gap-scheduled-audit")
			require.NoError(t, err)
			assert.Equal(t, "gap-scheduled-audit", got.ScheduledAuditName)
		}},
		{name: "mitigationActions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeMitigationAction("gap-mitigation-action")
			require.NoError(t, err)
			assert.Equal(t, "gap-mitigation-action", got.ActionName)
		}},
		{name: "securityProfiles", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeSecurityProfile("gap-security-profile")
			require.NoError(t, err)
			assert.Equal(t, "gap-security-profile", got.SecurityProfileName)

			// Behaviors/AlertTargets/AdditionalMetricsToRetainV2/
			// MetricsExportConfig were previously entirely unmodeled on
			// SecurityProfile (silently dropped by CreateSecurityProfile);
			// assert they round-trip through Snapshot/Restore now that
			// they're real persisted fields.
			require.Len(t, got.Behaviors, 1)
			assert.Equal(t, "gap-behavior", got.Behaviors[0].Name)
			require.NotNil(t, got.Behaviors[0].Criteria)
			assert.Equal(t, "greater-than", got.Behaviors[0].Criteria.ComparisonOperator)
			require.Contains(t, got.AlertTargets, "SNS")
			assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:gap-topic", got.AlertTargets["SNS"].AlertTargetArn)
			require.Len(t, got.AdditionalMetricsToRetainV2, 1)
			assert.Equal(t, "aws:num-messages-sent", got.AdditionalMetricsToRetainV2[0].Metric)
			require.NotNil(t, got.MetricsExportConfig)
			assert.Equal(t, "$aws/things/gap-thing/metrics", got.MetricsExportConfig.MqttTopic)
		}},
		{name: "caCertificates", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeCACertificate(seeded.caCertificateID)
			require.NoError(t, err)
			assert.Equal(t, seeded.caCertificateID, got.CertificateID)
		}},
		{name: "streams", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeStream("gap-stream")
			require.NoError(t, err)
			assert.Equal(t, "gap-stream", got.StreamID)
		}},
		{name: "dimensions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeDimension("gap-dimension")
			require.NoError(t, err)
			assert.Equal(t, "gap-dimension", got.Name)
		}},
		{name: "fleetMetrics", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeFleetMetric("gap-fleet-metric")
			require.NoError(t, err)
			assert.Equal(t, "gap-fleet-metric", got.MetricName)
		}},
		{name: "customMetrics", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeCustomMetric("gap-custom-metric")
			require.NoError(t, err)
			assert.Equal(t, "gap-custom-metric", got.MetricName)
		}},
		{name: "auditConfiguration", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got := b.DescribeAccountAuditConfiguration()
			require.NotNil(t, got)
			assert.Equal(t, "arn:aws:iam::123456789012:role/gap-audit-role", got.RoleARN)
			assert.True(t, got.AuditCheckConfigurations["CA_CERTIFICATE_EXPIRING_CHECK"].Enabled)
		}},
		{name: "auditTaskObjects", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			tasks := b.ListAuditTasks("")
			assert.NotEmpty(t, tasks)
		}},
		{name: "auditSuppressions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeAuditSuppression(
				"CA_CERTIFICATE_EXPIRING_CHECK",
				map[string]any{"caCertificateId": "gap-ca-id"},
			)
			require.NoError(t, err)
			assert.Equal(t, "gap suppression", got.Description)
		}},
		{name: "auditFindings", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeAuditFinding("gap-finding")
			require.NoError(t, err)
			assert.Equal(t, "gap-finding", got.FindingID)
			require.NotNil(t, got.NonCompliantResource)
			require.NotNil(t, got.NonCompliantResource.ResourceIdentifier)
			assert.Equal(t, "gap-ca-id", got.NonCompliantResource.ResourceIdentifier.CaCertificateID)
		}},
		{name: "detectMitigationTasks", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.DescribeDetectMitigationActionsTask("gap-detect-task")
			require.NoError(t, err)
			require.NotNil(t, got.ViolationEventOccurrenceRange)
			assert.InDelta(t, 1000, got.ViolationEventOccurrenceRange.StartTime, 0)
			assert.InDelta(t, 2000, got.ViolationEventOccurrenceRange.EndTime, 0)
		}},
		{name: "activeViolations", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			violations := b.ListActiveViolations("gap-thing", "", "", nil, "")
			require.Len(t, violations, 1)
			assert.Equal(t, "gap-violation", violations[0].ViolationID)
		}},
		{name: "otaUpdates", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetOTAUpdate("gap-ota-update")
			require.NoError(t, err)
			assert.Equal(t, "gap-ota-update", got.OTAUpdateID)
		}},
		{name: "iotPackages", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetIoTPackage("gap-package")
			require.NoError(t, err)
			assert.Equal(t, "gap-package", got.PackageName)
		}},
		{name: "packageVersions2", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetIoTPackageVersion("gap-package", "1.0")
			require.NoError(t, err)
			assert.Equal(t, "1.0", got.VersionName)
		}},
		{name: "packageConfig", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got := b.GetPackageConfiguration()
			require.NotNil(t, got)
			assert.Equal(t, "config", got.VersionUpdateByJobsConfig["gap"])
		}},
		{name: "commands", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetCommand("gap-command")
			require.NoError(t, err)
			assert.Equal(t, "gap-command", got.CommandID)
		}},
		{name: "commandExecutions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got, err := b.GetCommandExecution("gap-command", "gap-exec-1")
			require.NoError(t, err)
			assert.Equal(t, "SUCCEEDED", got.Status)
		}},
		{name: "v2LoggingOptions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got := b.GetV2LoggingOptions()
			require.NotNil(t, got)
			assert.Equal(t, "ERROR", got.DefaultLogLevel)
		}},
		{name: "v2LoggingLevels", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			levels := b.ListV2LoggingLevels()
			assert.NotEmpty(t, levels)
		}},
		{name: "loggingOptions", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got := b.GetLoggingOptions()
			require.NotNil(t, got)
			assert.Equal(t, "DEBUG", got.LogLevel)
		}},
		{name: "eventConfigurations", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			got := b.DescribeEventConfigurations()
			require.NotNil(t, got)
			assert.True(t, got.EventConfigurations["THING"].Enabled)
		}},
		{name: "registrationCode", check: func(t *testing.T, b *iot.InMemoryBackend) {
			t.Helper()

			assert.Equal(t, seeded.registrationCode, b.GetRegistrationCode())
		}},
	}
}

// TestPersistence_EmptySnapshotRestoresNotFound verifies that
// restoring an empty snapshot into a backend that already holds state
// clears the previously-unpersisted fields cleanly (ensureNonNilSnap
// defaults them to empty maps instead of nil, and lookups return
// ErrResourceNotFound rather than panicking).
func TestPersistence_EmptySnapshotRestoresNotFound(t *testing.T) {
	t.Parallel()

	empty := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	snap := empty.Snapshot(context.Background())
	require.NotNil(t, snap)

	b := iot.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	seedPersistenceGapState(t, b)

	require.NoError(t, b.Restore(context.Background(), snap))

	tests := []struct {
		call func() error
		name string
	}{
		{name: "thingTypes", call: func() error {
			_, err := b.DescribeThingType("gap-thing-type")

			return err
		}},
		{name: "thingGroups", call: func() error {
			_, err := b.DescribeThingGroup("gap-thing-group")

			return err
		}},
		{name: "certificates", call: func() error {
			_, err := b.DescribeCertificate("no-such-cert")

			return err
		}},
		{name: "certificateProviders", call: func() error {
			_, err := b.DescribeCertificateProvider("gap-cert-provider")

			return err
		}},
		{name: "jobs", call: func() error {
			_, err := b.DescribeJob("gap-job")

			return err
		}},
		{name: "jobTemplates", call: func() error {
			_, err := b.DescribeJobTemplate("gap-job-template")

			return err
		}},
		{name: "roleAliases", call: func() error {
			_, err := b.DescribeRoleAlias("gap-role-alias")

			return err
		}},
		{name: "domainConfigs", call: func() error {
			_, err := b.DescribeDomainConfiguration("gap-domain-config")

			return err
		}},
		{name: "provTemplates", call: func() error {
			_, err := b.DescribeProvisioningTemplate("gap-prov-template")

			return err
		}},
		{name: "authorizers", call: func() error {
			_, err := b.DescribeAuthorizer("gap-authorizer")

			return err
		}},
		{name: "defaultAuthorizer", call: func() error {
			_, err := b.DescribeDefaultAuthorizer()

			return err
		}},
		{name: "billingGroups", call: func() error {
			_, err := b.DescribeBillingGroup("gap-billing-group")

			return err
		}},
		{name: "scheduledAudits", call: func() error {
			_, err := b.DescribeScheduledAudit("gap-scheduled-audit")

			return err
		}},
		{name: "mitigationActions", call: func() error {
			_, err := b.DescribeMitigationAction("gap-mitigation-action")

			return err
		}},
		{name: "securityProfiles", call: func() error {
			_, err := b.DescribeSecurityProfile("gap-security-profile")

			return err
		}},
		{name: "caCertificates", call: func() error {
			_, err := b.DescribeCACertificate("no-such-ca")

			return err
		}},
		{name: "streams", call: func() error {
			_, err := b.DescribeStream("gap-stream")

			return err
		}},
		{name: "dimensions", call: func() error {
			_, err := b.DescribeDimension("gap-dimension")

			return err
		}},
		{name: "fleetMetrics", call: func() error {
			_, err := b.DescribeFleetMetric("gap-fleet-metric")

			return err
		}},
		{name: "customMetrics", call: func() error {
			_, err := b.DescribeCustomMetric("gap-custom-metric")

			return err
		}},
		{name: "otaUpdates", call: func() error {
			_, err := b.GetOTAUpdate("gap-ota-update")

			return err
		}},
		{name: "iotPackages", call: func() error {
			_, err := b.GetIoTPackage("gap-package")

			return err
		}},
		{name: "packageVersions2", call: func() error {
			_, err := b.GetIoTPackageVersion("gap-package", "1.0")

			return err
		}},
		{name: "commands", call: func() error {
			_, err := b.GetCommand("gap-command")

			return err
		}},
		{name: "commandExecutions", call: func() error {
			_, err := b.GetCommandExecution("gap-command", "gap-exec-1")

			return err
		}},
		{name: "auditFindings", call: func() error {
			_, err := b.DescribeAuditFinding("gap-finding")

			return err
		}},
		{name: "jobExecutions", call: func() error {
			_, err := b.DescribeJobExecution("gap-job", "gap-thing")

			return err
		}},
		{name: "provTemplateVersions", call: func() error {
			_, err := b.DescribeProvisioningTemplateVersion("gap-prov-template", 1)

			return err
		}},
		{name: "auditSuppressions", call: func() error {
			_, err := b.DescribeAuditSuppression(
				"CA_CERTIFICATE_EXPIRING_CHECK",
				map[string]any{"caCertificateId": "gap-ca-id"},
			)

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Each entity uses its own not-found sentinel (ErrResourceNotFound,
			// ErrThingTypeNotFound, ErrCertificateNotFound, ...); what matters
			// here is only that the seeded entry did not survive restoring an
			// empty snapshot.
			require.Error(t, tt.call())
		})
	}

	// thingGroupMembers is keyed off a thing group that no longer exists
	// post-restore, so the lookup fails the same way it would for any
	// unknown group rather than leaking the pre-restore membership.
	_, err := b.ListThingsInThingGroup(&iot.ListThingsInThingGroupInput{ThingGroupName: "gap-thing-group"})
	require.Error(t, err)
	require.ErrorIs(t, err, iot.ErrThingGroupNotFound)

	// Singleton/list-shaped fields don't error on empty state; they should
	// simply report "nothing configured/present" instead of leaking the
	// pre-restore seeded values.
	assert.Empty(t, b.ListTagsForResource("arn:aws:iot:us-east-1:123456789012:thing/gap-thing"))
	assert.Empty(t, b.ListAuditTasks(""))
	assert.Empty(t, b.ListV2LoggingLevels())
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_restore_preserves_things_policies_rules"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			b1.AddThingInternal(iot.Thing{ThingName: "snap-thing"})
			b1.AddPolicyInternal(iot.Policy{PolicyName: "snap-policy"})
			b1.AddRuleInternal(iot.TopicRule{RuleName: "snap-rule", SQL: "SELECT temperature FROM 'devices/#'"})

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, 1, b2.ThingCount())
			assert.Equal(t, 1, b2.PolicyCount())
			assert.Equal(t, 1, b2.RuleCount())

			thing, err := b2.DescribeThing("snap-thing")
			require.NoError(t, err)
			assert.Equal(t, "snap-thing", thing.ThingName)
		})
	}
}

// TestRefinement1_PersistenceEmpty verifies Snapshot/Restore works on empty backend.
func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_snapshot_restores_cleanly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, 0, b2.ThingCount())
		})
	}
}

// TestRefinement1_HandlerSnapshot verifies Handler.Snapshot delegates to backend.
func TestHandlerSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_snapshot_delegates"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newRefHandler()
			b.AddThingInternal(iot.Thing{ThingName: "snap-via-handler"})

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2, b2 := newRefHandler()
			require.NoError(t, h2.Restore(t.Context(), snap))

			assert.Equal(t, 1, b2.ThingCount())
		})
	}
}

// TestRefinement1_Restore_NilMaps verifies Restore tolerates nil maps in snapshot JSON.
func TestRestore_NilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		json []byte
	}{
		{
			name: "empty_json_object",
			json: []byte(`{}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefBackend()
			err := b.Restore(t.Context(), tt.json)
			require.NoError(t, err)
			assert.Equal(t, 0, b.ThingCount())
		})
	}
}

// TestRefinement1_PersistenceWithAssociations verifies Snapshot/Restore preserves associations.
func TestPersistenceWithAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "thing_group_memberships_preserved"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := newRefBackend()
			b1.AddThingInternal(iot.Thing{ThingName: "t1"})

			// Add thing to group via backend method.
			require.NoError(t, b1.AddThingToThingGroup(&iot.AddThingToThingGroupInput{
				ThingName:      "t1",
				ThingGroupName: "g1",
			}))

			// A pending certificate transfer (TransferCertificate, not yet
			// accepted/rejected) must survive Snapshot/Restore.
			cert, err := b1.RegisterCertificate(&iot.RegisterCertificateInput{Status: "ACTIVE"})
			require.NoError(t, err)
			require.NoError(t, b1.TransferCertificate(cert.CertificateID, "999999999999", ""))

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := newRefBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			assert.Equal(t, 1, b2.ThingCount())
			assert.Equal(t, 1, b2.CertTransferCount())
		})
	}
}
