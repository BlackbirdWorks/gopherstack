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

// TestSlice30_IoT_RealClient drives iot's remaining typed-client-uncovered
// ops (gopherstack-n3zi slice 30) through the real aws-sdk-go-v2 client,
// asserting decoded response values: audit suppressions, account audit
// configuration, audit findings and related resources, audit/detect
// mitigation-action tasks, event configurations, v1/v2 logging, certificate
// providers, SBOM association, thing-group extras, thing registration
// tasks, tags, endpoint, effective policies, encryption configuration, and
// topic rule destination confirmation.
func TestSlice30_IoT_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testAuditSuppressionsRealClient, "audit_suppressions"},
		{testAccountAuditConfigRealClient, "account_audit_config"},
		{testAuditFindingsRealClient, "audit_findings"},
		{testAuditMitigationTasksRealClient, "audit_mitigation_tasks"},
		{testDetectMitigationCancelRealClient, "detect_mitigation_cancel"},
		{testEventConfigurationsRealClient, "event_configurations"},
		{testLoggingV1RealClient, "logging_v1"},
		{testLoggingV2RealClient, "logging_v2"},
		{testCertificateProvidersExtraRealClient, "certificate_providers_extra"},
		{testSbomRealClient, "sbom"},
		{testThingGroupExtrasRealClient, "thing_group_extras"},
		{testThingRegistrationTasksRealClient, "thing_registration_tasks"},
		{testMiscRealClient, "misc"},
		{testListMetricValuesRealClient, "list_metric_values"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newSlice30IoTClient(t *testing.T) *iotsdk.Client {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	return newTestIoTClient(t, h)
}

func newSlice30IoTClientAndBackend(t *testing.T) (*iotsdk.Client, *iot.InMemoryBackend) {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	h := iot.NewHandler(backend, nil)

	return newTestIoTClient(t, h), backend
}

// testAuditSuppressionsRealClient covers CreateAuditSuppression,
// DescribeAuditSuppression, UpdateAuditSuppression, ListAuditSuppressions,
// DeleteAuditSuppression.
func testAuditSuppressionsRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	resourceID := &types.ResourceIdentifier{DeviceCertificateId: aws.String("cert-slice30")}

	_, err := client.CreateAuditSuppression(t.Context(), &iotsdk.CreateAuditSuppressionInput{
		CheckName:            aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
		ClientRequestToken:   aws.String("token-1"),
		ResourceIdentifier:   resourceID,
		Description:          aws.String("slice30 suppression"),
		SuppressIndefinitely: aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeAuditSuppression(t.Context(), &iotsdk.DescribeAuditSuppressionInput{
		CheckName:          aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
		ResourceIdentifier: resourceID,
	})
	require.NoError(t, err)
	assert.Equal(t, "slice30 suppression", aws.ToString(desc.Description))
	require.NotNil(t, desc.ResourceIdentifier)
	assert.Equal(t, "cert-slice30", aws.ToString(desc.ResourceIdentifier.DeviceCertificateId))
	assert.True(t, aws.ToBool(desc.SuppressIndefinitely))

	_, err = client.UpdateAuditSuppression(t.Context(), &iotsdk.UpdateAuditSuppressionInput{
		CheckName:            aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
		ResourceIdentifier:   resourceID,
		Description:          aws.String("updated"),
		SuppressIndefinitely: aws.Bool(false),
	})
	require.NoError(t, err)

	listed, err := client.ListAuditSuppressions(t.Context(), &iotsdk.ListAuditSuppressionsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Suppressions, 1)
	assert.Equal(t, "updated", aws.ToString(listed.Suppressions[0].Description))

	_, err = client.DeleteAuditSuppression(t.Context(), &iotsdk.DeleteAuditSuppressionInput{
		CheckName:          aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
		ResourceIdentifier: resourceID,
	})
	require.NoError(t, err)

	_, err = client.DescribeAuditSuppression(t.Context(), &iotsdk.DescribeAuditSuppressionInput{
		CheckName:          aws.String("DEVICE_CERTIFICATE_EXPIRING_CHECK"),
		ResourceIdentifier: resourceID,
	})
	require.Error(t, err)
}

// testAccountAuditConfigRealClient covers DescribeAccountAuditConfiguration,
// UpdateAccountAuditConfiguration, DeleteAccountAuditConfiguration.
func testAccountAuditConfigRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.UpdateAccountAuditConfiguration(t.Context(), &iotsdk.UpdateAccountAuditConfigurationInput{
		RoleArn: aws.String("arn:aws:iam::000000000000:role/audit-role"),
		AuditCheckConfigurations: map[string]types.AuditCheckConfiguration{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": {Enabled: true},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeAccountAuditConfiguration(
		t.Context(), &iotsdk.DescribeAccountAuditConfigurationInput{},
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/audit-role", aws.ToString(desc.RoleArn))
	require.Contains(t, desc.AuditCheckConfigurations, "DEVICE_CERTIFICATE_EXPIRING_CHECK")
	assert.True(t, desc.AuditCheckConfigurations["DEVICE_CERTIFICATE_EXPIRING_CHECK"].Enabled)

	_, err = client.DeleteAccountAuditConfiguration(t.Context(), &iotsdk.DeleteAccountAuditConfigurationInput{})
	require.NoError(t, err)
}

// testAuditFindingsRealClient covers DescribeAuditFinding and
// ListRelatedResourcesForAuditFinding, seeding a finding via the test-only
// SeedAuditFinding hook (real AWS raises findings internally from an audit
// task run, with no public "create finding" API).
func testAuditFindingsRealClient(t *testing.T) {
	t.Helper()

	client, backend := newSlice30IoTClientAndBackend(t)

	seeded := backend.SeedAuditFinding(&iot.AuditFinding{
		CheckName: "DEVICE_CERTIFICATE_EXPIRING_CHECK",
		Severity:  "MEDIUM",
		RelatedResources: []map[string]any{
			{
				"resourceType":       "DEVICE_CERTIFICATE",
				"resourceIdentifier": map[string]any{"deviceCertificateId": "cert-1"},
			},
		},
	})
	require.NotEmpty(t, seeded.FindingID)

	desc, err := client.DescribeAuditFinding(t.Context(), &iotsdk.DescribeAuditFindingInput{
		FindingId: aws.String(seeded.FindingID),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Finding)
	assert.Equal(t, "DEVICE_CERTIFICATE_EXPIRING_CHECK", aws.ToString(desc.Finding.CheckName))
	assert.Equal(t, types.AuditFindingSeverityMedium, desc.Finding.Severity)

	related, err := client.ListRelatedResourcesForAuditFinding(
		t.Context(), &iotsdk.ListRelatedResourcesForAuditFindingInput{
			FindingId: aws.String(seeded.FindingID),
		},
	)
	require.NoError(t, err)
	require.Len(t, related.RelatedResources, 1)
	assert.Equal(t, types.ResourceTypeDeviceCertificate, related.RelatedResources[0].ResourceType)
}

// testAuditMitigationTasksRealClient covers CancelAuditMitigationActionsTask,
// DescribeAuditMitigationActionsTask, ListAuditMitigationActionsTasks.
func testAuditMitigationTasksRealClient(t *testing.T) {
	t.Helper()

	client, backend := newSlice30IoTClientAndBackend(t)

	finding := backend.SeedAuditFinding(&iot.AuditFinding{CheckName: "DEVICE_CERTIFICATE_EXPIRING_CHECK"})

	_, err := client.StartAuditMitigationActionsTask(t.Context(), &iotsdk.StartAuditMitigationActionsTaskInput{
		TaskId: aws.String("slice30-audit-mit-task"),
		Target: &types.AuditMitigationActionsTaskTarget{
			FindingIds: []string{finding.FindingID},
		},
		AuditCheckToActionsMapping: map[string][]string{
			"DEVICE_CERTIFICATE_EXPIRING_CHECK": {"update-ca-cert"},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeAuditMitigationActionsTask(
		t.Context(), &iotsdk.DescribeAuditMitigationActionsTaskInput{
			TaskId: aws.String("slice30-audit-mit-task"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.AuditMitigationActionsTaskStatusInProgress, desc.TaskStatus)
	require.NotNil(t, desc.Target)
	assert.Equal(t, []string{finding.FindingID}, desc.Target.FindingIds)

	listed, err := client.ListAuditMitigationActionsTasks(
		t.Context(), &iotsdk.ListAuditMitigationActionsTasksInput{
			StartTime: aws.Time(desc.StartTime.Add(-1)),
			EndTime:   aws.Time(desc.StartTime.Add(1)),
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.Tasks, 1)
	assert.Equal(t, "slice30-audit-mit-task", aws.ToString(listed.Tasks[0].TaskId))

	_, err = client.CancelAuditMitigationActionsTask(t.Context(), &iotsdk.CancelAuditMitigationActionsTaskInput{
		TaskId: aws.String("slice30-audit-mit-task"),
	})
	require.NoError(t, err)
}

// testDetectMitigationCancelRealClient covers CancelDetectMitigationActionsTask.
func testDetectMitigationCancelRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.StartDetectMitigationActionsTask(t.Context(), &iotsdk.StartDetectMitigationActionsTaskInput{
		TaskId: aws.String("slice30-detect-task"),
		Target: &types.DetectMitigationActionsTaskTarget{
			SecurityProfileName: aws.String("sp-slice30"),
		},
		Actions: []string{"update-ca-cert"},
	})
	require.NoError(t, err)

	_, err = client.CancelDetectMitigationActionsTask(t.Context(), &iotsdk.CancelDetectMitigationActionsTaskInput{
		TaskId: aws.String("slice30-detect-task"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeDetectMitigationActionsTask(
		t.Context(), &iotsdk.DescribeDetectMitigationActionsTaskInput{
			TaskId: aws.String("slice30-detect-task"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, desc.TaskSummary)
	assert.Equal(t, types.DetectMitigationActionsTaskStatusCanceled, desc.TaskSummary.TaskStatus)
}

// testEventConfigurationsRealClient covers DescribeEventConfigurations,
// UpdateEventConfigurations.
func testEventConfigurationsRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.UpdateEventConfigurations(t.Context(), &iotsdk.UpdateEventConfigurationsInput{
		EventConfigurations: map[string]types.Configuration{
			"THING": {Enabled: true},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeEventConfigurations(t.Context(), &iotsdk.DescribeEventConfigurationsInput{})
	require.NoError(t, err)
	require.Contains(t, desc.EventConfigurations, "THING")
	assert.True(t, desc.EventConfigurations["THING"].Enabled)
}

// testLoggingV1RealClient covers SetLoggingOptions, GetLoggingOptions.
func testLoggingV1RealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.SetLoggingOptions(t.Context(), &iotsdk.SetLoggingOptionsInput{
		LoggingOptionsPayload: &types.LoggingOptionsPayload{
			RoleArn:  aws.String("arn:aws:iam::000000000000:role/logging-role"),
			LogLevel: types.LogLevelInfo,
		},
	})
	require.NoError(t, err)

	got, err := client.GetLoggingOptions(t.Context(), &iotsdk.GetLoggingOptionsInput{})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/logging-role", aws.ToString(got.RoleArn))
	assert.Equal(t, types.LogLevelInfo, got.LogLevel)
}

// testLoggingV2RealClient covers SetV2LoggingOptions, GetV2LoggingOptions,
// SetV2LoggingLevel, ListV2LoggingLevels, DeleteV2LoggingLevel.
func testLoggingV2RealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.SetV2LoggingOptions(t.Context(), &iotsdk.SetV2LoggingOptionsInput{
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/v2-logging-role"),
		DefaultLogLevel: types.LogLevelWarn,
		DisableAllLogs:  false,
	})
	require.NoError(t, err)

	got, err := client.GetV2LoggingOptions(t.Context(), &iotsdk.GetV2LoggingOptionsInput{})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/v2-logging-role", aws.ToString(got.RoleArn))
	assert.Equal(t, types.LogLevelWarn, got.DefaultLogLevel)

	target := &types.LogTarget{
		TargetType: types.LogTargetTypeThingGroup,
		TargetName: aws.String("slice30-tg"),
	}

	_, err = client.SetV2LoggingLevel(t.Context(), &iotsdk.SetV2LoggingLevelInput{
		LogTarget: target,
		LogLevel:  types.LogLevelDebug,
	})
	require.NoError(t, err)

	listed, err := client.ListV2LoggingLevels(t.Context(), &iotsdk.ListV2LoggingLevelsInput{
		TargetType: types.LogTargetTypeThingGroup,
	})
	require.NoError(t, err)
	require.Len(t, listed.LogTargetConfigurations, 1)
	assert.Equal(t, types.LogLevelDebug, listed.LogTargetConfigurations[0].LogLevel)
	require.NotNil(t, listed.LogTargetConfigurations[0].LogTarget)
	assert.Equal(t, "slice30-tg", aws.ToString(listed.LogTargetConfigurations[0].LogTarget.TargetName))

	_, err = client.DeleteV2LoggingLevel(t.Context(), &iotsdk.DeleteV2LoggingLevelInput{
		TargetType: types.LogTargetTypeThingGroup,
		TargetName: aws.String("slice30-tg"),
	})
	require.NoError(t, err)

	listed, err = client.ListV2LoggingLevels(t.Context(), &iotsdk.ListV2LoggingLevelsInput{})
	require.NoError(t, err)
	assert.Empty(t, listed.LogTargetConfigurations)
}

// testCertificateProvidersExtraRealClient covers DescribeCertificateProvider,
// UpdateCertificateProvider, DeleteCertificateProvider (CreateCertificateProvider
// is already typed-covered).
func testCertificateProvidersExtraRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.CreateCertificateProvider(t.Context(), &iotsdk.CreateCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
		LambdaFunctionArn:       aws.String("arn:aws:lambda:us-east-1:000000000000:function:cp"),
		AccountDefaultForOperations: []types.CertificateProviderOperation{
			types.CertificateProviderOperationCreateCertificateFromCsr,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeCertificateProvider(t.Context(), &iotsdk.DescribeCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:cp", aws.ToString(desc.LambdaFunctionArn))
	require.Len(t, desc.AccountDefaultForOperations, 1)

	_, err = client.UpdateCertificateProvider(t.Context(), &iotsdk.UpdateCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
		LambdaFunctionArn:       aws.String("arn:aws:lambda:us-east-1:000000000000:function:cp2"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeCertificateProvider(t.Context(), &iotsdk.DescribeCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:cp2", aws.ToString(desc.LambdaFunctionArn))

	_, err = client.DeleteCertificateProvider(t.Context(), &iotsdk.DeleteCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
	})
	require.NoError(t, err)

	_, err = client.DescribeCertificateProvider(t.Context(), &iotsdk.DescribeCertificateProviderInput{
		CertificateProviderName: aws.String("slice30-cert-provider"),
	})
	require.Error(t, err)
}

// testSbomRealClient covers AssociateSbomWithPackageVersion,
// ListSbomValidationResults, DisassociateSbomFromPackageVersion.
func testSbomRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.CreatePackage(t.Context(), &iotsdk.CreatePackageInput{
		PackageName: aws.String("slice30-pkg"),
	})
	require.NoError(t, err)

	_, err = client.CreatePackageVersion(t.Context(), &iotsdk.CreatePackageVersionInput{
		PackageName: aws.String("slice30-pkg"),
		VersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	assoc, err := client.AssociateSbomWithPackageVersion(t.Context(), &iotsdk.AssociateSbomWithPackageVersionInput{
		PackageName: aws.String("slice30-pkg"),
		VersionName: aws.String("1.0.0"),
		Sbom: &types.Sbom{
			S3Location: &types.S3Location{
				Bucket: aws.String("slice30-bucket"),
				Key:    aws.String("sbom.json"),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.SbomValidationStatusSucceeded, assoc.SbomValidationStatus)

	results, err := client.ListSbomValidationResults(t.Context(), &iotsdk.ListSbomValidationResultsInput{
		PackageName: aws.String("slice30-pkg"),
		VersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)
	require.Len(t, results.ValidationResultSummaries, 1)
	assert.Equal(t, "sbom.json", aws.ToString(results.ValidationResultSummaries[0].FileName))
	assert.Equal(t, types.SbomValidationResultSucceeded, results.ValidationResultSummaries[0].ValidationResult)

	_, err = client.DisassociateSbomFromPackageVersion(
		t.Context(), &iotsdk.DisassociateSbomFromPackageVersionInput{
			PackageName: aws.String("slice30-pkg"),
			VersionName: aws.String("1.0.0"),
		},
	)
	require.NoError(t, err)

	results, err = client.ListSbomValidationResults(t.Context(), &iotsdk.ListSbomValidationResultsInput{
		PackageName: aws.String("slice30-pkg"),
		VersionName: aws.String("1.0.0"),
	})
	require.NoError(t, err)
	assert.Empty(t, results.ValidationResultSummaries)
}

// testThingGroupExtrasRealClient covers DescribeThingGroup,
// ListThingsInThingGroup.
func testThingGroupExtrasRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	_, err := client.CreateThingGroup(t.Context(), &iotsdk.CreateThingGroupInput{
		ThingGroupName: aws.String("slice30-tg"),
		ThingGroupProperties: &types.ThingGroupProperties{
			ThingGroupDescription: aws.String("slice30 group"),
		},
	})
	require.NoError(t, err)

	_, err = client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
		ThingName: aws.String("slice30-thing"),
	})
	require.NoError(t, err)

	_, err = client.AddThingToThingGroup(t.Context(), &iotsdk.AddThingToThingGroupInput{
		ThingGroupName: aws.String("slice30-tg"),
		ThingName:      aws.String("slice30-thing"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeThingGroup(t.Context(), &iotsdk.DescribeThingGroupInput{
		ThingGroupName: aws.String("slice30-tg"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice30-tg", aws.ToString(desc.ThingGroupName))
	require.NotNil(t, desc.ThingGroupProperties)
	assert.Equal(t, "slice30 group", aws.ToString(desc.ThingGroupProperties.ThingGroupDescription))

	things, err := client.ListThingsInThingGroup(t.Context(), &iotsdk.ListThingsInThingGroupInput{
		ThingGroupName: aws.String("slice30-tg"),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"slice30-thing"}, things.Things)
}

// testThingRegistrationTasksRealClient covers ListThingRegistrationTasks,
// ListThingRegistrationTaskReports, StopThingRegistrationTask
// (StartThingRegistrationTask is already typed-covered).
func testThingRegistrationTasksRealClient(t *testing.T) {
	t.Helper()

	client := newSlice30IoTClient(t)

	started, err := client.StartThingRegistrationTask(t.Context(), &iotsdk.StartThingRegistrationTaskInput{
		TemplateBody:    aws.String("{}"),
		InputFileBucket: aws.String("slice30-bucket"),
		InputFileKey:    aws.String("input.json"),
		RoleArn:         aws.String("arn:aws:iam::000000000000:role/reg-role"),
	})
	require.NoError(t, err)
	taskID := aws.ToString(started.TaskId)
	require.NotEmpty(t, taskID)

	listed, err := client.ListThingRegistrationTasks(t.Context(), &iotsdk.ListThingRegistrationTasksInput{})
	require.NoError(t, err)
	assert.Contains(t, listed.TaskIds, taskID)

	reports, err := client.ListThingRegistrationTaskReports(
		t.Context(), &iotsdk.ListThingRegistrationTaskReportsInput{
			TaskId:     aws.String(taskID),
			ReportType: types.ReportTypeResults,
		},
	)
	require.NoError(t, err)
	require.Len(t, reports.ResourceLinks, 1)
	assert.Equal(t, types.ReportTypeResults, reports.ReportType)

	_, err = client.StopThingRegistrationTask(t.Context(), &iotsdk.StopThingRegistrationTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err)

	desc, err := client.DescribeThingRegistrationTask(t.Context(), &iotsdk.DescribeThingRegistrationTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StatusCancelled, desc.Status)
}

// testMiscRealClient covers TagResource/UntagResource, DescribeEndpoint,
// GetEffectivePolicies, GetBehaviorModelTrainingSummaries,
// PutVerificationStateOnViolation, UpdateEncryptionConfiguration,
// ConfirmTopicRuleDestination.
func testMiscRealClient(t *testing.T) {
	t.Helper()

	client, backend := newSlice30IoTClientAndBackend(t)

	created, err := client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
		ThingName: aws.String("slice30-tag-thing"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(t.Context(), &iotsdk.TagResourceInput{
		ResourceArn: created.ThingArn,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	tagged, err := client.ListTagsForResource(t.Context(), &iotsdk.ListTagsForResourceInput{
		ResourceArn: created.ThingArn,
	})
	require.NoError(t, err)
	require.Len(t, tagged.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagged.Tags[0].Key))

	_, err = client.UntagResource(t.Context(), &iotsdk.UntagResourceInput{
		ResourceArn: created.ThingArn,
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	tagged, err = client.ListTagsForResource(t.Context(), &iotsdk.ListTagsForResourceInput{
		ResourceArn: created.ThingArn,
	})
	require.NoError(t, err)
	assert.Empty(t, tagged.Tags)

	endpoint, err := client.DescribeEndpoint(t.Context(), &iotsdk.DescribeEndpointInput{
		EndpointType: aws.String("iot:Data-ATS"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(endpoint.EndpointAddress))

	_, err = client.CreatePolicy(t.Context(), &iotsdk.CreatePolicyInput{
		PolicyName:     aws.String("slice30-eff-policy"),
		PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.AttachPolicy(t.Context(), &iotsdk.AttachPolicyInput{
		PolicyName: aws.String("slice30-eff-policy"),
		Target:     created.ThingArn,
	})
	require.NoError(t, err)

	effective, err := client.GetEffectivePolicies(t.Context(), &iotsdk.GetEffectivePoliciesInput{
		Principal: created.ThingArn,
		ThingName: aws.String("slice30-tag-thing"),
	})
	require.NoError(t, err)
	require.Len(t, effective.EffectivePolicies, 1)
	assert.Equal(t, "slice30-eff-policy", aws.ToString(effective.EffectivePolicies[0].PolicyName))

	backend.AddBehaviorModelTrainingSummaryInternal("", iot.BehaviorModelTrainingSummary{
		BehaviorName: "slice30-behavior",
		ModelStatus:  "TRAINED",
	})

	summaries, err := client.GetBehaviorModelTrainingSummaries(
		t.Context(), &iotsdk.GetBehaviorModelTrainingSummariesInput{},
	)
	require.NoError(t, err)
	require.Len(t, summaries.Summaries, 1)
	assert.Equal(t, "slice30-behavior", aws.ToString(summaries.Summaries[0].BehaviorName))

	_, err = backend.SeedActiveViolation(&iot.SeedActiveViolationInput{
		ViolationID:         "slice30-violation",
		ThingName:           "slice30-tag-thing",
		SecurityProfileName: "slice30-sp",
	})
	require.NoError(t, err)

	_, err = client.PutVerificationStateOnViolation(t.Context(), &iotsdk.PutVerificationStateOnViolationInput{
		ViolationId:                  aws.String("slice30-violation"),
		VerificationState:            types.VerificationStateBenignPositive,
		VerificationStateDescription: aws.String("confirmed benign"),
	})
	require.NoError(t, err)

	_, err = client.UpdateEncryptionConfiguration(t.Context(), &iotsdk.UpdateEncryptionConfigurationInput{
		EncryptionType: types.EncryptionTypeAwsOwnedKmsKey,
	})
	require.NoError(t, err)

	encCfg, err := client.DescribeEncryptionConfiguration(t.Context(), &iotsdk.DescribeEncryptionConfigurationInput{})
	require.NoError(t, err)
	assert.Equal(t, types.EncryptionTypeAwsOwnedKmsKey, encCfg.EncryptionType)

	dest, err := client.CreateTopicRuleDestination(t.Context(), &iotsdk.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &types.TopicRuleDestinationConfiguration{
			HttpUrlConfiguration: &types.HttpUrlDestinationConfiguration{
				ConfirmationUrl: aws.String("https://example.com/confirm"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, dest.TopicRuleDestination)
	destARN := aws.ToString(dest.TopicRuleDestination.Arn)
	require.NotEmpty(t, destARN)
	assert.Equal(t, types.TopicRuleDestinationStatusInProgress, dest.TopicRuleDestination.Status)

	token := backend.TopicRuleDestConfirmationToken(destARN)
	require.NotEmpty(t, token)

	_, err = client.ConfirmTopicRuleDestination(t.Context(), &iotsdk.ConfirmTopicRuleDestinationInput{
		ConfirmationToken: aws.String(token),
	})
	require.NoError(t, err)

	got, err := client.GetTopicRuleDestination(t.Context(), &iotsdk.GetTopicRuleDestinationInput{
		Arn: aws.String(destARN),
	})
	require.NoError(t, err)
	require.NotNil(t, got.TopicRuleDestination)
	assert.Equal(t, types.TopicRuleDestinationStatusEnabled, got.TopicRuleDestination.Status)
}

// testListMetricValuesRealClient covers ListMetricValues, seeding a
// datapoint via the test-only AddMetricValueInternal hook (real AWS has no
// public PutMetricValue control-plane operation; values are normally
// reported by the device SDK's Device Defender metrics agent).
func testListMetricValuesRealClient(t *testing.T) {
	t.Helper()

	client, backend := newSlice30IoTClientAndBackend(t)

	_, err := client.CreateThing(t.Context(), &iotsdk.CreateThingInput{
		ThingName: aws.String("slice30-metric-thing"),
	})
	require.NoError(t, err)

	backend.AddMetricValueInternal("slice30-metric-thing", "aws:num-listeners", iot.MetricDatapoint{
		Timestamp: 1700000000,
		Value:     &iot.MetricValueData{Count: aws.Int64(3)},
	})

	values, err := client.ListMetricValues(t.Context(), &iotsdk.ListMetricValuesInput{
		ThingName:  aws.String("slice30-metric-thing"),
		MetricName: aws.String("aws:num-listeners"),
		StartTime:  aws.Time(time.Unix(1699999999, 0)),
		EndTime:    aws.Time(time.Unix(1700000001, 0)),
	})
	require.NoError(t, err)
	require.Len(t, values.MetricDatumList, 1)
	require.NotNil(t, values.MetricDatumList[0].Value)
	assert.Equal(t, int64(3), aws.ToInt64(values.MetricDatumList[0].Value.Count))
}
