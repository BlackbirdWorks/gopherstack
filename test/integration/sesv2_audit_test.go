package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSESv2AuditClient returns an SES v2 client pointed at the shared test
// container. Named uniquely so it does not collide with any future shared
// helper in main_test.go.
func createSESv2AuditClient(t *testing.T) *sesv2.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return sesv2.NewFromConfig(cfg, func(o *sesv2.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_SESv2Audit_ConfigurationSetArchivingOptions verifies that
// PutConfigurationSetArchivingOptions stores the archive ARN and that
// GetConfigurationSet returns it.
func TestIntegration_SESv2Audit_ConfigurationSetArchivingOptions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	csName := "audit-archiving-" + uuid.NewString()[:8]
	archiveARN := "arn:aws:ses:us-east-1:123456789012:mailmanager-archive/" + uuid.NewString()[:8]

	_, err := client.CreateConfigurationSet(ctx, &sesv2.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String(csName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteConfigurationSet(cleanupCtx, &sesv2.DeleteConfigurationSetInput{
			ConfigurationSetName: aws.String(csName),
		})
	})

	_, err = client.PutConfigurationSetArchivingOptions(
		ctx,
		&sesv2.PutConfigurationSetArchivingOptionsInput{
			ConfigurationSetName: aws.String(csName),
			ArchiveArn:           aws.String(archiveARN),
		},
	)
	require.NoError(t, err)

	out, err := client.GetConfigurationSet(ctx, &sesv2.GetConfigurationSetInput{
		ConfigurationSetName: aws.String(csName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ArchivingOptions, "archiving options should be returned")
	assert.Equal(t, archiveARN, aws.ToString(out.ArchivingOptions.ArchiveArn))
}

// TestIntegration_SESv2Audit_ConfigurationSetVdmOptions verifies that
// PutConfigurationSetVdmOptions stores the VDM options and that
// GetConfigurationSet surfaces them.
func TestIntegration_SESv2Audit_ConfigurationSetVdmOptions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	csName := "audit-vdm-" + uuid.NewString()[:8]

	_, err := client.CreateConfigurationSet(ctx, &sesv2.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String(csName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteConfigurationSet(cleanupCtx, &sesv2.DeleteConfigurationSetInput{
			ConfigurationSetName: aws.String(csName),
		})
	})

	_, err = client.PutConfigurationSetVdmOptions(ctx, &sesv2.PutConfigurationSetVdmOptionsInput{
		ConfigurationSetName: aws.String(csName),
		VdmOptions: &types.VdmOptions{
			DashboardOptions: &types.DashboardOptions{
				EngagementMetrics: types.FeatureStatusEnabled,
			},
			GuardianOptions: &types.GuardianOptions{
				OptimizedSharedDelivery: types.FeatureStatusEnabled,
			},
		},
	})
	require.NoError(t, err)

	out, err := client.GetConfigurationSet(ctx, &sesv2.GetConfigurationSetInput{
		ConfigurationSetName: aws.String(csName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.VdmOptions, "VDM options should be returned")
	require.NotNil(t, out.VdmOptions.DashboardOptions)
	assert.Equal(
		t,
		types.FeatureStatusEnabled,
		out.VdmOptions.DashboardOptions.EngagementMetrics,
	)
	require.NotNil(t, out.VdmOptions.GuardianOptions)
	assert.Equal(
		t,
		types.FeatureStatusEnabled,
		out.VdmOptions.GuardianOptions.OptimizedSharedDelivery,
	)
}

// TestIntegration_SESv2Audit_DedicatedIPInPool verifies that PutDedicatedIpInPool
// actually moves the dedicated IP into the requested pool and that GetDedicatedIp
// reflects the new pool assignment.
func TestIntegration_SESv2Audit_DedicatedIPInPool(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	poolName := "audit-pool-" + uuid.NewString()[:8]
	ip := "192.0.2.10"

	_, err := client.CreateDedicatedIpPool(ctx, &sesv2.CreateDedicatedIpPoolInput{
		PoolName: aws.String(poolName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDedicatedIpPool(cleanupCtx, &sesv2.DeleteDedicatedIpPoolInput{
			PoolName: aws.String(poolName),
		})
	})

	_, err = client.PutDedicatedIpInPool(ctx, &sesv2.PutDedicatedIpInPoolInput{
		Ip:                  aws.String(ip),
		DestinationPoolName: aws.String(poolName),
	})
	require.NoError(t, err)

	out, err := client.GetDedicatedIp(ctx, &sesv2.GetDedicatedIpInput{
		Ip: aws.String(ip),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DedicatedIp)
	assert.Equal(t, poolName, aws.ToString(out.DedicatedIp.PoolName))
	assert.Equal(t, ip, aws.ToString(out.DedicatedIp.Ip))
}

// TestIntegration_SESv2Audit_DedicatedIPWarmupAttributes verifies that
// PutDedicatedIpWarmupAttributes records the warmup percentage and that
// GetDedicatedIp reflects it (and derives an in-progress status).
func TestIntegration_SESv2Audit_DedicatedIPWarmupAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	ip := "192.0.2.20"

	const warmupPct int32 = 42

	_, err := client.PutDedicatedIpWarmupAttributes(
		ctx,
		&sesv2.PutDedicatedIpWarmupAttributesInput{
			Ip:               aws.String(ip),
			WarmupPercentage: aws.Int32(warmupPct),
		},
	)
	require.NoError(t, err)

	out, err := client.GetDedicatedIp(ctx, &sesv2.GetDedicatedIpInput{
		Ip: aws.String(ip),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DedicatedIp)
	assert.Equal(t, warmupPct, aws.ToInt32(out.DedicatedIp.WarmupPercentage))
	assert.Equal(t, types.WarmupStatusInProgress, out.DedicatedIp.WarmupStatus)
}

// TestIntegration_SESv2Audit_ReputationEntityCustomerManagedStatus verifies that
// UpdateReputationEntityCustomerManagedStatus stores the customer-managed sending
// status and that GetReputationEntity reflects it.
func TestIntegration_SESv2Audit_ReputationEntityCustomerManagedStatus(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	entityRef := "audit-entity-status-" + uuid.NewString()[:8]

	_, err := client.UpdateReputationEntityCustomerManagedStatus(
		ctx,
		&sesv2.UpdateReputationEntityCustomerManagedStatusInput{
			ReputationEntityReference: aws.String(entityRef),
			ReputationEntityType:      types.ReputationEntityTypeResource,
			SendingStatus:             types.SendingStatusDisabled,
		},
	)
	require.NoError(t, err)

	out, err := client.GetReputationEntity(ctx, &sesv2.GetReputationEntityInput{
		ReputationEntityReference: aws.String(entityRef),
		ReputationEntityType:      types.ReputationEntityTypeResource,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReputationEntity)
	require.NotNil(
		t,
		out.ReputationEntity.CustomerManagedStatus,
		"customer-managed status should be stored",
	)
	assert.Equal(
		t,
		types.SendingStatusDisabled,
		out.ReputationEntity.CustomerManagedStatus.Status,
	)
}

// TestIntegration_SESv2Audit_ReputationEntityPolicy verifies that
// UpdateReputationEntityPolicy stores the reputation management policy and that
// GetReputationEntity reflects it.
func TestIntegration_SESv2Audit_ReputationEntityPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSESv2AuditClient(t)
	ctx := t.Context()

	entityRef := "audit-entity-policy-" + uuid.NewString()[:8]
	policyARN := "arn:aws:ses::aws:policy/ReputationDefault"

	_, err := client.UpdateReputationEntityPolicy(
		ctx,
		&sesv2.UpdateReputationEntityPolicyInput{
			ReputationEntityReference: aws.String(entityRef),
			ReputationEntityType:      types.ReputationEntityTypeResource,
			ReputationEntityPolicy:    aws.String(policyARN),
		},
	)
	require.NoError(t, err)

	out, err := client.GetReputationEntity(ctx, &sesv2.GetReputationEntityInput{
		ReputationEntityReference: aws.String(entityRef),
		ReputationEntityType:      types.ReputationEntityTypeResource,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReputationEntity)
	assert.Equal(
		t,
		policyARN,
		aws.ToString(out.ReputationEntity.ReputationManagementPolicy),
	)
}
