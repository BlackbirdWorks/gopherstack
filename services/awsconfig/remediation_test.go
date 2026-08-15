package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestPutRemediationConfigurations(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	configs := []awsconfig.RemediationConfiguration{
		{ConfigRuleName: "rule1", TargetType: "SSM_DOCUMENT", TargetID: "AWS-RunShellScript"},
	}

	err := b.PutRemediationConfigurations(configs)
	if err != nil {
		t.Fatalf("PutRemediationConfigurations: %v", err)
	}

	out := b.DescribeRemediationConfigurations([]string{"rule1"})
	if len(out) != 1 || out[0].ConfigRuleName != "rule1" {
		t.Fatalf("DescribeRemediationConfigurations: %v", out)
	}
}

func TestDescribeRemediationConfigurations_All(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
		{ConfigRuleName: "r1"},
		{ConfigRuleName: "r2"},
	})

	out := b.DescribeRemediationConfigurations(nil)
	if len(out) != 2 {
		t.Fatalf("expected 2, got %d", len(out))
	}
}

func TestDeleteRemediationConfiguration(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{{ConfigRuleName: "r1"}})
	_ = b.DeleteRemediationConfiguration("r1")

	out := b.DescribeRemediationConfigurations([]string{"r1"})
	if len(out) != 0 {
		t.Fatalf("expected empty after delete, got %v", out)
	}
}

func TestPutRemediationExceptions(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutRemediationExceptions("rule1", []awsconfig.RemediationExceptionResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "my-bucket"},
	})
	if err != nil {
		t.Fatalf("PutRemediationExceptions: %v", err)
	}

	exs := b.DescribeRemediationExceptions("rule1")
	if len(exs) != 1 || exs[0].ResourceID != "my-bucket" {
		t.Fatalf("DescribeRemediationExceptions: %v", exs)
	}
}

func TestPutRemediationExceptions_MultipleKeys(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutRemediationExceptions("rule1", []awsconfig.RemediationExceptionResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket1"},
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket2"},
	})
	require.NoError(t, err)

	exs := b.DescribeRemediationExceptions("rule1")
	assert.Len(t, exs, 2)
}

func TestDeleteRemediationExceptions(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutRemediationExceptions("rule1", []awsconfig.RemediationExceptionResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket1"},
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket2"},
	})
	_ = b.DeleteRemediationExceptions("rule1", []awsconfig.RemediationExceptionResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket1"},
	})

	exs := b.DescribeRemediationExceptions("rule1")
	if len(exs) != 1 || exs[0].ResourceID != "bucket2" {
		t.Fatalf("expected one exception, got %v", exs)
	}
}

func TestAWSConfigBackend_StartRemediationExecution(t *testing.T) {
	t.Parallel()

	t.Run("no_remediation_configuration_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.StartRemediationExecution("no-such-rule", []awsconfig.ResourceKey{
			{ResourceType: "AWS::S3::Bucket", ResourceID: "b1"},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchRemediationConfiguration)
	})

	t.Run("empty_rule_name_is_validation_error", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		err := b.StartRemediationExecution("", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrValidation)
	})

	t.Run("records_execution_readable_via_describe", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
			{ConfigRuleName: "rule1", TargetType: "SSM_DOCUMENT", TargetID: "AWS-RunShellScript"},
		}))

		key := awsconfig.ResourceKey{ResourceType: "AWS::S3::Bucket", ResourceID: "b1"}
		require.NoError(t, b.StartRemediationExecution("rule1", []awsconfig.ResourceKey{key}))

		statuses, err := b.DescribeRemediationExecutionStatus("rule1", nil)
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		assert.Equal(t, "SUCCEEDED", statuses[0].State)
		assert.Equal(t, key, statuses[0].ResourceKey)
		require.Len(t, statuses[0].StepDetails, 1)
	})
}

func TestAWSConfigBackend_DeleteRemediationConfiguration_CascadesExecutions(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
		{ConfigRuleName: "rule1"},
	}))
	require.NoError(t, b.StartRemediationExecution("rule1", []awsconfig.ResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "b1"},
	}))

	require.NoError(t, b.DeleteRemediationConfiguration("rule1"))

	// Re-creating the configuration must not resurrect the deleted execution.
	require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
		{ConfigRuleName: "rule1"},
	}))

	statuses, err := b.DescribeRemediationExecutionStatus("rule1", nil)
	require.NoError(t, err)
	assert.Empty(t, statuses, "deleting the remediation configuration must cascade-delete its executions")
}

func TestAWSConfigBackend_DescribeRemediationExecutionStatus(t *testing.T) {
	t.Parallel()

	t.Run("no_remediation_configuration_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		_, err := b.DescribeRemediationExecutionStatus("no-such-rule", nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchRemediationConfiguration)
	})

	t.Run("filters_by_resource_key", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{
			{ConfigRuleName: "rule1"},
		}))
		keyA := awsconfig.ResourceKey{ResourceType: "AWS::S3::Bucket", ResourceID: "a"}
		keyB := awsconfig.ResourceKey{ResourceType: "AWS::S3::Bucket", ResourceID: "b"}
		require.NoError(t, b.StartRemediationExecution("rule1", []awsconfig.ResourceKey{keyA, keyB}))

		statuses, err := b.DescribeRemediationExecutionStatus("rule1", []awsconfig.ResourceKey{keyA})
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		assert.Equal(t, keyA, statuses[0].ResourceKey)
	})
}
