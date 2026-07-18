package awsconfig_test

import (
	"testing"

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

	err := b.PutRemediationExceptions("rule1", "AWS::S3::Bucket", "my-bucket")
	if err != nil {
		t.Fatalf("PutRemediationExceptions: %v", err)
	}

	exs := b.DescribeRemediationExceptions("rule1")
	if len(exs) != 1 || exs[0].ResourceID != "my-bucket" {
		t.Fatalf("DescribeRemediationExceptions: %v", exs)
	}
}

func TestDeleteRemediationExceptions(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutRemediationExceptions("rule1", "AWS::S3::Bucket", "bucket1")
	_ = b.PutRemediationExceptions("rule1", "AWS::S3::Bucket", "bucket2")
	_ = b.DeleteRemediationExceptions("rule1", "bucket1")

	exs := b.DescribeRemediationExceptions("rule1")
	if len(exs) != 1 || exs[0].ResourceID != "bucket2" {
		t.Fatalf("expected one exception, got %v", exs)
	}
}
