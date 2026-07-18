package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestPutRetentionConfiguration(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutRetentionConfiguration("default", 90)
	if err != nil {
		t.Fatalf("PutRetentionConfiguration: %v", err)
	}

	configs := b.DescribeRetentionConfigurations()
	if len(configs) != 1 || configs[0].Name != "default" || configs[0].RetentionPeriodInDays != 90 {
		t.Fatalf("DescribeRetentionConfigurations: %v", configs)
	}
}

func TestDeleteRetentionConfiguration(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutRetentionConfiguration("default", 90)
	_ = b.DeleteRetentionConfiguration("default")

	configs := b.DescribeRetentionConfigurations()
	if len(configs) != 0 {
		t.Fatalf("expected empty after delete, got %v", configs)
	}
}

func TestPutRetentionConfiguration_EmptyName(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.PutRetentionConfiguration("", 90)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}
