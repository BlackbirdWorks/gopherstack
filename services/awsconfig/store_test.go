package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestReset_ClearsNewMaps(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.TagResource("arn:test", []awsconfig.Tag{{Key: "k", Value: "v"}})
	_ = b.PutRetentionConfiguration("default", 90)
	_ = b.PutRemediationConfigurations([]awsconfig.RemediationConfiguration{{ConfigRuleName: "r1"}})
	_ = b.PutResourceConfig("AWS::S3::Bucket", "b1", "{}")

	b.Reset()

	if tags := b.ListTagsForResource("arn:test"); len(tags) != 0 {
		t.Fatal("tags not cleared by Reset")
	}

	if configs := b.DescribeRetentionConfigurations(); len(configs) != 0 {
		t.Fatal("retentionConfigs not cleared by Reset")
	}

	if rc := b.DescribeRemediationConfigurations(nil); len(rc) != 0 {
		t.Fatal("remediationConfigs not cleared by Reset")
	}

	if count := b.GetAggregateDiscoveredResourceCounts(); count != 0 {
		t.Fatalf("resourceConfigs not cleared by Reset, count=%d", count)
	}
}
