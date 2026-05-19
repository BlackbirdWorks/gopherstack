package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// --- Group 1: Tag operations ---

func TestTagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.TagResource("arn:aws:config::123:rule/r1", []awsconfig.Tag{{Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}

	tags := b.ListTagsForResource("arn:aws:config::123:rule/r1")
	if len(tags) != 1 || tags[0].Key != "env" || tags[0].Value != "prod" {
		t.Fatalf("ListTagsForResource: got %v, want [{env prod}]", tags)
	}
}

func TestTagResource_Update(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}})
	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "staging"}, {Key: "owner", Value: "team"}})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 2 {
		t.Fatalf("expected 2 tags, got %d: %v", len(tags), tags)
	}

	for _, tag := range tags {
		if tag.Key == "env" && tag.Value != "staging" {
			t.Errorf("env tag not updated: got %q", tag.Value)
		}
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	arn := "arn:aws:config::123:rule/r1"

	_ = b.TagResource(arn, []awsconfig.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "team"}})
	_ = b.UntagResource(arn, []string{"env"})

	tags := b.ListTagsForResource(arn)
	if len(tags) != 1 || tags[0].Key != "owner" {
		t.Fatalf("UntagResource: got %v, want [{owner team}]", tags)
	}
}

func TestListTagsForResource_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	tags := b.ListTagsForResource("arn:missing")
	if len(tags) != 0 {
		t.Fatalf("expected empty tags, got %v", tags)
	}
}

// --- Group 2: StoredQuery operations ---

func TestGetStoredQuery(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	_ = b.PutStoredQuery("my-query")

	q := b.GetStoredQuery("my-query")
	if q == nil || q.QueryName != "my-query" {
		t.Fatalf("GetStoredQuery: got %v", q)
	}
}

func TestGetStoredQuery_NotFound(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	q := b.GetStoredQuery("nonexistent")
	if q != nil {
		t.Fatalf("expected nil, got %v", q)
	}
}

func TestDeleteStoredQuery(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutStoredQuery("q1")
	_ = b.DeleteStoredQuery("q1")

	q := b.GetStoredQuery("q1")
	if q != nil {
		t.Fatal("expected nil after delete")
	}
}

// --- Group 3: RetentionConfiguration operations ---

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

// --- Group 4: RemediationConfiguration operations ---

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

// --- Group 5: ConfigRule evaluation operations ---

func TestDescribeConfigRuleEvaluationStatus(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "rule1"})
	_ = b.StartConfigRulesEvaluation()

	statuses := b.DescribeConfigRuleEvaluationStatus([]string{"rule1"})
	if len(statuses) != 1 || statuses[0].ConfigRuleName != "rule1" {
		t.Fatalf("DescribeConfigRuleEvaluationStatus: %v", statuses)
	}
}

func TestDescribeConfigRuleEvaluationStatus_All(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r1"})
	_ = b.PutConfigRule(&awsconfig.ConfigRule{ConfigRuleName: "r2"})
	_ = b.StartConfigRulesEvaluation()

	statuses := b.DescribeConfigRuleEvaluationStatus(nil)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d: %v", len(statuses), statuses)
	}
}

func TestPutEvaluations(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "rule1", ComplianceType: "COMPLIANT"},
	})
	if err != nil {
		t.Fatalf("PutEvaluations: %v", err)
	}

	ct := b.GetConfigRuleComplianceType("rule1")
	if ct != "COMPLIANT" {
		t.Fatalf("expected COMPLIANT, got %q", ct)
	}
}

func TestPutExternalEvaluation(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutExternalEvaluation(awsconfig.EvaluationResult{
		ConfigRuleName: "rule1",
		ComplianceType: "NON_COMPLIANT",
	})
	if err != nil {
		t.Fatalf("PutExternalEvaluation: %v", err)
	}

	ct := b.GetConfigRuleComplianceType("rule1")
	if ct != "NON_COMPLIANT" {
		t.Fatalf("expected NON_COMPLIANT, got %q", ct)
	}
}

func TestDescribeComplianceByConfigRule(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
	})

	out := b.DescribeComplianceByConfigRule([]string{"r1"})
	if len(out) != 1 || out[0].Compliance.ComplianceType != "COMPLIANT" {
		t.Fatalf("DescribeComplianceByConfigRule: %v", out)
	}
}

func TestGetComplianceSummaryByConfigRule(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	out := b.GetComplianceSummaryByConfigRule()
	if out == nil {
		t.Fatal("expected non-nil slice")
	}
}

// --- Group 6: Delivery channel status ---

func TestDescribeDeliveryChannelStatus(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutDeliveryChannel("chan1", "my-bucket", "")

	statuses := b.DescribeDeliveryChannelStatus(nil)
	if len(statuses) != 1 || statuses[0].Name != "chan1" {
		t.Fatalf("DescribeDeliveryChannelStatus: %v", statuses)
	}
}

func TestDescribeDeliveryChannelStatus_Filtered(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutDeliveryChannel("chan1", "bucket1", "")
	_ = b.PutDeliveryChannel("chan2", "bucket2", "")

	statuses := b.DescribeDeliveryChannelStatus([]string{"chan1"})
	if len(statuses) != 1 || statuses[0].Name != "chan1" {
		t.Fatalf("expected 1 status, got %v", statuses)
	}
}

// --- Group 7: ConformancePack status ---

func TestDescribeConformancePackStatus(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutConformancePack("pack1")

	statuses := b.DescribeConformancePackStatus(nil)
	if len(statuses) != 1 || statuses[0].ConformancePackName != "pack1" {
		t.Fatalf("DescribeConformancePackStatus: %v", statuses)
	}

	if statuses[0].ConformancePackState != "COMPLETE" {
		t.Fatalf("expected COMPLETE state, got %q", statuses[0].ConformancePackState)
	}
}

func TestDescribeConformancePackCompliance(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	out := b.DescribeConformancePackCompliance("pack1")
	if len(out) != 0 {
		t.Fatalf("expected empty, got %v", out)
	}
}

// --- Group 8: Org rule/pack status ---

func TestDescribeOrganizationConfigRuleStatuses(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutOrganizationConfigRule("org-rule1")

	statuses := b.DescribeOrganizationConfigRuleStatuses(nil)
	if len(statuses) != 1 || statuses[0].OrganizationConfigRuleName != "org-rule1" {
		t.Fatalf("DescribeOrganizationConfigRuleStatuses: %v", statuses)
	}

	if statuses[0].OrganizationRuleStatus != "CREATE_SUCCESSFUL" {
		t.Fatalf("expected CREATE_SUCCESSFUL, got %q", statuses[0].OrganizationRuleStatus)
	}
}

func TestDescribeOrganizationConformancePackStatuses(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutOrganizationConformancePack("org-pack1")

	statuses := b.DescribeOrganizationConformancePackStatuses(nil)
	if len(statuses) != 1 || statuses[0].OrganizationConformancePackName != "org-pack1" {
		t.Fatalf("DescribeOrganizationConformancePackStatuses: %v", statuses)
	}

	if statuses[0].Status != "CREATE_SUCCESSFUL" {
		t.Fatalf("expected CREATE_SUCCESSFUL, got %q", statuses[0].Status)
	}
}

// --- Group 9: ResourceConfig operations ---

func TestPutResourceConfig(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutResourceConfig("AWS::S3::Bucket", "my-bucket", `{"Name":"my-bucket"}`)
	if err != nil {
		t.Fatalf("PutResourceConfig: %v", err)
	}

	items := b.GetResourceConfigHistory("AWS::S3::Bucket", "my-bucket")
	if len(items) != 1 || items[0].ResourceID != "my-bucket" {
		t.Fatalf("GetResourceConfigHistory: %v", items)
	}
}

func TestGetResourceConfigHistory_NotFound(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	items := b.GetResourceConfigHistory("AWS::S3::Bucket", "nonexistent")
	if len(items) != 0 {
		t.Fatalf("expected empty, got %v", items)
	}
}

func TestListDiscoveredResources(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutResourceConfig("AWS::S3::Bucket", "bucket1", "{}")
	_ = b.PutResourceConfig("AWS::S3::Bucket", "bucket2", "{}")
	_ = b.PutResourceConfig("AWS::EC2::Instance", "i-123", "{}")

	s3Items := b.ListDiscoveredResources("AWS::S3::Bucket")
	if len(s3Items) != 2 {
		t.Fatalf("expected 2 S3 items, got %d", len(s3Items))
	}

	ec2Items := b.ListDiscoveredResources("AWS::EC2::Instance")
	if len(ec2Items) != 1 {
		t.Fatalf("expected 1 EC2 item, got %d", len(ec2Items))
	}
}

// --- Group 10: Custom rule policy operations ---

func TestGetCustomRulePolicy_DefaultEmpty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	policy := b.GetCustomRulePolicy("my-rule")
	if policy != "" {
		t.Fatalf("expected empty policy, got %q", policy)
	}
}

func TestGetOrganizationCustomRulePolicy_DefaultEmpty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	policy := b.GetOrganizationCustomRulePolicy("org-rule")
	if policy != "" {
		t.Fatalf("expected empty policy, got %q", policy)
	}
}

// --- Group 11: Misc operations ---

func TestGetAggregateDiscoveredResourceCounts(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	if b.GetAggregateDiscoveredResourceCounts() != 0 {
		t.Fatal("expected 0 initially")
	}

	_ = b.PutResourceConfig("AWS::S3::Bucket", "b1", "{}")
	_ = b.PutResourceConfig("AWS::S3::Bucket", "b2", "{}")
	_ = b.PutResourceConfig("AWS::EC2::Instance", "i1", "{}")

	if got := b.GetAggregateDiscoveredResourceCounts(); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	// Empty — should return non-nil empty item.
	item := b.GetAggregateResourceConfig()
	if item == nil {
		t.Fatal("expected non-nil")
	}

	_ = b.PutResourceConfig("AWS::S3::Bucket", "my-bucket", "{}")
	item = b.GetAggregateResourceConfig()
	if item.ResourceType != "AWS::S3::Bucket" {
		t.Fatalf("expected AWS::S3::Bucket, got %q", item.ResourceType)
	}
}

func TestDescribeAggregateComplianceByConfigRules(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ComplianceType: "COMPLIANT"},
	})

	out := b.DescribeAggregateComplianceByConfigRules()
	if len(out) != 1 {
		t.Fatalf("expected 1, got %d", len(out))
	}
}

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
