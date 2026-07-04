package awsconfig_test

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// --- SelectResourceConfig / SelectAggregateResourceConfig ---

func TestSelectResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		expression string
		wantIDs    []string
	}{
		{
			name:       "select all with no where clause",
			expression: "SELECT resourceId",
			wantIDs:    []string{"bucket-a", "bucket-b", "instance-a"},
		},
		{
			name:       "where equals filters by resource type",
			expression: "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'",
			wantIDs:    []string{"bucket-a", "bucket-b"},
		},
		{
			name:       "where equals filters by configuration field",
			expression: "SELECT resourceId WHERE region = 'us-west-2'",
			wantIDs:    []string{"bucket-b"},
		},
		{
			name:       "where like matches a pattern",
			expression: "SELECT resourceId WHERE resourceId LIKE 'bucket-%'",
			wantIDs:    []string{"bucket-a", "bucket-b"},
		},
		{
			name:       "no matches returns empty",
			expression: "SELECT resourceId WHERE resourceType = 'AWS::DynamoDB::Table'",
			wantIDs:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			mustPutResourceConfig(t, b, "AWS::S3::Bucket", "bucket-a", `{"region":"us-east-1"}`)
			mustPutResourceConfig(t, b, "AWS::S3::Bucket", "bucket-b", `{"region":"us-west-2"}`)
			mustPutResourceConfig(t, b, "AWS::EC2::Instance", "instance-a", `{"region":"us-east-1"}`)

			results := b.SelectResourceConfig(tt.expression)
			gotIDs := extractResourceIDs(t, results)

			assertStringSlicesEqual(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestSelectResourceConfig_SelectsRequestedFields(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	mustPutResourceConfig(t, b, "AWS::S3::Bucket", "bucket-a", `{"region":"us-east-1"}`)

	results := b.SelectResourceConfig("SELECT resourceId, resourceType, region WHERE resourceType = 'AWS::S3::Bucket'")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}

	var row map[string]any
	if err := json.Unmarshal([]byte(results[0]), &row); err != nil {
		t.Fatalf("unmarshal row: %v", err)
	}

	if row["resourceId"] != "bucket-a" || row["resourceType"] != "AWS::S3::Bucket" || row["region"] != "us-east-1" {
		t.Fatalf("unexpected row: %v", row)
	}
}

func TestSelectAggregateResourceConfig_EvaluatesSameQuery(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	mustPutResourceConfig(t, b, "AWS::S3::Bucket", "bucket-a", `{}`)

	results := b.SelectAggregateResourceConfig("SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'")
	gotIDs := extractResourceIDs(t, results)
	assertStringSlicesEqual(t, []string{"bucket-a"}, gotIDs)
}

func mustPutResourceConfig(t *testing.T, b *awsconfig.InMemoryBackend, resourceType, resourceID, config string) {
	t.Helper()

	if err := b.PutResourceConfig(resourceType, resourceID, config); err != nil {
		t.Fatalf("PutResourceConfig: %v", err)
	}
}

func extractResourceIDs(t *testing.T, results []string) []string {
	t.Helper()

	ids := make([]string, 0, len(results))

	for _, r := range results {
		var row map[string]any
		if err := json.Unmarshal([]byte(r), &row); err != nil {
			t.Fatalf("unmarshal row: %v", err)
		}

		if id, ok := row["resourceId"].(string); ok {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	return ids
}

func assertStringSlicesEqual(t *testing.T, want, got []string) {
	t.Helper()

	if len(want) != len(got) {
		t.Fatalf("want %v, got %v", want, got)
	}

	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// --- GetComplianceSummaryByResourceType ---

func TestGetComplianceSummaryByResourceType_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	out := b.GetComplianceSummaryByResourceType(nil)
	if len(out) != 0 {
		t.Fatalf("expected empty summary for fresh backend, got %v", out)
	}
}

func TestGetComplianceSummaryByResourceType_AggregatesByResourceType(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ResourceType: "AWS::S3::Bucket", ResourceID: "b1", ComplianceType: "COMPLIANT"},
		{ConfigRuleName: "r1", ResourceType: "AWS::S3::Bucket", ResourceID: "b2", ComplianceType: "NON_COMPLIANT"},
		{ConfigRuleName: "r1", ResourceType: "AWS::EC2::Instance", ResourceID: "i1", ComplianceType: "COMPLIANT"},
	})
	if err != nil {
		t.Fatalf("PutEvaluations: %v", err)
	}

	out := b.GetComplianceSummaryByResourceType(nil)
	if len(out) != 2 {
		t.Fatalf("expected 2 resource types, got %d: %v", len(out), out)
	}

	byType := make(map[string]awsconfig.ComplianceSummaryByResourceType, len(out))
	for _, s := range out {
		byType[s.ResourceType] = s
	}

	bucket, ok := byType["AWS::S3::Bucket"]
	if !ok {
		t.Fatalf("missing AWS::S3::Bucket summary: %v", out)
	}

	if bucket.ComplianceSummary.CompliantResourceCount.CappedCount != 1 ||
		bucket.ComplianceSummary.NonCompliantResourceCount.CappedCount != 1 {
		t.Fatalf("unexpected bucket summary: %+v", bucket)
	}

	instance, ok := byType["AWS::EC2::Instance"]
	if !ok {
		t.Fatalf("missing AWS::EC2::Instance summary: %v", out)
	}

	if instance.ComplianceSummary.CompliantResourceCount.CappedCount != 1 ||
		instance.ComplianceSummary.NonCompliantResourceCount.CappedCount != 0 {
		t.Fatalf("unexpected instance summary: %+v", instance)
	}
}

func TestGetComplianceSummaryByResourceType_FiltersByResourceTypes(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutEvaluations([]awsconfig.EvaluationResult{
		{ConfigRuleName: "r1", ResourceType: "AWS::S3::Bucket", ResourceID: "b1", ComplianceType: "COMPLIANT"},
		{ConfigRuleName: "r1", ResourceType: "AWS::EC2::Instance", ResourceID: "i1", ComplianceType: "COMPLIANT"},
	})
	if err != nil {
		t.Fatalf("PutEvaluations: %v", err)
	}

	out := b.GetComplianceSummaryByResourceType([]string{"AWS::S3::Bucket"})
	if len(out) != 1 || out[0].ResourceType != "AWS::S3::Bucket" {
		t.Fatalf("expected only AWS::S3::Bucket, got %v", out)
	}
}
