package accessanalyzer_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestApplyArchiveRule verifies ApplyArchiveRule archives active findings.
func TestApplyArchiveRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		name       string
		wantStatus int
	}{
		{
			name: "archives_active_findings",
			body: map[string]any{"ruleName": "my-rule"},
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				arn := mustAnalyzer(t, b, "arch-analyzer")
				_, _ = b.CreateArchiveRule("arch-analyzer", "my-rule", nil)
				_, _ = b.AddFinding("arch-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::b", nil, nil, nil)

				return arn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found_returns_404",
			body: map[string]any{
				"analyzerArn": "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing",
				"ruleName":    "r",
			},
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return ""
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			arn := tt.setupFn(b)

			if arn != "" {
				tt.body["analyzerArn"] = arn
			}

			rec := doRequest(t, h, http.MethodPut, "/archive-rule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCreateArchiveRule_RealClient_OnlyArchivesMatchingFindings drives
// CreateArchiveRule through the real aws-sdk-go-v2 client with a
// resourceType-scoped filter. Real AWS's auto-apply behavior only archives
// existing active findings that match the new rule's own filter -- this
// used to archive every active finding for the analyzer regardless of the
// filter, which would have wrongly archived a finding the rule's criteria
// does not even match.
func TestCreateArchiveRule_RealClient_OnlyArchivesMatchingFindings(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	_, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("archive-filter-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	bucketFinding, err := b.AddFinding(
		"archive-filter-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil,
	)
	require.NoError(t, err)
	roleFinding, err := b.AddFinding(
		"archive-filter-analyzer", "AWS::IAM::Role", "arn:aws:iam::000000000000:role/r", nil, nil, nil,
	)
	require.NoError(t, err)

	_, err = client.CreateArchiveRule(t.Context(), &aasdk.CreateArchiveRuleInput{
		AnalyzerName: aws.String("archive-filter-analyzer"),
		RuleName:     aws.String("s3-only"),
		Filter: map[string]aatypes.Criterion{
			"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
		},
	})
	require.NoError(t, err)

	got, err := b.GetFinding("archive-filter-analyzer", bucketFinding.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusArchived, got.Status, "matching finding must be archived")

	got, err = b.GetFinding("archive-filter-analyzer", roleFinding.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusActive, got.Status, "non-matching finding must stay active")
}

// TestApplyArchiveRule_RealClient_OnlyArchivesMatchingFindings is the
// retroactive-apply counterpart of
// TestCreateArchiveRule_RealClient_OnlyArchivesMatchingFindings: real AWS's
// ApplyArchiveRule archives only the findings matching the NAMED rule's own
// filter (ApplyArchiveRuleInput.RuleName is required,
// api_op_ApplyArchiveRule.go:37-40) -- this used to archive every active
// finding for the analyzer regardless of that rule's criteria, and treated
// the required RuleName as optional.
func TestApplyArchiveRule_RealClient_OnlyArchivesMatchingFindings(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)
	client := newTestAccessAnalyzerClient(t, h)

	analyzer, err := client.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
		AnalyzerName: aws.String("apply-filter-analyzer"),
		Type:         aatypes.TypeAccount,
	})
	require.NoError(t, err)

	_, err = client.CreateArchiveRule(t.Context(), &aasdk.CreateArchiveRuleInput{
		AnalyzerName: aws.String("apply-filter-analyzer"),
		RuleName:     aws.String("s3-only"),
		Filter: map[string]aatypes.Criterion{
			"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
		},
	})
	require.NoError(t, err)

	bucketFinding, err := b.AddFinding(
		"apply-filter-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil,
	)
	require.NoError(t, err)
	roleFinding, err := b.AddFinding(
		"apply-filter-analyzer", "AWS::IAM::Role", "arn:aws:iam::000000000000:role/r", nil, nil, nil,
	)
	require.NoError(t, err)

	_, err = client.ApplyArchiveRule(t.Context(), &aasdk.ApplyArchiveRuleInput{
		AnalyzerArn: analyzer.Arn,
		RuleName:    aws.String("s3-only"),
	})
	require.NoError(t, err)

	got, err := b.GetFinding("apply-filter-analyzer", bucketFinding.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusArchived, got.Status, "matching finding must be archived")

	got, err = b.GetFinding("apply-filter-analyzer", roleFinding.ID)
	require.NoError(t, err)
	assert.Equal(t, accessanalyzer.FindingStatusActive, got.Status, "non-matching finding must stay active")
}
