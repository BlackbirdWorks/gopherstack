package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

func TestListFindings_FilterOperators(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter map[string]aatypes.Criterion
		name   string
		want   []string
	}{
		{name: "neq_resource_type", filter: map[string]aatypes.Criterion{
			"resourceType": {Neq: []string{"AWS::S3::Bucket"}},
		}, want: []string{"queue"}},
		{name: "eq_principal", filter: map[string]aatypes.Criterion{
			"principal.AWS": {Eq: []string{"123456789012"}},
		}, want: []string{"bucket"}},
		{name: "exists_principal_false", filter: map[string]aatypes.Criterion{
			"principal.AWS": {Exists: aws.Bool(false)},
		}, want: []string{"queue"}},
		{name: "exists_principal_true", filter: map[string]aatypes.Criterion{
			"principal.AWS": {Exists: aws.Bool(true)},
		}, want: []string{"bucket"}},
		{name: "eq_is_public", filter: map[string]aatypes.Criterion{
			"isPublic": {Eq: []string{"true"}},
		}, want: []string{"bucket"}},
		{name: "owner_account_match", filter: map[string]aatypes.Criterion{
			"resourceOwnerAccount": {Eq: []string{"000000000000"}},
		}, want: []string{"bucket", "queue"}},
		{name: "owner_account_other", filter: map[string]aatypes.Criterion{
			"resourceOwnerAccount": {Eq: []string{"111111111111"}},
		}, want: nil},
		{name: "error_exists", filter: map[string]aatypes.Criterion{
			"error": {Exists: aws.Bool(true)},
		}, want: nil},
		{name: "error_absent", filter: map[string]aatypes.Criterion{
			"error": {Exists: aws.Bool(false)},
		}, want: []string{"bucket", "queue"}},
		{name: "eq_action", filter: map[string]aatypes.Criterion{
			"action": {Eq: []string{"sqs:SendMessage"}},
		}, want: []string{"queue"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(b))
			ctx := t.Context()

			an, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("filter-ops"), Type: aatypes.TypeAccount,
			})
			require.NoError(t, err)

			bucket, err := b.AddFinding("filter-ops", "AWS::S3::Bucket", "arn:aws:s3:::bucket",
				[]string{"s3:GetObject"}, map[string]string{"AWS": "123456789012"}, aws.Bool(true))
			require.NoError(t, err)
			queue, err := b.AddFinding("filter-ops", "AWS::SQS::Queue", "arn:aws:sqs:us-east-1:0:queue",
				[]string{"sqs:SendMessage"}, nil, aws.Bool(false))
			require.NoError(t, err)

			ids := map[string]string{"bucket": bucket.ID, "queue": queue.ID}

			out, err := client.ListFindings(ctx, &aasdk.ListFindingsInput{AnalyzerArn: an.Arn, Filter: tt.filter})
			require.NoError(t, err)

			var got []string

			for _, f := range out.Findings {
				for name, id := range ids {
					if aws.ToString(f.Id) == id {
						got = append(got, name)
					}
				}
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestStartResourceScan_RecordsLastResourceAnalyzed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		want     string
	}{
		{name: "known_resource", resource: "arn:aws:s3:::known", want: "arn:aws:s3:::known"},
		{name: "unknown_resource", resource: "arn:aws:s3:::other", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(b))
			ctx := t.Context()

			an, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("scan"), Type: aatypes.TypeAccount,
			})
			require.NoError(t, err)

			_, err = b.AddAnalyzedResource(aws.ToString(an.Arn), "arn:aws:s3:::known", "AWS::S3::Bucket", false)
			require.NoError(t, err)

			_, err = client.StartResourceScan(ctx, &aasdk.StartResourceScanInput{
				AnalyzerArn: an.Arn, ResourceArn: aws.String(tt.resource),
			})
			require.NoError(t, err)

			got, err := client.GetAnalyzer(ctx, &aasdk.GetAnalyzerInput{AnalyzerName: aws.String("scan")})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(got.Analyzer.LastResourceAnalyzed))
			assert.Equal(t, tt.want != "", got.Analyzer.LastResourceAnalyzedAt != nil)
		})
	}
}

func TestValidatePolicy_RejectsUnknownEnumMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		locale  aatypes.Locale
		rtype   aatypes.ValidatePolicyResourceType
		wantErr bool
	}{
		{name: "valid", locale: aatypes.LocaleFr, rtype: aatypes.ValidatePolicyResourceTypeS3Bucket},
		{name: "bad_locale", locale: "XX", wantErr: true},
		{name: "bad_resource_type", rtype: "AWS::Nope::Thing", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAccessAnalyzerClient(t,
				accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")))

			_, err := client.ValidatePolicy(t.Context(), &aasdk.ValidatePolicyInput{
				PolicyDocument:             aws.String(`{"Version":"2012-10-17","Statement":[]}`),
				PolicyType:                 aatypes.PolicyTypeResourcePolicy,
				Locale:                     tt.locale,
				ValidatePolicyResourceType: tt.rtype,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestListAnalyzers_TypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		typ  aatypes.Type
		name string
		want []string
	}{
		{name: "account", typ: aatypes.TypeAccount, want: []string{"acct"}},
		{name: "org", typ: aatypes.TypeOrganization, want: []string{"org"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAccessAnalyzerClient(t,
				accessanalyzer.NewHandler(accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")))
			ctx := t.Context()

			for name, typ := range map[string]aatypes.Type{"acct": aatypes.TypeAccount, "org": aatypes.TypeOrganization} {
				_, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
					AnalyzerName: aws.String(name), Type: typ,
				})
				require.NoError(t, err)
			}

			out, err := client.ListAnalyzers(ctx, &aasdk.ListAnalyzersInput{Type: tt.typ})
			require.NoError(t, err)

			var got []string
			for _, a := range out.Analyzers {
				got = append(got, aws.ToString(a.Name))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
