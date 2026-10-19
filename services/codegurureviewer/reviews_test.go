package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ruleIDs(recs []types.RecommendationSummary) []string {
	out := make([]string, 0, len(recs))
	for _, r := range recs {
		out = append(out, aws.ToString(r.RuleMetadata.RuleId))
	}

	return out
}

func TestCodeReviewCodeCommit(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(
		t,
		"app",
		map[string]string{"main.go": badGo, "keys.py": badPy, "aws-codeguru-reviewer.yml": "version: 1\n"},
	)
	assoc := f.associateCodeCommit(t, "app")

	created := f.review(t, "first", assoc, "main")
	cr := created.CodeReview
	assert.Equal(t, types.JobStatePending, cr.State)
	assert.Equal(t, types.TypeRepositoryAnalysis, cr.Type)
	assert.Equal(t, "app", aws.ToString(cr.RepositoryName))
	assert.Equal(t, testAccount, aws.ToString(cr.Owner))
	assert.Equal(t, assoc, aws.ToString(cr.AssociationArn))
	assert.Equal(t, types.ProviderTypeCodeCommit, cr.ProviderType)
	assert.Contains(t, aws.ToString(cr.CodeReviewArn), assoc+":codereview:")
	assert.Equal(t, "main", aws.ToString(cr.SourceCodeType.RepositoryHead.BranchName))
	assert.Nil(t, cr.Metrics, "metrics appear once the review completes")

	pending, err := f.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: cr.CodeReviewArn},
	)
	require.NoError(t, err)
	assert.Empty(t, pending.RecommendationSummaries)

	f.settle()

	d, err := f.client.DescribeCodeReview(t.Context(), &sdk.DescribeCodeReviewInput{CodeReviewArn: cr.CodeReviewArn})
	require.NoError(t, err)
	assert.Equal(t, types.JobStateCompleted, d.CodeReview.State)
	assert.Equal(t, types.ConfigFileStatePresent, d.CodeReview.ConfigFileState)
	assert.ElementsMatch(
		t,
		[]types.AnalysisType{types.AnalysisTypeSecurity, types.AnalysisTypeCodeQuality},
		d.CodeReview.AnalysisTypes,
	)

	recs, err := f.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: cr.CodeReviewArn},
	)
	require.NoError(t, err)

	byRule := map[string]types.RecommendationSummary{}
	for _, r := range recs.RecommendationSummaries {
		byRule[aws.ToString(r.RuleMetadata.RuleId)] = r
	}

	assert.ElementsMatch(t, []string{
		"unclosed-resource",
		"err-nil-misuse",
		"sql-string-concatenation",
		"aws-hardcoded-access-key",
		"todo-fixme-density",
	}, ruleIDs(recs.RecommendationSummaries))

	key := byRule["aws-hardcoded-access-key"]
	assert.Equal(t, "keys.py", aws.ToString(key.FilePath))
	assert.Equal(t, int32(4), aws.ToInt32(key.StartLine))
	assert.Equal(t, types.SeverityCritical, key.Severity)
	assert.Equal(t, types.RecommendationCategorySecurityIssues, key.RecommendationCategory)
	assert.NotContains(t, aws.ToString(key.Description), "ABCDEFGHIJKLMNOP", "the key must be masked")
	assert.Len(t, aws.ToString(key.RecommendationId), 64)

	leak := byRule["unclosed-resource"]
	assert.Equal(t, "main.go", aws.ToString(leak.FilePath))
	assert.Equal(t, int32(6), aws.ToInt32(leak.StartLine))
	assert.Equal(t, types.RecommendationCategoryResourceLeaks, leak.RecommendationCategory)

	nilMisuse := byRule["err-nil-misuse"]
	assert.Equal(t, int32(7), aws.ToInt32(nilMisuse.StartLine))
	assert.Equal(t, int32(8), aws.ToInt32(nilMisuse.EndLine))

	m := d.CodeReview.Metrics
	require.NotNil(t, m)
	assert.Equal(t, int64(5), aws.ToInt64(m.FindingsCount))
	assert.Positive(t, aws.ToInt64(m.MeteredLinesOfCodeCount))
	assert.Equal(t, int64(0), aws.ToInt64(m.SuppressedLinesOfCodeCount))

	again := f.review(t, "second", assoc, "main")
	f.settle()

	recs2, err := f.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: again.CodeReview.CodeReviewArn},
	)
	require.NoError(t, err)
	require.Len(t, recs2.RecommendationSummaries, len(recs.RecommendationSummaries))

	for i := range recs.RecommendationSummaries {
		assert.Equal(t, aws.ToString(recs.RecommendationSummaries[i].RecommendationId),
			aws.ToString(recs2.RecommendationSummaries[i].RecommendationId), "same code, same recommendation ids")
	}

	var ids []string

	var token *string

	for range 10 {
		page, perr := f.client.ListRecommendations(t.Context(), &sdk.ListRecommendationsInput{
			CodeReviewArn: cr.CodeReviewArn, MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, perr)

		ids = append(ids, ruleIDs(page.RecommendationSummaries)...)

		if token = page.NextToken; token == nil {
			break
		}
	}

	assert.ElementsMatch(t, ruleIDs(recs.RecommendationSummaries), ids)
}

func TestCodeReviewAnalysisTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		types []types.AnalysisType
		want  []string
	}{
		{
			name:  "security",
			types: []types.AnalysisType{types.AnalysisTypeSecurity},
			want:  []string{"aws-hardcoded-access-key", "sql-string-concatenation"},
		},
		{
			name: "code quality", types: []types.AnalysisType{types.AnalysisTypeCodeQuality},
			want: []string{"unclosed-resource", "err-nil-misuse", "todo-fixme-density"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.commitRepo(t, "app", map[string]string{"main.go": badGo, "keys.py": badPy})
			assoc := f.associateCodeCommit(t, "app")

			out, err := f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
				Name: aws.String("r"), RepositoryAssociationArn: aws.String(assoc),
				Type: &types.CodeReviewType{
					AnalysisTypes: tt.types,
					RepositoryAnalysis: &types.RepositoryAnalysis{
						RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String("main")},
					},
				},
			})
			require.NoError(t, err)
			f.settle()

			recs, err := f.client.ListRecommendations(
				t.Context(),
				&sdk.ListRecommendationsInput{CodeReviewArn: out.CodeReview.CodeReviewArn},
			)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, ruleIDs(recs.RecommendationSummaries))
		})
	}
}

func TestCodeReviewS3(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucketWithZip(
		t,
		"codeguru-reviewer-src",
		"code/src.zip",
		map[string]string{"app/db.py": "q = f\"select * from t where a = {a}\"\n"},
	)

	assoc, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{
		Repository: &types.Repository{S3Bucket: &types.S3Repository{
			BucketName: aws.String("codeguru-reviewer-src"), Name: aws.String("s3app"),
		}},
	})
	require.NoError(t, err)
	f.settle()

	s3src := func(name, bucket, key string) *types.CodeReviewType {
		return &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{
			SourceCodeType: &types.SourceCodeType{S3BucketRepository: &types.S3BucketRepository{
				Name: aws.String(name),
				Details: &types.S3RepositoryDetails{
					BucketName:    aws.String(bucket),
					CodeArtifacts: &types.CodeArtifacts{SourceCodeArtifactsObjectKey: aws.String(key)},
				},
			}},
		}}
	}

	arn := assoc.RepositoryAssociation.AssociationArn

	ok, err := f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
		Name: aws.String(
			"s3-ok",
		),
		RepositoryAssociationArn: arn,
		Type:                     s3src("s3app", "codeguru-reviewer-src", "code/src.zip"),
	})
	require.NoError(t, err)

	missing, err := f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
		Name: aws.String(
			"s3-missing",
		),
		RepositoryAssociationArn: arn,
		Type:                     s3src("s3app", "codeguru-reviewer-src", "code/gone.zip"),
	})
	require.NoError(t, err)

	_, err = f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
		Name: aws.String(
			"s3-wrong",
		),
		RepositoryAssociationArn: arn,
		Type:                     s3src("other", "codeguru-reviewer-src", "code/src.zip"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.CreateCodeReview(t.Context(), &sdk.CreateCodeReviewInput{
		Name: aws.String("s3-nosrc"), RepositoryAssociationArn: arn,
		Type: &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{
			RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String("main")},
		}},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	f.settle()

	recs, err := f.client.ListRecommendations(
		t.Context(),
		&sdk.ListRecommendationsInput{CodeReviewArn: ok.CodeReview.CodeReviewArn},
	)
	require.NoError(t, err)
	require.Len(t, recs.RecommendationSummaries, 1)
	assert.Equal(t, "app/db.py", aws.ToString(recs.RecommendationSummaries[0].FilePath))
	assert.Equal(t, "sql-string-concatenation", aws.ToString(recs.RecommendationSummaries[0].RuleMetadata.RuleId))

	failed, err := f.client.DescribeCodeReview(
		t.Context(),
		&sdk.DescribeCodeReviewInput{CodeReviewArn: missing.CodeReview.CodeReviewArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.JobStateFailed, failed.CodeReview.State)
	assert.Contains(t, aws.ToString(failed.CodeReview.StateReason), "code/gone.zip")
	assert.Nil(t, failed.CodeReview.Metrics)
}

func TestCodeReviewErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "app", map[string]string{"main.go": "package main\n"})
	assoc := f.associateCodeCommit(t, "app")
	f.review(t, "taken", assoc, "main")

	pendingAssoc := func() string {
		f.commitRepo(t, "late", map[string]string{"a.go": "package a\n"})
		out, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{
			Repository: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("late")}},
		})
		require.NoError(t, err)

		return aws.ToString(out.RepositoryAssociation.AssociationArn)
	}()

	head := func(branch string) *types.CodeReviewType {
		return &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{
			RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String(branch)},
		}}
	}

	tests := []struct {
		in       *sdk.CreateCodeReviewInput
		name     string
		wantCode string
	}{
		{
			name: "duplicate name",
			in: &sdk.CreateCodeReviewInput{
				Name:                     aws.String("taken"),
				RepositoryAssociationArn: aws.String(assoc),
				Type:                     head("main"),
			},
			wantCode: "ConflictException",
		},
		{
			name: "bad name",
			in: &sdk.CreateCodeReviewInput{
				Name:                     aws.String("has space"),
				RepositoryAssociationArn: aws.String(assoc),
				Type:                     head("main"),
			},
			wantCode: "ValidationException",
		},
		{
			name: "association still associating",
			in: &sdk.CreateCodeReviewInput{
				Name:                     aws.String("n1"),
				RepositoryAssociationArn: aws.String(pendingAssoc),
				Type:                     head("main"),
			},
			wantCode: "ConflictException",
		},
		{
			name: "unknown association",
			in: &sdk.CreateCodeReviewInput{
				Name: aws.String("n2"),
				Type: head("main"),
				RepositoryAssociationArn: aws.String(
					"arn:aws:codeguru-reviewer:us-east-1:000000000000:association:nope",
				),
			},
			wantCode: "NotFoundException",
		},
		{
			name: "malformed association arn",
			in: &sdk.CreateCodeReviewInput{
				Name:                     aws.String("n3"),
				RepositoryAssociationArn: aws.String("x"),
				Type:                     head("main"),
			},
			wantCode: "ValidationException",
		},
		{
			name: "missing branch",
			in: &sdk.CreateCodeReviewInput{
				Name:                     aws.String("n4"),
				RepositoryAssociationArn: aws.String(assoc),
				Type:                     head(""),
			},
			wantCode: "ValidationException",
		},
		{
			name: "bad analysis type",
			in: &sdk.CreateCodeReviewInput{
				Name: aws.String("n6"), RepositoryAssociationArn: aws.String(assoc),
				Type: &types.CodeReviewType{
					AnalysisTypes:      []types.AnalysisType{"Vibes"},
					RepositoryAnalysis: head("main").RepositoryAnalysis,
				},
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateCodeReview(t.Context(), tt.in)
			assert.Equal(t, tt.wantCode, apiCode(err))
		})
	}
}

func TestListCodeReviews(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "app", map[string]string{"main.go": "package main\n"})
	f.commitRepo(t, "lib", map[string]string{"lib.go": "package lib\n"})
	appAssoc := f.associateCodeCommit(t, "app")
	libAssoc := f.associateCodeCommit(t, "lib")

	f.review(t, "r1", appAssoc, "main")
	f.settle()
	f.review(t, "r2", libAssoc, "main")
	f.clock.Add(1)
	f.review(t, "r3", appAssoc, "missing-branch")

	tests := []struct {
		in       *sdk.ListCodeReviewsInput
		name     string
		wantCode string
		want     []string
	}{
		{
			name: "repository analysis",
			in:   &sdk.ListCodeReviewsInput{Type: types.TypeRepositoryAnalysis},
			want: []string{"r3", "r2", "r1"},
		},
		{name: "pull request", in: &sdk.ListCodeReviewsInput{Type: types.TypePullRequest}, want: nil},
		{
			name: "by repository",
			in:   &sdk.ListCodeReviewsInput{Type: types.TypeRepositoryAnalysis, RepositoryNames: []string{"lib"}},
			want: []string{"r2"},
		},
		{
			name: "by state",
			in: &sdk.ListCodeReviewsInput{
				Type:   types.TypeRepositoryAnalysis,
				States: []types.JobState{types.JobStateCompleted},
			},
			want: []string{"r1"},
		},
		{
			name: "by pending state",
			in: &sdk.ListCodeReviewsInput{
				Type:   types.TypeRepositoryAnalysis,
				States: []types.JobState{types.JobStatePending},
			},
			want: []string{"r3", "r2"},
		},
		{
			name: "by provider",
			in: &sdk.ListCodeReviewsInput{
				Type: types.TypeRepositoryAnalysis, ProviderTypes: []types.ProviderType{types.ProviderTypeBitbucket},
			},
			want: nil,
		},
		{name: "bad type", in: &sdk.ListCodeReviewsInput{Type: "Mystery"}, wantCode: "ValidationException"},
		{
			name: "bad state",
			in: &sdk.ListCodeReviewsInput{
				Type:   types.TypeRepositoryAnalysis,
				States: []types.JobState{"Bogus"},
			},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.ListCodeReviews(t.Context(), tt.in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			var got []string
			for _, s := range out.CodeReviewSummaries {
				got = append(got, aws.ToString(s.Name))
			}

			assert.Equal(t, tt.want, got)
		})
	}

	done, err := f.client.ListCodeReviews(t.Context(), &sdk.ListCodeReviewsInput{
		Type: types.TypeRepositoryAnalysis, States: []types.JobState{types.JobStateCompleted},
	})
	require.NoError(t, err)
	require.Len(t, done.CodeReviewSummaries, 1)
	require.NotNil(t, done.CodeReviewSummaries[0].MetricsSummary)
	assert.Equal(t, int64(0), aws.ToInt64(done.CodeReviewSummaries[0].MetricsSummary.FindingsCount))
}

func TestCreateCodeReviewIdempotency(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "app", map[string]string{"main.go": "package main\n"})
	assoc := f.associateCodeCommit(t, "app")

	in := func(name string) *sdk.CreateCodeReviewInput {
		return &sdk.CreateCodeReviewInput{
			Name: aws.String(name), RepositoryAssociationArn: aws.String(assoc), ClientRequestToken: aws.String("tok"),
			Type: &types.CodeReviewType{RepositoryAnalysis: &types.RepositoryAnalysis{
				RepositoryHead: &types.RepositoryHeadSourceCodeType{BranchName: aws.String("main")},
			}},
		}
	}

	one, err := f.client.CreateCodeReview(t.Context(), in("same"))
	require.NoError(t, err)

	two, err := f.client.CreateCodeReview(t.Context(), in("same"))
	require.NoError(t, err)
	assert.Equal(t, one.CodeReview.CodeReviewArn, two.CodeReview.CodeReviewArn)

	_, err = f.client.CreateCodeReview(t.Context(), in("different"))
	assert.Equal(t, "ValidationException", apiCode(err))
}
func TestCodeReviewFailures(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "app", map[string]string{"main.go": "package main\n"})
	assoc := f.associateCodeCommit(t, "app")

	bad := f.review(t, "bad-branch", assoc, "nope")
	f.settle()

	d, err := f.client.DescribeCodeReview(
		t.Context(),
		&sdk.DescribeCodeReviewInput{CodeReviewArn: bad.CodeReview.CodeReviewArn},
	)
	require.NoError(t, err)
	assert.Equal(t, types.JobStateFailed, d.CodeReview.State)
	assert.Contains(t, aws.ToString(d.CodeReview.StateReason), `"nope"`)

	_, err = f.client.DescribeCodeReview(t.Context(), &sdk.DescribeCodeReviewInput{
		CodeReviewArn: aws.String(assoc + ":codereview:missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.ListRecommendations(t.Context(), &sdk.ListRecommendationsInput{CodeReviewArn: aws.String("nope")})
	assert.Equal(t, "ValidationException", apiCode(err))
}
