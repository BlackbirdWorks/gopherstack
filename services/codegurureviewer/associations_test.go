package codegurureviewer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/codegurureviewer"
	"github.com/aws/aws-sdk-go-v2/service/codegurureviewer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssociateRepository(t *testing.T) {
	t.Parallel()

	third := func(conn string) *types.ThirdPartySourceRepository {
		return &types.ThirdPartySourceRepository{
			ConnectionArn: aws.String(conn), Name: aws.String("app"), Owner: aws.String("octo"),
		}
	}
	conn := "arn:aws:codestar-connections:us-east-1:000000000000:connection/abc"

	tests := []struct {
		repo         *types.Repository
		kms          *types.KMSKeyDetails
		name         string
		wantCode     string
		wantProvider types.ProviderType
		wantOwner    string
	}{
		{
			name:         "codecommit",
			repo:         &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")}},
			wantProvider: types.ProviderTypeCodeCommit, wantOwner: testAccount,
		},
		{
			name:     "codecommit repository missing",
			repo:     &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("ghost")}},
			wantCode: "ValidationException",
		},
		{
			name: "s3",
			repo: &types.Repository{S3Bucket: &types.S3Repository{
				BucketName: aws.String("codeguru-reviewer-b"), Name: aws.String("s3repo"),
			}},
			wantProvider: types.ProviderTypeS3Bucket, wantOwner: testAccount,
		},
		{
			name: "s3 bucket prefix",
			repo: &types.Repository{S3Bucket: &types.S3Repository{
				BucketName: aws.String("plain-bucket"), Name: aws.String("s3repo"),
			}},
			wantCode: "ValidationException",
		},
		{
			name: "s3 bucket missing",
			repo: &types.Repository{S3Bucket: &types.S3Repository{
				BucketName: aws.String("codeguru-reviewer-nope"), Name: aws.String("s3repo"),
			}},
			wantCode: "ValidationException",
		},
		{
			name:         "bitbucket",
			repo:         &types.Repository{Bitbucket: third(conn)},
			wantProvider: types.ProviderTypeBitbucket, wantOwner: "octo",
		},
		{
			name:         "ghes",
			repo:         &types.Repository{GitHubEnterpriseServer: third(conn)},
			wantProvider: types.ProviderTypeGitHubEnterpriseServer, wantOwner: "octo",
		},
		{
			name:     "third party needs a connection",
			repo:     &types.Repository{Bitbucket: third("nope")},
			wantCode: "ValidationException",
		},
		{name: "no member", repo: &types.Repository{}, wantCode: "ValidationException"},
		{
			name: "two members",
			repo: &types.Repository{
				CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")},
				Bitbucket:  third(conn),
			},
			wantCode: "ValidationException",
		},
		{
			name: "bad encryption option",
			repo: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")}},
			kms:  &types.KMSKeyDetails{EncryptionOption: "ROT13"}, wantCode: "ValidationException",
		},
		{
			name: "cmk needs a key id",
			repo: &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")}},
			kms:  &types.KMSKeyDetails{EncryptionOption: types.EncryptionOptionCmCmk}, wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.commitRepo(t, "src", map[string]string{"a.txt": "x"})
			f.bucketWithZip(t, "codeguru-reviewer-b", "src.zip", map[string]string{"a.txt": "x"})

			out, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{
				Repository: tt.repo, KMSKeyDetails: tt.kms,
				Tags: map[string]string{"env": "test"},
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			a := out.RepositoryAssociation
			assert.Equal(t, types.RepositoryAssociationStateAssociating, a.State)
			assert.Equal(t, tt.wantProvider, a.ProviderType)
			assert.Equal(t, tt.wantOwner, aws.ToString(a.Owner))
			assert.Contains(t, aws.ToString(a.AssociationArn), ":codeguru-reviewer:us-east-1:000000000000:association:")
			assert.NotEmpty(t, aws.ToString(a.AssociationId))
			assert.NotNil(t, a.CreatedTimeStamp)
			assert.Equal(t, map[string]string{"env": "test"}, out.Tags)

			if tt.wantProvider == types.ProviderTypeS3Bucket {
				assert.Equal(t, "codeguru-reviewer-b", aws.ToString(a.S3RepositoryDetails.BucketName))
			}
		})
	}
}

func TestAssociationLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "src", map[string]string{"a.txt": "x"})

	repo := &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String("src")}}

	out, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{Repository: repo})
	require.NoError(t, err)

	arn := out.RepositoryAssociation.AssociationArn
	describe := func() types.RepositoryAssociationState {
		d, derr := f.client.DescribeRepositoryAssociation(
			t.Context(),
			&sdk.DescribeRepositoryAssociationInput{AssociationArn: arn},
		)
		require.NoError(t, derr)

		return d.RepositoryAssociation.State
	}

	assert.Equal(t, types.RepositoryAssociationStateAssociating, describe())

	_, err = f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{Repository: repo})
	assert.Equal(t, "ConflictException", apiCode(err), "an associating repository cannot be associated twice")

	f.settle()
	assert.Equal(t, types.RepositoryAssociationStateAssociated, describe())

	dis, err := f.client.DisassociateRepository(t.Context(), &sdk.DisassociateRepositoryInput{AssociationArn: arn})
	require.NoError(t, err)
	assert.Equal(t, types.RepositoryAssociationStateDisassociating, dis.RepositoryAssociation.State)

	f.settle()
	assert.Equal(t, types.RepositoryAssociationStateDisassociated, describe())

	again, err := f.client.AssociateRepository(t.Context(), &sdk.AssociateRepositoryInput{Repository: repo})
	require.NoError(t, err, "a disassociated repository can be associated again")
	assert.NotEqual(t, arn, again.RepositoryAssociation.AssociationArn)

	_, err = f.client.DescribeRepositoryAssociation(t.Context(), &sdk.DescribeRepositoryAssociationInput{
		AssociationArn: aws.String("arn:aws:codeguru-reviewer:us-east-1:000000000000:association:missing"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))

	_, err = f.client.DisassociateRepository(t.Context(), &sdk.DisassociateRepositoryInput{
		AssociationArn: aws.String("arn:aws:codeguru-reviewer:us-east-1:000000000000:association:missing"),
	})
	assert.Equal(t, "NotFoundException", apiCode(err))

	_, err = f.client.DescribeRepositoryAssociation(t.Context(), &sdk.DescribeRepositoryAssociationInput{
		AssociationArn: aws.String("not-an-arn"),
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestListRepositoryAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)

	for _, n := range []string{"one", "two", "three"} {
		f.commitRepo(t, n, map[string]string{"a.txt": n})
		f.associateCodeCommit(t, n)
	}

	_, err := f.client.DisassociateRepository(t.Context(), &sdk.DisassociateRepositoryInput{
		AssociationArn: first(t, f).AssociationArn,
	})
	require.NoError(t, err)

	tests := []struct {
		in       *sdk.ListRepositoryAssociationsInput
		name     string
		wantCode string
		want     int
	}{
		{name: "all", in: &sdk.ListRepositoryAssociationsInput{}, want: 3},
		{name: "by name", in: &sdk.ListRepositoryAssociationsInput{Names: []string{"two", "three"}}, want: 2},
		{name: "by owner", in: &sdk.ListRepositoryAssociationsInput{Owners: []string{testAccount}}, want: 3},
		{name: "other owner", in: &sdk.ListRepositoryAssociationsInput{Owners: []string{"nobody"}}, want: 0},
		{
			name: "by provider",
			in: &sdk.ListRepositoryAssociationsInput{
				ProviderTypes: []types.ProviderType{types.ProviderTypeCodeCommit},
			},
			want: 3,
		},
		{
			name: "other provider",
			in: &sdk.ListRepositoryAssociationsInput{
				ProviderTypes: []types.ProviderType{types.ProviderTypeBitbucket},
			},
			want: 0,
		},
		{
			name: "by state",
			in: &sdk.ListRepositoryAssociationsInput{
				States: []types.RepositoryAssociationState{types.RepositoryAssociationStateAssociated},
			},
			want: 2,
		},
		{
			name: "bad state",
			in: &sdk.ListRepositoryAssociationsInput{
				States: []types.RepositoryAssociationState{"Bogus"},
			},
			wantCode: "ValidationException",
		},
		{name: "zero page", in: &sdk.ListRepositoryAssociationsInput{MaxResults: aws.Int32(0)}, want: 3},
		{
			name:     "huge page",
			in:       &sdk.ListRepositoryAssociationsInput{MaxResults: aws.Int32(101)},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, lerr := f.client.ListRepositoryAssociations(t.Context(), tt.in)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(lerr))

				return
			}

			require.NoError(t, lerr)
			assert.Len(t, out.RepositoryAssociationSummaries, tt.want)
		})
	}

	var names []string

	var token *string

	for range 5 {
		page, perr := f.client.ListRepositoryAssociations(t.Context(), &sdk.ListRepositoryAssociationsInput{
			MaxResults: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, perr)

		for _, s := range page.RepositoryAssociationSummaries {
			names = append(names, aws.ToString(s.Name))
		}

		if token = page.NextToken; token == nil {
			break
		}
	}

	assert.Equal(t, []string{"one", "two", "three"}, names)

	_, err = f.client.ListRepositoryAssociations(
		t.Context(),
		&sdk.ListRepositoryAssociationsInput{NextToken: aws.String("!!")},
	)
	assert.Equal(t, "ValidationException", apiCode(err))
}

func first(t *testing.T, f *fixture) *types.RepositoryAssociationSummary {
	t.Helper()

	out, err := f.client.ListRepositoryAssociations(
		t.Context(),
		&sdk.ListRepositoryAssociationsInput{Names: []string{"one"}},
	)
	require.NoError(t, err)
	require.Len(t, out.RepositoryAssociationSummaries, 1)

	return &out.RepositoryAssociationSummaries[0]
}

func TestAssociationTags(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "src", map[string]string{"a.txt": "x"})
	arn := aws.String(f.associateCodeCommit(t, "src"))

	_, err := f.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: arn, Tags: map[string]string{"a": "1", "b": "2"},
	})
	require.NoError(t, err)

	_, err = f.client.UntagResource(t.Context(), &sdk.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a"}})
	require.NoError(t, err)

	got, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, got.Tags)

	d, err := f.client.DescribeRepositoryAssociation(
		t.Context(),
		&sdk.DescribeRepositoryAssociationInput{AssociationArn: arn},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, d.Tags)

	_, err = f.client.TagResource(t.Context(), &sdk.TagResourceInput{
		ResourceArn: arn, Tags: map[string]string{"aws:reserved": "x"},
	})
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:codeguru-reviewer:us-east-1:000000000000:association:missing"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func TestAssociateRepositoryIdempotency(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.commitRepo(t, "src", map[string]string{"a.txt": "x"})
	f.commitRepo(t, "other", map[string]string{"a.txt": "x"})

	in := func(name string) *sdk.AssociateRepositoryInput {
		return &sdk.AssociateRepositoryInput{
			ClientRequestToken: aws.String("tok"),
			Repository:         &types.Repository{CodeCommit: &types.CodeCommitRepository{Name: aws.String(name)}},
		}
	}

	one, err := f.client.AssociateRepository(t.Context(), in("src"))
	require.NoError(t, err)

	two, err := f.client.AssociateRepository(t.Context(), in("src"))
	require.NoError(t, err)
	assert.Equal(t, one.RepositoryAssociation.AssociationArn, two.RepositoryAssociation.AssociationArn)

	_, err = f.client.AssociateRepository(t.Context(), in("other"))
	assert.Equal(t, "ValidationException", apiCode(err))
}
