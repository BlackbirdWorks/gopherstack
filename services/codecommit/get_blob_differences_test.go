package codecommit_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

const blobDiffRepo = "blobdiff-repo"

func putBlob(t *testing.T, c *codecommitsdk.Client, path, content string) string {
	t.Helper()

	in := &codecommitsdk.PutFileInput{
		RepositoryName: aws.String(blobDiffRepo),
		BranchName:     aws.String("main"),
		FilePath:       aws.String(path),
		FileContent:    []byte(content),
	}

	head, err := c.GetBranch(t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String(blobDiffRepo), BranchName: aws.String("main"),
	})
	if err == nil && head.Branch != nil {
		in.ParentCommitId = head.Branch.CommitId
	}

	out, err := c.PutFile(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.BlobId)
}

func newBlobDiffClient(t *testing.T) *codecommitsdk.Client {
	t.Helper()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	c := newTestCodeCommitClient(t, h)
	_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(blobDiffRepo),
	})
	require.NoError(t, err)

	return c
}

func numberedLines() string {
	var sb strings.Builder

	for _, l := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		sb.WriteString(l + "\n")
	}

	return sb.String()
}

func TestSDKRoundTrip_GetBlobDifferences(t *testing.T) {
	t.Parallel()

	const tenLines = 10

	modified := strings.Replace(numberedLines(), "e\n", "E\n", 1)
	edges := strings.Replace(strings.Replace(numberedLines(), "a\n", "A\n", 1), "j\n", "J\n", 1)

	tests := []struct {
		before    string
		after     string
		name      string
		ctx       *int32
		wantHunks []types.DiffHunk
		noBefore  bool
	}{
		{
			name:   "middle_change_default_context",
			before: numberedLines(),
			after:  modified,
			wantHunks: []types.DiffHunk{{
				BeforeStartLine: aws.Int32(2), BeforeLineCount: aws.Int32(7),
				AfterStartLine: aws.Int32(2), AfterLineCount: aws.Int32(7),
			}},
		},
		{
			name:   "middle_change_zero_context",
			before: numberedLines(),
			after:  modified,
			ctx:    aws.Int32(0),
			wantHunks: []types.DiffHunk{{
				BeforeStartLine: aws.Int32(5), BeforeLineCount: aws.Int32(1),
				AfterStartLine: aws.Int32(5), AfterLineCount: aws.Int32(1),
			}},
		},
		{
			name:     "new_file_all_additions",
			after:    numberedLines(),
			noBefore: true,
			wantHunks: []types.DiffHunk{{
				BeforeStartLine: aws.Int32(1), BeforeLineCount: aws.Int32(0),
				AfterStartLine: aws.Int32(1), AfterLineCount: aws.Int32(tenLines),
			}},
		},
		{
			name:   "distant_changes_split_into_hunks",
			before: numberedLines(),
			after:  edges,
			ctx:    aws.Int32(1),
			wantHunks: []types.DiffHunk{
				{
					BeforeStartLine: aws.Int32(1), BeforeLineCount: aws.Int32(2),
					AfterStartLine: aws.Int32(1), AfterLineCount: aws.Int32(2),
				},
				{
					BeforeStartLine: aws.Int32(9), BeforeLineCount: aws.Int32(2),
					AfterStartLine: aws.Int32(9), AfterLineCount: aws.Int32(2),
				},
			},
		},
		{
			name:   "identical_blobs_have_no_hunks",
			before: numberedLines(),
			after:  numberedLines(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newBlobDiffClient(t)

			var beforeID *string
			if !tt.noBefore {
				beforeID = aws.String(putBlob(t, c, "before.txt", tt.before))
			}

			afterID := putBlob(t, c, "after.txt", tt.after)

			out, err := c.GetBlobDifferences(t.Context(), &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo),
				BeforeBlobId:   beforeID,
				AfterBlobId:    aws.String(afterID),
				ContextLines:   tt.ctx,
			})
			require.NoError(t, err)
			assert.False(t, aws.ToBool(out.IsBinary))
			assert.Equal(t, int64(len(tt.before)), out.BeforeBlobSize)
			assert.Equal(t, int64(len(tt.after)), out.AfterBlobSize)
			require.Len(t, out.Hunks, len(tt.wantHunks))

			for i, want := range tt.wantHunks {
				got := out.Hunks[i]
				assert.Equal(t, want.BeforeStartLine, got.BeforeStartLine, "hunk %d beforeStart", i)
				assert.Equal(t, want.BeforeLineCount, got.BeforeLineCount, "hunk %d beforeCount", i)
				assert.Equal(t, want.AfterStartLine, got.AfterStartLine, "hunk %d afterStart", i)
				assert.Equal(t, want.AfterLineCount, got.AfterLineCount, "hunk %d afterCount", i)
				assert.NotEmpty(t, got.Changes)
			}
		})
	}
}

func TestSDKRoundTrip_GetBlobDifferences_Changes(t *testing.T) {
	t.Parallel()

	c := newBlobDiffClient(t)
	before := putBlob(t, c, "f.txt", "one\ntwo\nthree\n")
	after := putBlob(t, c, "g.txt", "one\nTWO\nthree\nfour\n")

	out, err := c.GetBlobDifferences(t.Context(), &codecommitsdk.GetBlobDifferencesInput{
		RepositoryName: aws.String(blobDiffRepo),
		BeforeBlobId:   aws.String(before),
		AfterBlobId:    aws.String(after),
		ContextLines:   aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Hunks, 1)

	type change struct {
		typ     types.DiffChangeType
		content string
		before  int32
		after   int32
	}

	want := []change{
		{types.DiffChangeTypeContext, "one", 1, 1},
		{types.DiffChangeTypeDelete, "two", 2, 0},
		{types.DiffChangeTypeAdd, "TWO", 0, 2},
		{types.DiffChangeTypeContext, "three", 3, 3},
		{types.DiffChangeTypeAdd, "four", 0, 4},
	}

	require.Len(t, out.Hunks[0].Changes, len(want))

	for i, w := range want {
		got := out.Hunks[0].Changes[i]
		assert.Equal(t, w.typ, got.Type, "change %d type", i)
		assert.Equal(t, w.content, aws.ToString(got.Content), "change %d content", i)
		assert.Equal(t, w.before, aws.ToInt32(got.BeforeLineNumber), "change %d before", i)
		assert.Equal(t, w.after, aws.ToInt32(got.AfterLineNumber), "change %d after", i)
	}
}

func TestSDKRoundTrip_GetBlobDifferences_IgnoreWhitespace(t *testing.T) {
	t.Parallel()

	c := newBlobDiffClient(t)
	before := putBlob(t, c, "a.txt", "a b\nc\n")
	after := putBlob(t, c, "b.txt", "a  b\nc\n")

	tests := []struct {
		name      string
		ignore    bool
		wantHunks int
	}{
		{name: "ignored", ignore: true, wantHunks: 0},
		{name: "reported", ignore: false, wantHunks: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := c.GetBlobDifferences(t.Context(), &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName:   aws.String(blobDiffRepo),
				BeforeBlobId:     aws.String(before),
				AfterBlobId:      aws.String(after),
				IgnoreWhitespace: aws.Bool(tt.ignore),
			})
			require.NoError(t, err)
			assert.Len(t, out.Hunks, tt.wantHunks)
		})
	}
}

func TestSDKRoundTrip_GetBlobDifferences_Binary(t *testing.T) {
	t.Parallel()

	c := newBlobDiffClient(t)
	before := putBlob(t, c, "a.bin", "ab\x00cd")
	after := putBlob(t, c, "b.bin", "ab\x00cde")

	out, err := c.GetBlobDifferences(t.Context(), &codecommitsdk.GetBlobDifferencesInput{
		RepositoryName: aws.String(blobDiffRepo),
		BeforeBlobId:   aws.String(before),
		AfterBlobId:    aws.String(after),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.IsBinary))
	assert.Empty(t, out.Hunks)
	assert.Equal(t, int64(5), out.BeforeBlobSize)
	assert.Equal(t, int64(6), out.AfterBlobSize)
}

func TestSDKRoundTrip_GetBlobDifferences_Pagination(t *testing.T) {
	t.Parallel()

	c := newBlobDiffClient(t)
	before := putBlob(t, c, "a.txt", numberedLines())
	edited := strings.Replace(strings.Replace(numberedLines(), "a\n", "A\n", 1), "j\n", "J\n", 1)
	after := putBlob(t, c, "b.txt", edited)

	in := &codecommitsdk.GetBlobDifferencesInput{
		RepositoryName: aws.String(blobDiffRepo),
		BeforeBlobId:   aws.String(before),
		AfterBlobId:    aws.String(after),
		ContextLines:   aws.Int32(1),
		MaxResults:     aws.Int32(1),
	}

	first, err := c.GetBlobDifferences(t.Context(), in)
	require.NoError(t, err)
	require.Len(t, first.Hunks, 1)
	require.NotNil(t, first.NextToken)
	assert.Equal(t, int32(1), aws.ToInt32(first.Hunks[0].BeforeStartLine))

	in.NextToken = first.NextToken
	second, err := c.GetBlobDifferences(t.Context(), in)
	require.NoError(t, err)
	require.Len(t, second.Hunks, 1)
	assert.Nil(t, second.NextToken)
	assert.Equal(t, int32(9), aws.ToInt32(second.Hunks[0].BeforeStartLine))
}

func TestSDKRoundTrip_GetBlobDifferences_Errors(t *testing.T) {
	t.Parallel()

	c := newBlobDiffClient(t)
	blob := putBlob(t, c, "a.txt", "x\n")
	missing := strings.Repeat("0", 40)

	tests := []struct {
		in       *codecommitsdk.GetBlobDifferencesInput
		name     string
		wantCode string
	}{
		{
			name: "unknown_repository",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String("nope"), AfterBlobId: aws.String(blob),
			},
			wantCode: "RepositoryDoesNotExistException",
		},
		{
			name: "unknown_after_blob",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo), AfterBlobId: aws.String(missing),
			},
			wantCode: "BlobIdDoesNotExistException",
		},
		{
			name: "unknown_before_blob",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(
					blobDiffRepo,
				),
				AfterBlobId:  aws.String(blob),
				BeforeBlobId: aws.String(missing),
			},
			wantCode: "BlobIdDoesNotExistException",
		},
		{
			name: "malformed_blob_id",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo), AfterBlobId: aws.String("not-a-blob"),
			},
			wantCode: "InvalidBlobIdException",
		},
		{
			name: "bad_next_token",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo), AfterBlobId: aws.String(blob), NextToken: aws.String("%%%"),
			},
			wantCode: "InvalidContinuationTokenException",
		},
		{
			name: "context_lines_out_of_range",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo), AfterBlobId: aws.String(blob), ContextLines: aws.Int32(21),
			},
			wantCode: "ValidationException",
		},
		{
			name: "max_results_out_of_range",
			in: &codecommitsdk.GetBlobDifferencesInput{
				RepositoryName: aws.String(blobDiffRepo), AfterBlobId: aws.String(blob), MaxResults: aws.Int32(-1),
			},
			wantCode: "InvalidMaxResultsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := c.GetBlobDifferences(t.Context(), tt.in)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestGetBlobDifferences_PersistedBlobs(t *testing.T) {
	t.Parallel()

	src := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	c := newTestCodeCommitClient(t, codecommit.NewHandler(src))
	_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(blobDiffRepo),
	})
	require.NoError(t, err)

	before := putBlob(t, c, "a.txt", "x\n")
	after := putBlob(t, c, "a.txt", "y\n")

	dst := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

	out, err := newTestCodeCommitClient(t, codecommit.NewHandler(dst)).GetBlobDifferences(
		t.Context(), &codecommitsdk.GetBlobDifferencesInput{
			RepositoryName: aws.String(blobDiffRepo),
			BeforeBlobId:   aws.String(before),
			AfterBlobId:    aws.String(after),
		},
	)
	require.NoError(t, err)
	assert.Len(t, out.Hunks, 1)
}
