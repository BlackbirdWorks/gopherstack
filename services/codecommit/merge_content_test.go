package codecommit_test

import (
	"encoding/base64"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mergeFixture struct {
	client *codecommitsdk.Client
	t      *testing.T
	repo   string
}

// newMergeFixture creates a repository whose main branch holds base.
func newMergeFixture(t *testing.T, base map[string]string) *mergeFixture {
	t.Helper()

	client := newTestCodeCommitClient(t, newOrphanCodeTestHandler(t))
	f := &mergeFixture{client: client, repo: "merge-repo", t: t}
	createTestRepo(t, client, f.repo)
	f.commit("main", base)

	return f
}

func (f *mergeFixture) tip(branch string) string {
	f.t.Helper()

	out, err := f.client.GetBranch(f.t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String(f.repo), BranchName: aws.String(branch),
	})
	require.NoError(f.t, err)

	return aws.ToString(out.Branch.CommitId)
}

// commit puts files (and deletes paths) on branch on top of its current tip.
func (f *mergeFixture) commit(branch string, put map[string]string, del ...string) {
	f.t.Helper()

	in := &codecommitsdk.CreateCommitInput{RepositoryName: aws.String(f.repo), BranchName: aws.String(branch)}
	if _, err := f.client.GetBranch(f.t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String(f.repo), BranchName: aws.String(branch),
	}); err == nil {
		in.ParentCommitId = aws.String(f.tip(branch))
	}

	for path, content := range put {
		in.PutFiles = append(in.PutFiles, types.PutFileEntry{FilePath: aws.String(path), FileContent: []byte(content)})
	}

	for _, path := range del {
		in.DeleteFiles = append(in.DeleteFiles, types.DeleteFileEntry{FilePath: aws.String(path)})
	}

	_, err := f.client.CreateCommit(f.t.Context(), in)
	require.NoError(f.t, err)
}

func (f *mergeFixture) branchFeature() {
	f.t.Helper()

	_, err := f.client.CreateBranch(f.t.Context(), &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String(f.repo), BranchName: aws.String("feature"), CommitId: aws.String(f.tip("main")),
	})
	require.NoError(f.t, err)
}

func (f *mergeFixture) content(spec, path string) string {
	f.t.Helper()

	out, err := f.client.GetFile(f.t.Context(), &codecommitsdk.GetFileInput{
		RepositoryName: aws.String(f.repo), CommitSpecifier: aws.String(spec), FilePath: aws.String(path),
	})
	require.NoError(f.t, err)

	return string(out.FileContent)
}

func (f *mergeFixture) paths(spec string) []string {
	f.t.Helper()

	out, err := f.client.GetFolder(f.t.Context(), &codecommitsdk.GetFolderInput{
		RepositoryName: aws.String(f.repo), CommitSpecifier: aws.String(spec), FolderPath: aws.String("/"),
	})
	if err != nil {
		out, err = f.client.GetFolder(f.t.Context(), &codecommitsdk.GetFolderInput{
			RepositoryName: aws.String(f.repo), CommitSpecifier: aws.String(spec), FolderPath: aws.String(""),
		})
	}

	require.NoError(f.t, err)

	paths := make([]string, 0, len(out.Files))
	for _, file := range out.Files {
		paths = append(paths, aws.ToString(file.AbsolutePath))
	}

	sort.Strings(paths)

	return paths
}

func (f *mergeFixture) conflicts(
	detail types.ConflictDetailLevelTypeEnum,
	strategy types.ConflictResolutionStrategyTypeEnum,
) *codecommitsdk.GetMergeConflictsOutput {
	f.t.Helper()

	out, err := f.client.GetMergeConflicts(f.t.Context(), &codecommitsdk.GetMergeConflictsInput{
		RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
		ConflictDetailLevel: detail, ConflictResolutionStrategy: strategy,
	})
	require.NoError(f.t, err)

	return out
}

func (f *mergeFixture) threeWay(
	in *codecommitsdk.MergeBranchesByThreeWayInput,
) (*codecommitsdk.MergeBranchesByThreeWayOutput, error) {
	f.t.Helper()

	in.RepositoryName = aws.String(f.repo)
	in.SourceCommitSpecifier = aws.String("feature")
	in.DestinationCommitSpecifier = aws.String("main")

	return f.client.MergeBranchesByThreeWay(f.t.Context(), in)
}

const (
	lines5      = "one\ntwo\nthree\nfour\nfive\n"
	lines5Top   = "ONE\ntwo\nthree\nfour\nfive\n"
	lines5Bot   = "one\ntwo\nthree\nfour\nFIVE\n"
	lines5Both  = "ONE\ntwo\nthree\nfour\nFIVE\n"
	lines5Dest  = "one\ntwo\nDEST\nfour\nfive\n"
	lines5Src   = "one\ntwo\nSRC\nfour\nfive\n"
	fileLevel   = types.ConflictDetailLevelTypeEnumFileLevel
	lineLevel   = types.ConflictDetailLevelTypeEnumLineLevel
	strategyNon = types.ConflictResolutionStrategyTypeEnumNone
)

func TestMergeBranches_ThreeWayContent_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		base         map[string]string
		mainPut      map[string]string
		featPut      map[string]string
		resolution   *types.ConflictResolution
		wantFiles    map[string]string
		name         string
		detail       types.ConflictDetailLevelTypeEnum
		strategy     types.ConflictResolutionStrategyTypeEnum
		wantMergeErr string
		mainDel      []string
		featDel      []string
	}{
		{
			name:      "disjoint_files_merge",
			base:      map[string]string{"a.txt": "a"},
			mainPut:   map[string]string{"b.txt": "b"},
			featPut:   map[string]string{"c.txt": "c"},
			wantFiles: map[string]string{"a.txt": "a", "b.txt": "b", "c.txt": "c"},
		},
		{
			name:         "same_file_file_level_conflicts",
			base:         map[string]string{"a.txt": lines5},
			mainPut:      map[string]string{"a.txt": lines5Top},
			featPut:      map[string]string{"a.txt": lines5Bot},
			detail:       fileLevel,
			wantMergeErr: "ManualMergeRequiredException",
		},
		{
			name:      "same_file_line_level_merges_disjoint_lines",
			base:      map[string]string{"a.txt": lines5},
			mainPut:   map[string]string{"a.txt": lines5Top},
			featPut:   map[string]string{"a.txt": lines5Bot},
			detail:    lineLevel,
			wantFiles: map[string]string{"a.txt": lines5Both},
		},
		{
			name:         "same_line_line_level_conflicts",
			base:         map[string]string{"a.txt": lines5},
			mainPut:      map[string]string{"a.txt": lines5Dest},
			featPut:      map[string]string{"a.txt": lines5Src},
			detail:       lineLevel,
			wantMergeErr: "ManualMergeRequiredException",
		},
		{
			name:      "accept_source_file_level",
			base:      map[string]string{"a.txt": lines5},
			mainPut:   map[string]string{"a.txt": lines5Dest},
			featPut:   map[string]string{"a.txt": lines5Src},
			strategy:  types.ConflictResolutionStrategyTypeEnumAcceptSource,
			wantFiles: map[string]string{"a.txt": lines5Src},
		},
		{
			name:      "accept_destination_file_level",
			base:      map[string]string{"a.txt": lines5},
			mainPut:   map[string]string{"a.txt": lines5Dest},
			featPut:   map[string]string{"a.txt": lines5Src},
			strategy:  types.ConflictResolutionStrategyTypeEnumAcceptDestination,
			wantFiles: map[string]string{"a.txt": lines5Dest},
		},
		{
			name:      "accept_source_line_level_keeps_clean_hunks",
			base:      map[string]string{"a.txt": lines5},
			mainPut:   map[string]string{"a.txt": "ONE\ntwo\nDEST\nfour\nfive\n"},
			featPut:   map[string]string{"a.txt": "one\ntwo\nSRC\nfour\nFIVE\n"},
			detail:    lineLevel,
			strategy:  types.ConflictResolutionStrategyTypeEnumAcceptSource,
			wantFiles: map[string]string{"a.txt": "ONE\ntwo\nSRC\nfour\nFIVE\n"},
		},
		{
			name:     "automerge_with_new_content",
			base:     map[string]string{"a.txt": lines5},
			mainPut:  map[string]string{"a.txt": lines5Dest},
			featPut:  map[string]string{"a.txt": lines5Src},
			strategy: types.ConflictResolutionStrategyTypeEnumAutomerge,
			resolution: &types.ConflictResolution{ReplaceContents: []types.ReplaceContentEntry{
				{
					FilePath: aws.String(
						"a.txt",
					),
					ReplacementType: types.ReplacementTypeEnumUseNewContent,
					Content:         []byte("custom\n"),
				},
			}},
			wantFiles: map[string]string{"a.txt": "custom\n"},
		},
		{
			name:     "automerge_keep_base",
			base:     map[string]string{"a.txt": lines5},
			mainPut:  map[string]string{"a.txt": lines5Dest},
			featPut:  map[string]string{"a.txt": lines5Src},
			strategy: types.ConflictResolutionStrategyTypeEnumAutomerge,
			resolution: &types.ConflictResolution{ReplaceContents: []types.ReplaceContentEntry{{
				FilePath: aws.String("a.txt"), ReplacementType: types.ReplacementTypeEnumKeepBase,
			}}},
			wantFiles: map[string]string{"a.txt": lines5},
		},
		{
			name:    "resolution_delete_file",
			base:    map[string]string{"a.txt": lines5, "keep.txt": "k"},
			mainPut: map[string]string{"a.txt": lines5Dest},
			featPut: map[string]string{"a.txt": lines5Src},
			resolution: &types.ConflictResolution{
				DeleteFiles: []types.DeleteFileEntry{{FilePath: aws.String("a.txt")}},
			},
			wantFiles: map[string]string{"keep.txt": "k"},
		},
		{
			name:         "delete_vs_modify_conflicts",
			base:         map[string]string{"a.txt": "a", "k.txt": "k"},
			mainPut:      map[string]string{"a.txt": "changed"},
			featDel:      []string{"a.txt"},
			wantMergeErr: "ManualMergeRequiredException",
		},
		{
			name:      "delete_vs_modify_accept_source_deletes",
			base:      map[string]string{"a.txt": "a", "k.txt": "k"},
			mainPut:   map[string]string{"a.txt": "changed"},
			featDel:   []string{"a.txt"},
			strategy:  types.ConflictResolutionStrategyTypeEnumAcceptSource,
			wantFiles: map[string]string{"k.txt": "k"},
		},
		{
			name:      "one_sided_delete_is_clean",
			base:      map[string]string{"a.txt": "a", "k.txt": "k"},
			featDel:   []string{"a.txt"},
			mainPut:   map[string]string{"z.txt": "z"},
			wantFiles: map[string]string{"k.txt": "k", "z.txt": "z"},
		},
		{
			name:      "add_add_identical_is_clean",
			base:      map[string]string{"k.txt": "k"},
			mainPut:   map[string]string{"n.txt": "same"},
			featPut:   map[string]string{"n.txt": "same"},
			wantFiles: map[string]string{"k.txt": "k", "n.txt": "same"},
		},
		{
			name:         "add_add_different_conflicts",
			base:         map[string]string{"k.txt": "k"},
			mainPut:      map[string]string{"n.txt": "one"},
			featPut:      map[string]string{"n.txt": "two"},
			wantMergeErr: "ManualMergeRequiredException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, tc.base)
			f.branchFeature()
			f.commit("main", tc.mainPut, tc.mainDel...)
			f.commit("feature", tc.featPut, tc.featDel...)

			out, err := f.threeWay(&codecommitsdk.MergeBranchesByThreeWayInput{
				ConflictDetailLevel: tc.detail, ConflictResolutionStrategy: tc.strategy,
				ConflictResolution: tc.resolution, TargetBranch: aws.String("main"),
			})
			if tc.wantMergeErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantMergeErr)

				return
			}

			require.NoError(t, err)

			commit, err := f.client.GetCommit(t.Context(), &codecommitsdk.GetCommitInput{
				RepositoryName: aws.String(f.repo), CommitId: out.CommitId,
			})
			require.NoError(t, err)
			require.Len(t, commit.Commit.Parents, 2)

			var want []string
			for p, c := range tc.wantFiles {
				want = append(want, p)
				assert.Equal(t, c, f.content("main", p), p)
			}

			sort.Strings(want)
			assert.Equal(t, want, f.paths("main"))
		})
	}
}

func TestGetMergeConflicts_Consistency_RealClient(t *testing.T) {
	t.Parallel()

	f := newMergeFixture(t, map[string]string{"a.txt": lines5, "ok.txt": "k"})
	f.branchFeature()
	f.commit("main", map[string]string{"a.txt": lines5Dest})
	f.commit("feature", map[string]string{"a.txt": lines5Src, "new.txt": "n"})

	fileOut := f.conflicts(fileLevel, strategyNon)
	assert.False(t, fileOut.Mergeable)
	require.Len(t, fileOut.ConflictMetadataList, 1)

	meta := fileOut.ConflictMetadataList[0]
	assert.Equal(t, "a.txt", aws.ToString(meta.FilePath))
	assert.True(t, meta.ContentConflict)
	assert.False(t, meta.FileModeConflict)
	assert.Equal(t, int32(1), meta.NumberOfConflicts)
	assert.Equal(t, int64(len(lines5)), meta.FileSizes.Base)
	assert.Equal(t, types.ChangeTypeEnumModified, meta.MergeOperations.Source)
	assert.Equal(t, types.ChangeTypeEnumModified, meta.MergeOperations.Destination)
	assert.NotEmpty(t, aws.ToString(fileOut.BaseCommitId))

	options, err := f.client.GetMergeOptions(t.Context(), &codecommitsdk.GetMergeOptionsInput{
		RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
	})
	require.NoError(t, err)
	assert.Empty(t, options.MergeOptions, "a conflicting diverged merge offers no strategy")

	described, err := f.client.DescribeMergeConflicts(t.Context(), &codecommitsdk.DescribeMergeConflictsInput{
		RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
		FilePath: aws.String("a.txt"), ConflictDetailLevel: lineLevel,
	})
	require.NoError(t, err)
	require.Len(t, described.MergeHunks, 1)
	assert.True(t, described.MergeHunks[0].IsConflict)
	assert.Equal(t, int32(3), aws.ToInt32(described.MergeHunks[0].Source.StartLine))
	assert.Equal(
		t,
		base64.StdEncoding.EncodeToString([]byte("SRC\n")),
		aws.ToString(described.MergeHunks[0].Source.HunkContent),
	)
	assert.Equal(
		t,
		base64.StdEncoding.EncodeToString([]byte("DEST\n")),
		aws.ToString(described.MergeHunks[0].Destination.HunkContent),
	)
	assert.Equal(
		t,
		base64.StdEncoding.EncodeToString([]byte("three\n")),
		aws.ToString(described.MergeHunks[0].Base.HunkContent),
	)
	assert.True(t, described.ConflictMetadata.ContentConflict)

	batch, err := f.client.BatchDescribeMergeConflicts(t.Context(), &codecommitsdk.BatchDescribeMergeConflictsInput{
		RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
		FilePaths: []string{"a.txt", "ok.txt", "ghost.txt"},
	})
	require.NoError(t, err)
	require.Len(t, batch.Conflicts, 2)
	assert.True(t, batch.Conflicts[0].ConflictMetadata.ContentConflict)
	assert.False(t, batch.Conflicts[1].ConflictMetadata.ContentConflict)
	require.Len(t, batch.Errors, 1)
	assert.Equal(t, "ghost.txt", aws.ToString(batch.Errors[0].FilePath))
	assert.Equal(t, "FileDoesNotExistException", aws.ToString(batch.Errors[0].ExceptionName))

	cleaned := f.conflicts(lineLevel, types.ConflictResolutionStrategyTypeEnumAcceptSource)
	assert.True(t, cleaned.Mergeable)
	assert.Empty(t, cleaned.ConflictMetadataList)
}

func TestMergeBranches_SquashHasOneParent_RealClient(t *testing.T) {
	t.Parallel()

	f := newMergeFixture(t, map[string]string{"a.txt": "a"})
	f.branchFeature()
	f.commit("feature", map[string]string{"b.txt": "b"})

	out, err := f.client.MergeBranchesBySquash(t.Context(), &codecommitsdk.MergeBranchesBySquashInput{
		RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
	})
	require.NoError(t, err)

	commit, err := f.client.GetCommit(t.Context(), &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(f.repo), CommitId: out.CommitId,
	})
	require.NoError(t, err)
	assert.Len(t, commit.Commit.Parents, 1)
	assert.Equal(t, "b", f.content("main", "b.txt"))
}

func TestMergeBranches_KeepEmptyFolders_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		wantPaths []string
		keep      bool
	}{
		{name: "default_drops_folder", wantPaths: []string{"root.txt", "side.txt"}},
		{name: "keep_adds_gitkeep", keep: true, wantPaths: []string{"dir/.gitkeep", "root.txt", "side.txt"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, map[string]string{"dir/only.txt": "x", "root.txt": "r"})
			f.branchFeature()
			f.commit("main", map[string]string{"side.txt": "s"})
			f.commit("feature", nil, "dir/only.txt")

			_, err := f.threeWay(&codecommitsdk.MergeBranchesByThreeWayInput{
				KeepEmptyFolders: tc.keep, TargetBranch: aws.String("main"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantPaths, f.paths("main"))
		})
	}
}

func TestMergeBranches_FileModeConflict_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		strategy types.ConflictResolutionStrategyTypeEnum
		wantErr  bool
	}{
		{name: "unresolved", wantErr: true},
		{name: "accept_source", strategy: types.ConflictResolutionStrategyTypeEnumAcceptSource},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, map[string]string{"run.sh": "echo"})
			f.branchFeature()

			for branch, mode := range map[string]types.FileModeTypeEnum{
				"main": types.FileModeTypeEnumExecutable, "feature": types.FileModeTypeEnumSymlink,
			} {
				_, err := f.client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
					RepositoryName: aws.String(f.repo), BranchName: aws.String(branch),
					ParentCommitId: aws.String(f.tip(branch)),
					PutFiles: []types.PutFileEntry{{
						FilePath: aws.String("run.sh"), FileContent: []byte("echo hi"), FileMode: mode,
					}},
				})
				require.NoError(t, err)
			}

			conflicts := f.conflicts(fileLevel, tc.strategy)
			assert.Equal(t, !tc.wantErr, conflicts.Mergeable)

			if tc.wantErr {
				require.Len(t, conflicts.ConflictMetadataList, 1)
				assert.True(t, conflicts.ConflictMetadataList[0].FileModeConflict)
				assert.False(t, conflicts.ConflictMetadataList[0].ContentConflict)
			}
		})
	}
}

func TestMergeBranches_InvalidConflictInputs_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   *codecommitsdk.MergeBranchesByThreeWayInput
		want string
	}{
		{
			name: "bad_detail_level",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{
				ConflictDetailLevel: types.ConflictDetailLevelTypeEnum("WORD_LEVEL"),
			},
			want: "InvalidConflictDetailLevelException",
		},
		{
			name: "bad_strategy",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{
				ConflictResolutionStrategy: types.ConflictResolutionStrategyTypeEnum("COIN_FLIP"),
			},
			want: "InvalidConflictResolutionStrategyException",
		},
		{
			name: "bad_replacement_type",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{ConflictResolution: &types.ConflictResolution{
				ReplaceContents: []types.ReplaceContentEntry{{
					FilePath: aws.String("a"), ReplacementType: types.ReplacementTypeEnum("KEEP_BOTH"),
				}},
			}},
			want: "InvalidReplacementTypeException",
		},
		{
			name: "new_content_required",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{ConflictResolution: &types.ConflictResolution{
				ReplaceContents: []types.ReplaceContentEntry{{
					FilePath: aws.String("a"), ReplacementType: types.ReplacementTypeEnumUseNewContent,
				}},
			}},
			want: "ReplacementContentRequiredException",
		},
		{
			name: "duplicate_entries",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{ConflictResolution: &types.ConflictResolution{
				DeleteFiles: []types.DeleteFileEntry{{FilePath: aws.String("a")}, {FilePath: aws.String("a")}},
			}},
			want: "MultipleConflictResolutionEntriesException",
		},
		{
			name: "bad_file_mode",
			in: &codecommitsdk.MergeBranchesByThreeWayInput{ConflictResolution: &types.ConflictResolution{
				SetFileModes: []types.SetFileModeEntry{
					{FilePath: aws.String("a"), FileMode: types.FileModeTypeEnum("WEIRD")},
				},
			}},
			want: "InvalidFileModeException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, map[string]string{"a": "a"})
			f.branchFeature()
			f.commit("feature", map[string]string{"b": "b"})

			_, err := f.threeWay(tc.in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestGetMergeConflicts_Paging_RealClient(t *testing.T) {
	t.Parallel()

	f := newMergeFixture(t, map[string]string{"a": "1", "b": "1", "c": "1"})
	f.branchFeature()
	f.commit("main", map[string]string{"a": "m", "b": "m", "c": "m"})
	f.commit("feature", map[string]string{"a": "f", "b": "f", "c": "f"})

	var got []string

	token := (*string)(nil)

	for range 5 {
		out, err := f.client.GetMergeConflicts(t.Context(), &codecommitsdk.GetMergeConflictsInput{
			RepositoryName: aws.String(f.repo), SourceCommitSpecifier: aws.String("feature"),
			DestinationCommitSpecifier: aws.String("main"), MergeOption: types.MergeOptionTypeEnumThreeWayMerge,
			MaxConflictFiles: aws.Int32(2), NextToken: token,
		})
		require.NoError(t, err)

		for _, m := range out.ConflictMetadataList {
			got = append(got, aws.ToString(m.FilePath))
		}

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	assert.Equal(t, []string{"a", "b", "c"}, got)
}

func TestMergePullRequest_Conflicts_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		strategy types.ConflictResolutionStrategyTypeEnum
		wantErr  bool
	}{
		{name: "unresolved_conflict", wantErr: true},
		{name: "accept_source", strategy: types.ConflictResolutionStrategyTypeEnumAcceptSource},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMergeFixture(t, map[string]string{"a.txt": lines5})
			f.branchFeature()
			f.commit("main", map[string]string{"a.txt": lines5Dest})
			f.commit("feature", map[string]string{"a.txt": lines5Src})

			pr, err := f.client.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
				Title: aws.String("t"),
				Targets: []types.Target{{
					RepositoryName: aws.String(f.repo), SourceReference: aws.String("feature"),
					DestinationReference: aws.String("main"),
				}},
			})
			require.NoError(t, err)

			_, err = f.client.MergePullRequestByThreeWay(t.Context(), &codecommitsdk.MergePullRequestByThreeWayInput{
				PullRequestId: pr.PullRequest.PullRequestId, RepositoryName: aws.String(f.repo),
				ConflictResolutionStrategy: tc.strategy,
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ManualMergeRequiredException")

				open, getErr := f.client.GetPullRequest(t.Context(), &codecommitsdk.GetPullRequestInput{
					PullRequestId: pr.PullRequest.PullRequestId,
				})
				require.NoError(t, getErr)
				assert.Equal(t, types.PullRequestStatusEnumOpen, open.PullRequest.PullRequestStatus)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, lines5Src, f.content("main", "a.txt"))
		})
	}
}
