package azureblob_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tagMap(set []*blob.Tags) map[string]string {
	out := make(map[string]string, len(set))
	for _, t := range set {
		out[*t.Key] = *t.Value
	}

	return out
}

func uploadTagged(t *testing.T, env *sdkEnv, name string, tags map[string]string) *blockblob.Client {
	t.Helper()

	bb := env.cc.NewBlockBlobClient(name)
	_, err := bb.UploadBuffer(t.Context(), []byte("data"), &blockblob.UploadBufferOptions{Tags: tags})
	require.NoError(t, err)

	return bb
}

func requireTags(t *testing.T, bc *blob.Client, want map[string]string) {
	t.Helper()

	got, err := bc.GetTags(t.Context(), nil)
	require.NoError(t, err)
	assert.Equal(t, want, tagMap(got.BlobTagSet))
}

func manyTags(n int) map[string]string {
	m := make(map[string]string, n)
	for i := range n {
		m["k"+strings.Repeat("x", i)] = "v"
	}

	return m
}

func TestSDK_Tags_SetGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		initial  map[string]string
		set      map[string]string
		wantCode bloberror.Code
	}{
		{name: "set_two", set: map[string]string{"env": "prod", "team": "a b+c-d.e/f:g=h_i"}},
		{name: "replace_all", initial: map[string]string{"old": "1"}, set: map[string]string{"new": "2"}},
		{name: "clear_with_empty_set", initial: map[string]string{"old": "1"}, set: map[string]string{}},
		{name: "empty_value_ok", set: map[string]string{"k": ""}},
		{name: "ten_tags_ok", set: manyTags(10)},
		{name: "eleven_tags", set: manyTags(11), wantCode: "TagsTooLarge"},
		{name: "bad_key_char", set: map[string]string{"bad!": "v"}, wantCode: "InvalidTag"},
		{name: "bad_value_char", set: map[string]string{"k": "v@"}, wantCode: "InvalidTag"},
		{name: "key_too_long", set: map[string]string{strings.Repeat("k", 129): "v"}, wantCode: "InvalidTag"},
		{name: "key_max_ok", set: map[string]string{strings.Repeat("k", 128): "v"}},
		{name: "value_too_long", set: map[string]string{"k": strings.Repeat("v", 257)}, wantCode: "InvalidTag"},
		{name: "value_max_ok", set: map[string]string{"k": strings.Repeat("v", 256)}},
		{name: "empty_key", set: map[string]string{"": "v"}, wantCode: "InvalidTag"},
		{name: "case_sensitive_keys", set: map[string]string{"Env": "a", "env": "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb := uploadTagged(t, env, "b.txt", tt.initial)
			bc := bb.BlobClient()

			_, err := bc.SetTags(t.Context(), tt.set, nil)
			if tt.wantCode != "" {
				requireStorageErr(t, err, tt.wantCode, 400)
				requireTags(t, bc, orEmpty(tt.initial))

				return
			}

			require.NoError(t, err)
			requireTags(t, bc, tt.set)
		})
	}
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return m
}

func TestSDK_Tags_SetKeepsETagAndLastModified(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb := uploadTagged(t, env, "b.txt", nil)

	before, err := bb.GetProperties(t.Context(), nil)
	require.NoError(t, err)

	env.clock.Advance(3600e9)

	_, err = bb.BlobClient().SetTags(t.Context(), map[string]string{"a": "b"}, nil)
	require.NoError(t, err)

	after, err := bb.GetProperties(t.Context(), nil)
	require.NoError(t, err)
	assert.Equal(t, *before.ETag, *after.ETag)
	assert.Equal(t, *before.LastModified, *after.LastModified)
}

func TestSDK_Tags_MissingBlob(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bc := env.cc.NewBlobClient("nope")

	_, err := bc.SetTags(t.Context(), map[string]string{"a": "b"}, nil)
	requireStorageErr(t, err, bloberror.BlobNotFound, 404)

	_, err = bc.GetTags(t.Context(), nil)
	requireStorageErr(t, err, bloberror.BlobNotFound, 404)
}

func TestSDK_Tags_TagCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
		want int32
	}{
		{name: "none", tags: nil, want: 0},
		{name: "one", tags: map[string]string{"a": "1"}, want: 1},
		{name: "three", tags: map[string]string{"a": "1", "b": "2", "c": "3"}, want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb := uploadTagged(t, env, "b.txt", tt.tags)

			props, err := bb.GetProperties(t.Context(), nil)
			require.NoError(t, err)

			if tt.want == 0 {
				assert.Nil(t, props.TagCount)
			} else {
				require.NotNil(t, props.TagCount)
				assert.Equal(t, tt.want, int32(*props.TagCount))
			}

			dl, err := bb.DownloadStream(t.Context(), nil)
			require.NoError(t, err)

			defer dl.Body.Close()

			if tt.want == 0 {
				assert.Nil(t, dl.TagCount)
			} else {
				require.NotNil(t, dl.TagCount)
				assert.Equal(t, tt.want, int32(*dl.TagCount))
			}
		})
	}
}

func TestSDK_Tags_WriteOps(t *testing.T) {
	t.Parallel()

	t.Run("put_blob_overwrite_clears", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		bb := uploadTagged(t, env, "b.txt", map[string]string{"a": "1"})
		_, err := bb.UploadBuffer(t.Context(), []byte("new"), nil)
		require.NoError(t, err)
		requireTags(t, bb.BlobClient(), map[string]string{})
	})

	t.Run("put_blob_overwrite_sets", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		bb := uploadTagged(t, env, "b.txt", map[string]string{"a": "1"})
		_, err := bb.UploadBuffer(t.Context(), []byte("new"),
			&blockblob.UploadBufferOptions{Tags: map[string]string{"b": "2"}})
		require.NoError(t, err)
		requireTags(t, bb.BlobClient(), map[string]string{"b": "2"})
	})

	t.Run("put_blob_invalid_tags", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		_, err := env.cc.NewBlockBlobClient("b.txt").UploadBuffer(t.Context(), []byte("x"),
			&blockblob.UploadBufferOptions{Tags: map[string]string{"bad!": "v"}})
		requireStorageErr(t, err, "InvalidTag", 400)
	})

	t.Run("put_block_list", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		bb := env.cc.NewBlockBlobClient("blocks.txt")
		id := "YmxvY2swMDAx"

		_, err := bb.StageBlock(t.Context(), id, body("hello"), nil)
		require.NoError(t, err)

		_, err = bb.CommitBlockList(t.Context(), []string{id},
			&blockblob.CommitBlockListOptions{Tags: map[string]string{"stage": "done"}})
		require.NoError(t, err)
		requireTags(t, bb.BlobClient(), map[string]string{"stage": "done"})
	})

	t.Run("copy_does_not_inherit", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		src := uploadTagged(t, env, "src.txt", map[string]string{"a": "1"})
		dst := env.cc.NewBlockBlobClient("dst.txt")

		_, err := dst.StartCopyFromURL(t.Context(), src.URL(), nil)
		require.NoError(t, err)
		requireTags(t, dst.BlobClient(), map[string]string{})
	})

	t.Run("copy_sets_given_tags", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		src := uploadTagged(t, env, "src.txt", map[string]string{"a": "1"})
		dst := env.cc.NewBlockBlobClient("dst.txt")

		_, err := dst.StartCopyFromURL(t.Context(), src.URL(),
			&blob.StartCopyFromURLOptions{BlobTags: map[string]string{"copied": "yes"}})
		require.NoError(t, err)
		requireTags(t, dst.BlobClient(), map[string]string{"copied": "yes"})
	})

	t.Run("snapshot_keeps_tags_and_is_independent", func(t *testing.T) {
		t.Parallel()

		env := newSDKEnv(t)
		bb := uploadTagged(t, env, "b.txt", map[string]string{"a": "1"})

		snap, err := bb.BlobClient().CreateSnapshot(t.Context(), nil)
		require.NoError(t, err)

		_, err = bb.BlobClient().SetTags(t.Context(), map[string]string{"a": "2"}, nil)
		require.NoError(t, err)

		sc, err := bb.BlobClient().WithSnapshot(*snap.Snapshot)
		require.NoError(t, err)
		requireTags(t, sc, map[string]string{"a": "1"})
		requireTags(t, bb.BlobClient(), map[string]string{"a": "2"})

		_, err = sc.SetTags(t.Context(), map[string]string{"s": "1"}, nil)
		require.NoError(t, err)
		requireTags(t, sc, map[string]string{"s": "1"})
		requireTags(t, bb.BlobClient(), map[string]string{"a": "2"})
	})
}

func ifTags(expr string) *blob.AccessConditions {
	return &blob.AccessConditions{ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfTags: &expr}}
}

func TestSDK_Tags_IfTagsEvaluation(t *testing.T) {
	t.Parallel()

	tags := map[string]string{"env": "prod", "Rank": "050", "team": "core"}

	tests := []struct {
		name     string
		expr     string
		wantCode bloberror.Code
		want     int
	}{
		{name: "eq_match", expr: `"env" = 'prod'`, want: 200},
		{name: "eq_mismatch", expr: `"env" = 'dev'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "unquoted_identifier", expr: `env = 'prod'`, want: 200},
		{name: "key_case_sensitive", expr: `"ENV" = 'prod'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "value_case_sensitive", expr: `"env" = 'PROD'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "ne_true", expr: `"env" <> 'dev'`, want: 200},
		{name: "ne_false", expr: `"env" <> 'prod'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "gt_true", expr: `"Rank" > '010'`, want: 200},
		{name: "gt_false", expr: `"Rank" > '050'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "ge_true", expr: `"Rank" >= '050'`, want: 200},
		{name: "lt_true", expr: `"Rank" < '100'`, want: 200},
		{name: "le_false", expr: `"Rank" <= '049'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "and_true", expr: `"env" = 'prod' AND "team" = 'core'`, want: 200},
		{name: "and_false", expr: `"env" = 'prod' AND "team" = 'x'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "and_lowercase", expr: `"env" = 'prod' and "team" = 'core'`, want: 200},
		{name: "or_true", expr: `"env" = 'dev' OR "team" = 'core'`, want: 200},
		{name: "or_false", expr: `"env" = 'dev' OR "team" = 'x'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "parens", expr: `("env" = 'dev' OR "env" = 'prod') AND "team" = 'core'`, want: 200},
		{name: "precedence", expr: `"env" = 'dev' AND "team" = 'x' OR "team" = 'core'`, want: 200},
		{name: "absent_key", expr: `"missing" = 'x'`, want: 412, wantCode: bloberror.ConditionNotMet},
		{name: "malformed_no_value", expr: `"env" =`, want: 400, wantCode: bloberror.InvalidHeaderValue},
		{name: "malformed_unquoted_value", expr: `"env" = prod`, want: 400, wantCode: bloberror.InvalidHeaderValue},
		{name: "malformed_unbalanced", expr: `("env" = 'prod'`, want: 400, wantCode: bloberror.InvalidHeaderValue},
		{name: "too_many_ops", expr: strings.TrimSuffix(strings.Repeat(`"env" = 'prod' AND `, 12), " AND "),
			want: 400, wantCode: bloberror.InvalidHeaderValue},
		{name: "ten_ops_ok", expr: strings.TrimSuffix(strings.Repeat(`"env" = 'prod' AND `, 11), " AND "), want: 200},
	}

	ops := map[string]func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error{
		"properties": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.GetProperties(t.Context(), &blob.GetPropertiesOptions{AccessConditions: ac})

			return err
		},
		"download": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			resp, err := bb.DownloadStream(t.Context(), &blob.DownloadStreamOptions{AccessConditions: ac})
			if err == nil {
				_ = resp.Body.Close()
			}

			return err
		},
		"set_metadata": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.SetMetadata(t.Context(), map[string]*string{"k": new("v")},
				&blob.SetMetadataOptions{AccessConditions: ac})

			return err
		},
		"set_tags": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.SetTags(t.Context(), tags, &blob.SetTagsOptions{AccessConditions: ac})

			return err
		},
		"get_tags": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.GetTags(t.Context(), &blob.GetTagsOptions{BlobAccessConditions: ac})

			return err
		},
		"snapshot": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.CreateSnapshot(t.Context(), &blob.CreateSnapshotOptions{AccessConditions: ac})

			return err
		},
		"upload_overwrite": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.Upload(t.Context(), body("x"), &blockblob.UploadOptions{AccessConditions: ac, Tags: tags})

			return err
		},
		"delete": func(t *testing.T, bb *blockblob.Client, ac *blob.AccessConditions) error {
			t.Helper()
			_, err := bb.Delete(t.Context(), &blob.DeleteOptions{AccessConditions: ac})

			return err
		},
	}

	for op, run := range ops {
		for _, tt := range tests {
			t.Run(op+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				env := newSDKEnv(t)
				bb := uploadTagged(t, env, "b.txt", tags)

				err := run(t, bb, ifTags(tt.expr))
				if tt.want == 200 {
					require.NoError(t, err)

					return
				}

				if op == "properties" {
					requireStatus(t, err, tt.want)

					return
				}

				requireStorageErr(t, err, tt.wantCode, tt.want)
			})
		}
	}
}

func TestSDK_Tags_IfTagsOnMissingBlobAndNoTags(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)

	_, err := env.cc.NewBlockBlobClient("new.txt").Upload(t.Context(), body("x"),
		&blockblob.UploadOptions{AccessConditions: ifTags(`"a" = 'b'`)})
	requireStorageErr(t, err, bloberror.ConditionNotMet, 412)

	bb := uploadTagged(t, env, "plain.txt", nil)
	_, err = bb.GetProperties(t.Context(), &blob.GetPropertiesOptions{AccessConditions: ifTags(`"a" = 'b'`)})
	requireStatus(t, err, 412)
}

func TestSDK_Tags_IfTagsReflectsLatestSet(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb := uploadTagged(t, env, "b.txt", map[string]string{"state": "new"})

	_, err := bb.SetMetadata(t.Context(), nil, &blob.SetMetadataOptions{AccessConditions: ifTags(`"state" = 'old'`)})
	requireStorageErr(t, err, bloberror.ConditionNotMet, 412)

	_, err = bb.BlobClient().SetTags(t.Context(), map[string]string{"state": "old"},
		&blob.SetTagsOptions{AccessConditions: ifTags(`"state" = 'new'`)})
	require.NoError(t, err)

	_, err = bb.SetMetadata(t.Context(), nil, &blob.SetMetadataOptions{AccessConditions: ifTags(`"state" = 'old'`)})
	require.NoError(t, err)
}

func TestSDK_Tags_SourceIfTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		expr string
		want bloberror.Code
	}{
		{name: "match", expr: `"env" = 'prod'`},
		{name: "mismatch", expr: `"env" = 'dev'`, want: bloberror.SourceConditionNotMet},
		{name: "or", expr: `"env" = 'dev' OR "env" = 'prod'`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			src := uploadTagged(t, env, "src.txt", map[string]string{"env": "prod"})
			dst := env.cc.NewBlockBlobClient("dst.txt")

			_, err := dst.StartCopyFromURL(t.Context(), src.URL(), &blob.StartCopyFromURLOptions{
				SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{SourceIfTags: &tt.expr},
			})
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, tt.want, 412)
		})
	}
}

func seedFindEnv(t *testing.T) *sdkEnv {
	t.Helper()

	env := newSDKEnv(t)
	other := env.svc.ServiceClient().NewContainerClient("other")
	_, err := other.Create(t.Context(), nil)
	require.NoError(t, err)

	uploadTagged(t, env, "a.txt", map[string]string{"env": "prod", "Rank": "010"})
	uploadTagged(t, env, "b.txt", map[string]string{"env": "dev", "Rank": "050"})
	uploadTagged(t, env, "c.txt", map[string]string{"env": "prod", "Rank": "090"})
	uploadTagged(t, env, "untagged.txt", nil)

	_, err = other.NewBlockBlobClient("o.txt").UploadBuffer(t.Context(), []byte("x"),
		&blockblob.UploadBufferOptions{Tags: map[string]string{"env": "prod", "Rank": "020"}})
	require.NoError(t, err)

	return env
}

func foundNames(blobs []*service.FilterBlobItem) []string {
	names := make([]string, 0, len(blobs))
	for _, b := range blobs {
		names = append(names, *b.ContainerName+"/"+*b.Name)
	}

	return names
}

func TestSDK_FindBlobsByTags_Account(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		where string
		want  []string
	}{
		{name: "eq", where: `"env" = 'prod'`, want: []string{"other/o.txt", "sdkenv/a.txt", "sdkenv/c.txt"}},
		{name: "unquoted_key", where: `env = 'dev'`, want: []string{"sdkenv/b.txt"}},
		{name: "gt", where: `"Rank" > '010'`, want: []string{"other/o.txt", "sdkenv/b.txt", "sdkenv/c.txt"}},
		{name: "ge", where: `"Rank" >= '050'`, want: []string{"sdkenv/b.txt", "sdkenv/c.txt"}},
		{name: "lt", where: `"Rank" < '050'`, want: []string{"other/o.txt", "sdkenv/a.txt"}},
		{name: "le", where: `"Rank" <= '050'`, want: []string{"other/o.txt", "sdkenv/a.txt", "sdkenv/b.txt"}},
		{name: "range", where: `"Rank" >= '020' AND "Rank" < '090'`, want: []string{"other/o.txt", "sdkenv/b.txt"}},
		{name: "and_two_keys", where: `"env" = 'prod' AND "Rank" > '050'`, want: []string{"sdkenv/c.txt"}},
		{name: "container_scope", where: `@container = 'other' AND "env" = 'prod'`, want: []string{"other/o.txt"}},
		{name: "container_scope_dev", where: `@container = 'sdkenv' AND "env" = 'dev'`,
			want: []string{"sdkenv/b.txt"}},
		{name: "unknown_container", where: `@container = 'nope' AND "env" = 'prod'`, want: []string{}},
		{name: "no_match", where: `"env" = 'staging'`, want: []string{}},
		{name: "case_sensitive", where: `"env" = 'PROD'`, want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := seedFindEnv(t)

			resp, err := env.svc.ServiceClient().FilterBlobs(t.Context(), tt.where, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, foundNames(resp.Blobs))
			assert.Equal(t, tt.where, *resp.Where)
			assert.Empty(t, resp.NextMarker)

			if len(resp.Blobs) > 0 {
				assert.NotEmpty(t, tagMap(resp.Blobs[0].Tags.BlobTagSet))
			}
		})
	}
}

func TestSDK_FindBlobsByTags_Container(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		where string
		want  []string
	}{
		{name: "eq", where: `"env" = 'prod'`, want: []string{"sdkenv/a.txt", "sdkenv/c.txt"}},
		{name: "range", where: `"Rank" > '010' AND "Rank" < '090'`, want: []string{"sdkenv/b.txt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := seedFindEnv(t)

			resp, err := env.cc.FilterBlobs(t.Context(), tt.where, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, foundNames(resp.Blobs))
		})
	}

	t.Run("missing_container", func(t *testing.T) {
		t.Parallel()

		env := seedFindEnv(t)
		_, err := env.svc.ServiceClient().NewContainerClient("ghost").FilterBlobs(t.Context(), `"a" = 'b'`, nil)
		requireStorageErr(t, err, bloberror.ContainerNotFound, 404)
	})
}

func TestSDK_FindBlobsByTags_InvalidExpressions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		where     string
		container bool
	}{
		{name: "empty", where: ``},
		{name: "or_not_supported", where: `"a" = 'b' OR "c" = 'd'`},
		{name: "ne_not_supported", where: `"a" <> 'b'`},
		{name: "parens_not_supported", where: `("a" = 'b')`},
		{name: "unquoted_value", where: `"a" = b`},
		{name: "dangling_and", where: `"a" = 'b' AND`},
		{name: "same_sided_range", where: `"Rank" > '10' AND "Rank" >= '15'`},
		{name: "container_only", where: `@container = 'x'`},
		{name: "container_not_eq", where: `@container > 'x' AND "a" = 'b'`},
		{name: "container_twice", where: `@container = 'x' AND @container = 'y' AND "a" = 'b'`},
		{name: "container_in_container_scope", where: `@container = 'sdkenv' AND "a" = 'b'`, container: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := seedFindEnv(t)

			var err error
			if tt.container {
				_, err = env.cc.FilterBlobs(t.Context(), tt.where, nil)
			} else {
				_, err = env.svc.ServiceClient().FilterBlobs(t.Context(), tt.where, nil)
			}

			requireStorageErr(t, err, bloberror.InvalidQueryParameterValue, 400)
		})
	}

	t.Run("maxresults_zero", func(t *testing.T) {
		t.Parallel()

		env := seedFindEnv(t)
		_, err := env.svc.ServiceClient().FilterBlobs(t.Context(), `"env" = 'prod'`,
			&service.FilterBlobsOptions{MaxResults: new(int32)})
		requireStorageErr(t, err, bloberror.InvalidQueryParameterValue, 400)
	})

	t.Run("bad_marker", func(t *testing.T) {
		t.Parallel()

		env := seedFindEnv(t)
		_, err := env.svc.ServiceClient().FilterBlobs(t.Context(), `"env" = 'prod'`,
			&service.FilterBlobsOptions{Marker: new("!!!")})
		requireStorageErr(t, err, bloberror.InvalidQueryParameterValue, 400)
	})
}

func TestSDK_FindBlobsByTags_Paging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		want      []string
		wantPages int
		pageSize  int32
		container bool
	}{
		{name: "account_by_one", pageSize: 1, wantPages: 3,
			want: []string{"other/o.txt", "sdkenv/a.txt", "sdkenv/c.txt"}},
		{name: "account_by_two", pageSize: 2, wantPages: 2,
			want: []string{"other/o.txt", "sdkenv/a.txt", "sdkenv/c.txt"}},
		{name: "account_one_page", pageSize: 100, wantPages: 1,
			want: []string{"other/o.txt", "sdkenv/a.txt", "sdkenv/c.txt"}},
		{name: "container_by_one", pageSize: 1, container: true, wantPages: 2,
			want: []string{"sdkenv/a.txt", "sdkenv/c.txt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := seedFindEnv(t)

			var (
				got    []string
				marker *string
				pages  int
			)

			for {
				var (
					blobs []*service.FilterBlobItem
					next  *string
					err   error
				)

				if tt.container {
					resp, cerr := env.cc.FilterBlobs(t.Context(), `"env" = 'prod'`,
						&container.FilterBlobsOptions{Marker: marker, MaxResults: &tt.pageSize})
					blobs, next, err = resp.Blobs, resp.NextMarker, cerr
				} else {
					resp, serr := env.svc.ServiceClient().FilterBlobs(t.Context(), `"env" = 'prod'`,
						&service.FilterBlobsOptions{Marker: marker, MaxResults: &tt.pageSize})
					blobs, next, err = resp.Blobs, resp.NextMarker, serr
				}

				require.NoError(t, err)

				pages++

				got = append(got, foundNames(blobs)...)

				if next == nil || *next == "" {
					break
				}

				marker = next
			}

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantPages, pages)
		})
	}
}

func TestSDK_FindBlobsByTags_TracksTagChanges(t *testing.T) {
	t.Parallel()

	env := seedFindEnv(t)
	svc := env.svc.ServiceClient()

	_, err := env.cc.NewBlobClient("a.txt").SetTags(t.Context(), map[string]string{"env": "dev"}, nil)
	require.NoError(t, err)

	resp, err := svc.FilterBlobs(t.Context(), `"env" = 'dev'`, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sdkenv/a.txt", "sdkenv/b.txt"}, foundNames(resp.Blobs))

	_, err = env.cc.NewBlobClient("b.txt").Delete(t.Context(), nil)
	require.NoError(t, err)

	resp, err = svc.FilterBlobs(t.Context(), `"env" = 'dev'`, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sdkenv/a.txt"}, foundNames(resp.Blobs))
}

func TestSDK_FindBlobsByTags_SkipsSnapshots(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb := uploadTagged(t, env, "b.txt", map[string]string{"k": "v"})
	_, err := bb.BlobClient().CreateSnapshot(t.Context(), nil)
	require.NoError(t, err)

	resp, err := env.svc.ServiceClient().FilterBlobs(t.Context(), `"k" = 'v'`, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"sdkenv/b.txt"}, foundNames(resp.Blobs))
}

func TestSDK_Tags_ListBlobsIncludeTags(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	uploadTagged(t, env, "tagged.txt", map[string]string{"a": "1", "b": "2"})
	uploadTagged(t, env, "plain.txt", nil)

	pager := env.cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Tags: true},
	})

	got := map[string]map[string]string{}

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, item := range page.Segment.BlobItems {
			var set []*blob.Tags
			if item.BlobTags != nil {
				set = item.BlobTags.BlobTagSet
			}

			got[*item.Name] = tagMap(set)

			if item.BlobTags != nil {
				require.NotNil(t, item.Properties.TagCount)
				assert.Equal(t, int32(2), *item.Properties.TagCount)
			}
		}
	}

	assert.Equal(t, map[string]map[string]string{
		"tagged.txt": {"a": "1", "b": "2"},
		"plain.txt":  {},
	}, got)
}

func TestSDK_Tags_Persistence(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb := uploadTagged(t, env, "b.txt", map[string]string{"a": "1"})

	snap, err := bb.BlobClient().CreateSnapshot(t.Context(), nil)
	require.NoError(t, err)

	_, err = bb.BlobClient().SetTags(t.Context(), map[string]string{"a": "2", "z": "9"}, nil)
	require.NoError(t, err)

	restored := newRestored(t, env, env.backend.Snapshot(t.Context()))

	got, err := restored.GetBlobTags("sdkenv", "b.txt", "")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "2", "z": "9"}, got)

	got, err = restored.GetBlobTags("sdkenv", "b.txt", *snap.Snapshot)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1"}, got)
}
