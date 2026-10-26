package azureblob_test

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// condCase builds ModifiedAccessConditions from the blob's ETag and Last-Modified.
type condCase struct {
	build    func(etag azcore.ETag, lm time.Time) *blob.ModifiedAccessConditions
	name     string
	wantCode bloberror.Code
	want     int
}

func condCases() []condCase {
	return []condCase{
		{name: "if_match_ok", want: 200, build: func(e azcore.ETag, _ time.Time) *blob.ModifiedAccessConditions {
			return &blob.ModifiedAccessConditions{IfMatch: &e}
		}},
		{name: "if_match_stale", want: 412, wantCode: bloberror.ConditionNotMet,
			build: func(azcore.ETag, time.Time) *blob.ModifiedAccessConditions {
				stale := azcore.ETag(`"stale"`)

				return &blob.ModifiedAccessConditions{IfMatch: &stale}
			}},
		{name: "if_none_match_other", want: 200, build: func(azcore.ETag, time.Time) *blob.ModifiedAccessConditions {
			other := azcore.ETag(`"other"`)

			return &blob.ModifiedAccessConditions{IfNoneMatch: &other}
		}},
		{name: "if_none_match_same", want: -1, wantCode: bloberror.ConditionNotMet,
			build: func(e azcore.ETag, _ time.Time) *blob.ModifiedAccessConditions {
				return &blob.ModifiedAccessConditions{IfNoneMatch: &e}
			}},
		{
			name: "if_modified_since_before",
			want: 200,
			build: func(_ azcore.ETag, lm time.Time) *blob.ModifiedAccessConditions {
				t := lm.Add(-time.Hour)

				return &blob.ModifiedAccessConditions{IfModifiedSince: &t}
			},
		},
		{name: "if_modified_since_equal", want: -1, wantCode: bloberror.ConditionNotMet,
			build: func(_ azcore.ETag, lm time.Time) *blob.ModifiedAccessConditions {
				return &blob.ModifiedAccessConditions{IfModifiedSince: &lm}
			}},
		{name: "if_unmodified_since_before", want: 412, wantCode: bloberror.ConditionNotMet,
			build: func(_ azcore.ETag, lm time.Time) *blob.ModifiedAccessConditions {
				t := lm.Add(-time.Hour)

				return &blob.ModifiedAccessConditions{IfUnmodifiedSince: &t}
			}},
		{
			name: "if_unmodified_since_equal",
			want: 200,
			build: func(_ azcore.ETag, lm time.Time) *blob.ModifiedAccessConditions {
				return &blob.ModifiedAccessConditions{IfUnmodifiedSince: &lm}
			},
		},
		{name: "if_tags", want: 412, wantCode: bloberror.ConditionNotMet,
			build: func(azcore.ETag, time.Time) *blob.ModifiedAccessConditions {
				return &blob.ModifiedAccessConditions{IfTags: new(`"env" = 'prod'`)}
			}},
	}
}

func seedSDKBlob(t *testing.T, env *sdkEnv, name string) (*blockblob.Client, azcore.ETag, time.Time) {
	t.Helper()

	bb := env.cc.NewBlockBlobClient(name)
	_, err := bb.UploadBuffer(t.Context(), []byte("data"), nil)
	require.NoError(t, err)

	props, err := bb.GetProperties(t.Context(), nil)
	require.NoError(t, err)
	require.NotNil(t, props.ETag)

	env.clock.Advance(time.Hour)

	return bb, *props.ETag, *props.LastModified
}

func TestSDK_Conditional_Reads(t *testing.T) {
	t.Parallel()

	for _, tt := range condCases() {
		for _, op := range []string{"download", "properties"} {
			t.Run(op+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				env := newSDKEnv(t)
				bb, etag, lm := seedSDKBlob(t, env, "r.txt")
				ac := &blob.AccessConditions{ModifiedAccessConditions: tt.build(etag, lm)}

				var err error

				if op == "download" {
					var resp blob.DownloadStreamResponse

					resp, err = bb.BlobClient().
						DownloadStream(t.Context(), &blob.DownloadStreamOptions{AccessConditions: ac})
					if err == nil && resp.Body != nil {
						_ = resp.Body.Close()
					}

					if tt.want == -1 {
						// The SDK reports a download 304 as a nil error plus an empty response.
						require.NoError(t, err)
						assert.Nil(t, resp.Body)
						assert.NotNil(t, resp.ErrorCode)

						return
					}
				} else {
					_, err = bb.GetProperties(t.Context(), &blob.GetPropertiesOptions{AccessConditions: ac})
				}

				switch {
				case tt.wantCode == "":
					require.NoError(t, err)
				case tt.want == -1:
					requireStatus(t, err, 304)
				default:
					requireStatus(t, err, 412)
				}
			})
		}
	}
}

func TestSDK_Conditional_Writes(t *testing.T) {
	t.Parallel()

	ops := map[string]func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error{
		"metadata": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			_, err := bb.SetMetadata(t.Context(), map[string]*string{"a": new("b")},
				&blob.SetMetadataOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac}})

			return err
		},
		"properties": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			_, err := bb.SetHTTPHeaders(t.Context(), blob.HTTPHeaders{BlobContentType: new("text/x")},
				&blob.SetHTTPHeadersOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac}})

			return err
		},
		"snapshot": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			_, err := bb.CreateSnapshot(t.Context(),
				&blob.CreateSnapshotOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac}})

			return err
		},
		"upload": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			_, err := bb.Upload(t.Context(), body("new"),
				&blockblob.UploadOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac}})

			return err
		},
		"commit": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			if _, err := bb.StageBlock(t.Context(), blockID(0), body("x"), nil); err != nil {
				return err
			}

			_, err := bb.CommitBlockList(
				t.Context(),
				[]string{blockID(0)},
				&blockblob.CommitBlockListOptions{
					AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac},
				},
			)

			return err
		},
		"delete": func(t *testing.T, bb *blockblob.Client, mac *blob.ModifiedAccessConditions) error {
			t.Helper()

			_, err := bb.Delete(t.Context(),
				&blob.DeleteOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: mac}})

			return err
		},
	}

	for op, run := range ops {
		for _, tt := range condCases() {
			t.Run(op+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				env := newSDKEnv(t)
				bb, etag, lm := seedSDKBlob(t, env, "w.txt")

				err := run(t, bb, tt.build(etag, lm))
				if tt.wantCode == "" {
					require.NoError(t, err)

					return
				}

				// Writes answer 412 for every failed condition.
				requireStorageErr(t, err, bloberror.ConditionNotMet, 412)
			})
		}
	}
}

func TestSDK_Conditional_Create(t *testing.T) {
	t.Parallel()

	star := azcore.ETagAny

	tests := []struct {
		mac      blob.ModifiedAccessConditions
		name     string
		wantCode bloberror.Code
		status   int
		exists   bool
	}{
		{name: "none_match_star_missing", mac: blob.ModifiedAccessConditions{IfNoneMatch: &star}},
		{name: "none_match_star_exists", exists: true, mac: blob.ModifiedAccessConditions{IfNoneMatch: &star},
			wantCode: bloberror.BlobAlreadyExists, status: 409},
		{name: "if_match_missing", mac: blob.ModifiedAccessConditions{IfMatch: &star},
			wantCode: bloberror.ConditionNotMet, status: 412},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			bb := env.cc.NewBlockBlobClient("create.txt")

			if tt.exists {
				_, err := bb.UploadBuffer(t.Context(), []byte("old"), nil)
				require.NoError(t, err)
			}

			_, err := bb.Upload(t.Context(), body("x"),
				&blockblob.UploadOptions{AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: &tt.mac}})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, tt.wantCode, tt.status)
		})
	}
}

func TestSDK_Conditional_Copy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dst      func(etag azcore.ETag) *blob.ModifiedAccessConditions
		src      func(etag azcore.ETag) *blob.SourceModifiedAccessConditions
		wantCode bloberror.Code
	}{
		{name: "source_match", src: func(e azcore.ETag) *blob.SourceModifiedAccessConditions {
			return &blob.SourceModifiedAccessConditions{SourceIfMatch: &e}
		}},
		{name: "source_stale", wantCode: bloberror.SourceConditionNotMet,
			src: func(azcore.ETag) *blob.SourceModifiedAccessConditions {
				stale := azcore.ETag(`"stale"`)

				return &blob.SourceModifiedAccessConditions{SourceIfMatch: &stale}
			}},
		{name: "dest_none_match_star", wantCode: bloberror.BlobAlreadyExists,
			dst: func(azcore.ETag) *blob.ModifiedAccessConditions {
				star := azcore.ETagAny

				return &blob.ModifiedAccessConditions{IfNoneMatch: &star}
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)
			src, etag, _ := seedSDKBlob(t, env, "src.txt")
			dst, _, _ := seedSDKBlob(t, env, "dst.txt")

			opts := &blob.StartCopyFromURLOptions{}
			if tt.src != nil {
				opts.SourceModifiedAccessConditions = tt.src(etag)
			}

			if tt.dst != nil {
				opts.AccessConditions = &blob.AccessConditions{ModifiedAccessConditions: tt.dst(etag)}
			}

			_, err := dst.StartCopyFromURL(t.Context(), src.URL(), opts)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			status := 412
			if tt.wantCode == bloberror.BlobAlreadyExists {
				status = 409
			}

			requireStorageErr(t, err, tt.wantCode, status)
		})
	}
}

func TestSDK_Conditional_Container(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build func(lm time.Time) *container.ModifiedAccessConditions
		name  string
		want  bool
	}{
		{name: "unmodified_since_before", want: false, build: func(lm time.Time) *container.ModifiedAccessConditions {
			t := lm.Add(-time.Hour)

			return &container.ModifiedAccessConditions{IfUnmodifiedSince: &t}
		}},
		{name: "unmodified_since_equal", want: true, build: func(lm time.Time) *container.ModifiedAccessConditions {
			return &container.ModifiedAccessConditions{IfUnmodifiedSince: &lm}
		}},
		{name: "modified_since_equal", want: false, build: func(lm time.Time) *container.ModifiedAccessConditions {
			return &container.ModifiedAccessConditions{IfModifiedSince: &lm}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSDKEnv(t)

			props, err := env.cc.GetProperties(t.Context(), nil)
			require.NoError(t, err)

			_, err = env.cc.Delete(t.Context(), &container.DeleteOptions{
				AccessConditions: &container.AccessConditions{ModifiedAccessConditions: tt.build(*props.LastModified)},
			})
			if tt.want {
				require.NoError(t, err)

				return
			}

			requireStorageErr(t, err, bloberror.ConditionNotMet, 412)

			_, err = env.cc.GetProperties(t.Context(), nil)
			require.NoError(t, err, "a failed condition must not delete the container")
		})
	}
}

func TestSDK_Conditional_304CarriesValidators(t *testing.T) {
	t.Parallel()

	env := newSDKEnv(t)
	bb, etag, _ := seedSDKBlob(t, env, "v.txt")

	_, err := bb.GetProperties(t.Context(), &blob.GetPropertiesOptions{
		AccessConditions: &blob.AccessConditions{
			ModifiedAccessConditions: &blob.ModifiedAccessConditions{IfNoneMatch: &etag},
		},
	})

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	assert.Equal(t, 304, re.StatusCode)
	assert.Equal(t, string(etag), re.RawResponse.Header.Get("ETag"))
}
