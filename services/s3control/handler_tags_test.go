package s3control_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3control "github.com/blackbirdworks/gopherstack/services/s3control"
)

// ---- Resource Tags ----

func TestResourceTags(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:s3:us-east-1:acct1:accesspoint/my-ap"
	const tagsPath = "/v20180820/tags/" + arn

	t.Run("tag and list tags", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		tagRec := doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags></TagResourceRequest>`)
		require.Equal(t, http.StatusOK, tagRec.Code)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		assert.Contains(t, listRec.Body.String(), "env")
		assert.Contains(t, listRec.Body.String(), "prod")
	})

	t.Run("list empty tags returns 200 with empty list", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())

		rec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("untag removes specific keys", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		twoTagsBody := `<TagResourceRequest><Tags>` +
			`<Tag><Key>a</Key><Value>1</Value></Tag>` +
			`<Tag><Key>b</Key><Value>2</Value></Tag>` +
			`</Tags></TagResourceRequest>`
		_ = doS3Request(t, h, http.MethodPost, tagsPath, twoTagsBody)

		untagRec := doS3Request(t, h, http.MethodDelete, tagsPath,
			`<UntagResourceRequest><TagKeys><TagKey>a</TagKey></TagKeys></UntagResourceRequest>`)
		require.Equal(t, http.StatusNoContent, untagRec.Code)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		body := listRec.Body.String()
		assert.NotContains(t, body, ">a<")
		assert.Contains(t, body, ">b<")
	})

	t.Run("tag overwrites existing keys", func(t *testing.T) {
		t.Parallel()

		h := s3control.NewHandler(s3control.NewInMemoryBackend())
		_ = doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>dev</Value></Tag></Tags></TagResourceRequest>`)
		_ = doS3Request(t, h, http.MethodPost, tagsPath,
			`<TagResourceRequest><Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags></TagResourceRequest>`)

		listRec := doS3Request(t, h, http.MethodGet, tagsPath, "")
		require.Equal(t, http.StatusOK, listRec.Code)
		assert.Contains(t, listRec.Body.String(), "prod")
		assert.NotContains(t, listRec.Body.String(), "dev")
	})
}

func TestBackendResourceTags(t *testing.T) {
	t.Parallel()

	t.Run("untag on unknown ARN is a no-op", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		// Should not panic
		b.UntagResource("arn:aws:s3:::nonexistent", []string{"key"})
	})

	t.Run("list on unknown ARN returns empty map", func(t *testing.T) {
		t.Parallel()

		b := s3control.NewInMemoryBackend()
		tags := b.ListTagsForResource("arn:aws:s3:::nonexistent")
		assert.Empty(t, tags)
	})
}

func TestResourceTags_Table(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		arn     string
		wantLen int
	}{
		{
			name:    "tag_and_list",
			arn:     "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			tags:    map[string]string{"Purpose": "analytics", "CostCenter": "123"},
			wantLen: 2,
		},
		{
			name:    "no_tags",
			arn:     "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			tags:    nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			if len(tt.tags) > 0 {
				b.TagResource(tt.arn, tt.tags)
			}

			rec := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/tags/"+tt.arn, "000000000000", "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for k, v := range tt.tags {
				assert.Contains(t, body, k)
				assert.Contains(t, body, v)
			}
		})
	}
}

func TestTagResource_PutViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		body     string
		wantKeys []string
		wantCode int
	}{
		{
			name: "tag_resource",
			arn:  "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			body: `<TagResourceRequest><Tags>` +
				`<Tag><Key>Env</Key><Value>prod</Value></Tag>` +
				`<Tag><Key>Owner</Key><Value>team-a</Value></Tag>` +
				`</Tags></TagResourceRequest>`,
			wantCode: http.StatusOK,
			wantKeys: []string{"Env", "Owner"},
		},
		{
			name:     "tag_resource_empty_tags",
			arn:      "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			body:     `<TagResourceRequest><Tags></Tags></TagResourceRequest>`,
			wantCode: http.StatusOK,
			wantKeys: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			rec := doS3ControlNewOpRequest(t, h, http.MethodPost,
				"/v20180820/tags/"+tt.arn, "000000000000", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			// Verify via list
			rec2 := doS3ControlNewOpRequest(t, h, http.MethodGet,
				"/v20180820/tags/"+tt.arn, "000000000000", "")
			listBody := rec2.Body.String()
			for _, k := range tt.wantKeys {
				assert.Contains(t, listBody, k)
			}
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		initialTags map[string]string
		name        string
		arn         string
		removeKeys  []string
		wantGone    []string
		wantStay    []string
		wantCode    int
	}{
		{
			name:        "remove_one_tag",
			arn:         "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp1",
			initialTags: map[string]string{"Env": "prod", "Owner": "alice"},
			removeKeys:  []string{"Env"},
			wantCode:    http.StatusNoContent,
			wantGone:    []string{"Env"},
			wantStay:    []string{"Owner"},
		},
		{
			name:        "remove_all_tags",
			arn:         "arn:aws:s3:us-east-1:000000000000:storagelensgroup/grp2",
			initialTags: map[string]string{"k1": "v1", "k2": "v2"},
			removeKeys:  []string{"k1", "k2"},
			wantCode:    http.StatusNoContent,
			wantGone:    []string{"k1", "k2"},
			wantStay:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			h := s3control.NewHandler(b)

			b.TagResource(tt.arn, tt.initialTags)

			// Build untag body
			var sb strings.Builder
			sb.WriteString(`<UntagResourceRequest><TagKeys>`)
			for _, k := range tt.removeKeys {
				sb.WriteString(`<TagKey>` + k + `</TagKey>`)
			}
			sb.WriteString(`</TagKeys></UntagResourceRequest>`)

			rec := doS3ControlNewOpRequest(t, h, http.MethodDelete,
				"/v20180820/tags/"+tt.arn, "000000000000", sb.String())
			assert.Equal(t, tt.wantCode, rec.Code)

			remaining := b.ListTagsForResource(tt.arn)
			for _, k := range tt.wantGone {
				_, ok := remaining[k]
				assert.False(t, ok, "tag %q should be gone", k)
			}
			for _, k := range tt.wantStay {
				_, ok := remaining[k]
				assert.True(t, ok, "tag %q should remain", k)
			}
		})
	}
}
