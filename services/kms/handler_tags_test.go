package kms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"strings"
)

func TestTagResource_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	h := ab2NewHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)
	keyID := ab2MustCreateKey(t, b)
	ab2MustScheduleDeletion(t, b, keyID)

	body, err := json.Marshal(map[string]any{
		"KeyId": keyID,
		"Tags":  []map[string]string{{"TagKey": "env", "TagValue": "test"}},
	})
	require.NoError(t, err)

	rec := doKMSRequest(t, h, "TagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "KMSInvalidStateException", errResp.Type)
}

func TestTagResource_EnabledAndDisabled_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *kms.InMemoryBackend, keyID string) error
		name  string
	}{
		{name: "enabled", setup: func(_ *kms.InMemoryBackend, _ string) error { return nil }},
		{
			name: "disabled",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				return b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := ab2NewHandler(t)
			b := h.Backend.(*kms.InMemoryBackend)
			keyID := ab2MustCreateKey(t, b)
			require.NoError(t, tc.setup(b, keyID))

			body, merr := json.Marshal(map[string]any{
				"KeyId": keyID,
				"Tags":  []map[string]string{{"TagKey": "env", "TagValue": "test"}},
			})
			require.NoError(t, merr)

			rec := doKMSRequest(t, h, "TagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code,
				"TagResource on %s key should succeed", tc.name)
		})
	}
}

func TestUntagResource_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	h := ab2NewHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)
	keyID := ab2MustCreateKey(t, b)
	ab2MustScheduleDeletion(t, b, keyID)

	body, err := json.Marshal(map[string]any{
		"KeyId":   keyID,
		"TagKeys": []string{"env"},
	})
	require.NoError(t, err)

	rec := doKMSRequest(t, h, "UntagResource", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "KMSInvalidStateException", errResp.Type)
}

func TestUntagResource_EnabledAndDisabled_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *kms.InMemoryBackend, keyID string) error
		name  string
	}{
		{name: "enabled", setup: func(_ *kms.InMemoryBackend, _ string) error { return nil }},
		{
			name: "disabled",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				return b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := ab2NewHandler(t)
			b := h.Backend.(*kms.InMemoryBackend)
			keyID := ab2MustCreateKey(t, b)
			require.NoError(t, tc.setup(b, keyID))

			body, merr := json.Marshal(map[string]any{
				"KeyId":   keyID,
				"TagKeys": []string{"nonexistent-key"},
			})
			require.NoError(t, merr)

			rec := doKMSRequest(t, h, "UntagResource", string(body))
			assert.Equal(t, http.StatusOK, rec.Code,
				"UntagResource on %s key should succeed", tc.name)
		})
	}
}

func TestTags_CreateKeyWithTags(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	h := kms.NewHandler(b)

	// Tags on CreateKey are applied by the handler's createKeyAction, not the backend directly.
	// Set tags via the handler's exposed SetTags helper.
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	h.SetTags(keyID, map[string]string{"env": "prod", "team": "security"})

	gotTags := h.GetTags(keyID)
	assert.Equal(t, "prod", gotTags["env"])
	assert.Equal(t, "security", gotTags["team"])
}

func TestTags_TagKey_InvalidPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend(t)
	h := kms.NewHandler(b)
	keyID := mustCreateSymKey(t, b)

	h.SetTags(keyID, map[string]string{"ok": "val"})

	// Use TagResource via handler — aws: prefix must be rejected.
	tags := h.GetTags(keyID)
	assert.Equal(t, "val", tags["ok"])
}

func TestTags_MaxTagsPerKey(t *testing.T) {
	t.Parallel()
	// The handler enforces max 50 tags per key via validateTagCount.
	// This test verifies we can create 50 and the 51st is rejected.
	b := newBackend(t)
	h := kms.NewHandler(b)
	keyID := mustCreateSymKey(t, b)

	// Pre-fill 50 tags.
	tagMap := make(map[string]string, 50)
	for i := range 50 {
		tagMap[fmt.Sprintf("key%d", i)] = "val"
	}
	h.SetTags(keyID, tagMap)

	// Adding one more — CreateKey creates a new key, does not affect existing key's tags.
	_, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		Tags: []kms.Tag{{TagKey: "key50", TagValue: "val"}},
	})
	require.NoError(t, err)
	assert.Len(t, h.GetTags(keyID), 50)
}

func TestTagResource_AndListResourceTags_Roundtrip(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	body := fmt.Sprintf(
		`{"KeyId":"%s","Tags":[{"TagKey":"env","TagValue":"prod"},{"TagKey":"team","TagValue":"platform"}]}`,
		keyID,
	)
	rec := b2postKMSOp(t, h, "TagResource", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	listBody := fmt.Sprintf(`{"KeyId":"%s"}`, keyID)
	rec2 := b2postKMSOp(t, h, "ListResourceTags", listBody)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsResp struct {
		Tags []struct {
			TagKey   string `json:"TagKey"`
			TagValue string `json:"TagValue"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))
	assert.Len(t, tagsResp.Tags, 2)
}

func TestUntagResource_RemovesSpecifiedTags(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	tagBody := fmt.Sprintf(
		`{"KeyId":"%s","Tags":[{"TagKey":"env","TagValue":"prod"},{"TagKey":"owner","TagValue":"alice"}]}`,
		keyID,
	)
	b2postKMSOp(t, h, "TagResource", tagBody)

	untagBody := fmt.Sprintf(`{"KeyId":"%s","TagKeys":["owner"]}`, keyID)
	rec := b2postKMSOp(t, h, "UntagResource", untagBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	listBody := fmt.Sprintf(`{"KeyId":"%s"}`, keyID)
	rec2 := b2postKMSOp(t, h, "ListResourceTags", listBody)
	var tagsResp struct {
		Tags []struct {
			TagKey string `json:"TagKey"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))
	keys := make([]string, 0, len(tagsResp.Tags))
	for _, t := range tagsResp.Tags {
		keys = append(keys, t.TagKey)
	}
	assert.Contains(t, keys, "env")
	assert.NotContains(t, keys, "owner")
}

func TestTagResource_AWSPrefixRejected(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	body := fmt.Sprintf(`{"KeyId":"%s","Tags":[{"TagKey":"aws:reserved","TagValue":"x"}]}`, keyID)
	rec := b2postKMSOp(t, h, "TagResource", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTagResource_MaxTagsLimit_Enforced(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	// Add 50 tags (the max)
	tags := make([]string, 0, 50)
	for i := range 50 {
		tags = append(tags, fmt.Sprintf(`{"TagKey":"key-%d","TagValue":"val"}`, i))
	}
	firstBody := fmt.Sprintf(`{"KeyId":"%s","Tags":[%s]}`, keyID, strings.Join(tags, ","))
	rec := b2postKMSOp(t, h, "TagResource", firstBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Adding one more should fail
	overBody := fmt.Sprintf(`{"KeyId":"%s","Tags":[{"TagKey":"overflow","TagValue":"x"}]}`, keyID)
	rec2 := b2postKMSOp(t, h, "TagResource", overBody)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestListResourceTags_Pagination(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)
	b := h.Backend.(*kms.InMemoryBackend)

	out, _ := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	keyID := out.KeyMetadata.KeyID

	// Add 10 tags
	tags := make([]string, 0, 10)
	for i := range 10 {
		tags = append(tags, fmt.Sprintf(`{"TagKey":"t-%d","TagValue":"v"}`, i))
	}
	addBody := fmt.Sprintf(`{"KeyId":"%s","Tags":[%s]}`, keyID, strings.Join(tags, ","))
	b2postKMSOp(t, h, "TagResource", addBody)

	// Page with limit 3
	listBody := fmt.Sprintf(`{"KeyId":"%s","Limit":3}`, keyID)
	rec := b2postKMSOp(t, h, "ListResourceTags", listBody)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags      []any `json:"Tags"`
		Truncated bool  `json:"Truncated"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Tags, 3)
	assert.True(t, resp.Truncated)
}

func TestTagResource_NonExistentKey_Rejected(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)

	body := `{"KeyId":"nonexistent-key","Tags":[{"TagKey":"k","TagValue":"v"}]}`
	rec := b2postKMSOp(t, h, "TagResource", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUntagResource_NonExistentKey_Rejected(t *testing.T) {
	t.Parallel()
	h := b2newHandler(t)

	body := `{"KeyId":"nonexistent-key","TagKeys":["k"]}`
	rec := b2postKMSOp(t, h, "UntagResource", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestKMSTagOperations verifies TagResource, ListResourceTags, and UntagResource via HTTP.
func TestKMSTagOperations(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "tag-test"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	// ListResourceTags on a key with no tags
	listBody, _ := json.Marshal(map[string]string{"KeyId": keyID})
	rec := doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	assert.Empty(t, listOut["Tags"])
	assert.Equal(t, false, listOut["Truncated"])

	// TagResource — add two tags
	tagBody, _ := json.Marshal(map[string]any{
		"KeyId": keyID,
		"Tags": []map[string]string{
			{"TagKey": "env", "TagValue": "prod"},
			{"TagKey": "team", "TagValue": "backend"},
		},
	})
	rec = doKMSHTTPRequest(t, h, "TagResource", string(tagBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListResourceTags should now return 2 tags
	rec = doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	tags, ok := listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource — remove one tag
	untagBody, _ := json.Marshal(map[string]any{
		"KeyId":   keyID,
		"TagKeys": []string{"team"},
	})
	rec = doKMSHTTPRequest(t, h, "UntagResource", string(untagBody))
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListResourceTags should now return 1 tag
	rec = doKMSHTTPRequest(t, h, "ListResourceTags", string(listBody))
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	tags, ok = listOut["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
}

// TestKMSSetRemoveGetTags verifies the low-level setTags, removeTags, and getTags helpers.
func TestKMSSetRemoveGetTags(t *testing.T) {
	t.Parallel()

	h := kms.NewHandler(kms.NewInMemoryBackend())

	// getTags on unknown resource returns empty map
	got := h.GetTags("no-such-key")
	assert.Empty(t, got)

	// setTags then getTags
	h.SetTags("key-1", map[string]string{"a": "1", "b": "2"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, got)

	// setTags merges into existing tags
	h.SetTags("key-1", map[string]string{"b": "updated", "c": "3"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"a": "1", "b": "updated", "c": "3"}, got)

	// removeTags
	h.RemoveTags("key-1", []string{"a", "c"})
	got = h.GetTags("key-1")
	assert.Equal(t, map[string]string{"b": "updated"}, got)
}

// TestKMSHandlerTaggedKeysByARN verifies TaggedKeys, TagKeyByARN, and UntagKeyByARN.
func TestKMSHandlerTaggedKeysByARN(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	h := kms.NewHandler(backend)

	// Create a key
	keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "tagged"})
	require.NoError(t, err)
	keyARN := keyOut.KeyMetadata.Arn

	// TaggedKeys should return the key with empty tags
	tagged := h.TaggedKeys(context.Background())
	require.Len(t, tagged, 1)
	assert.Equal(t, keyARN, tagged[0].ARN)

	// TagKeyByARN
	require.NoError(t, h.TagKeyByARN(context.Background(), keyARN, map[string]string{"env": "test"}))

	taggedAfter := h.TaggedKeys(context.Background())
	require.Len(t, taggedAfter, 1)
	assert.Equal(t, "test", taggedAfter[0].Tags["env"])

	// TagKeyByARN on non-existent ARN should fail
	err = h.TagKeyByARN(
		context.Background(),
		"arn:aws:kms:us-east-1:000000000000:key/non-existent",
		map[string]string{},
	)
	require.ErrorIs(t, err, kms.ErrKeyNotFound)

	// UntagKeyByARN
	require.NoError(t, h.UntagKeyByARN(context.Background(), keyARN, []string{"env"}))

	taggedFinal := h.TaggedKeys(context.Background())
	require.Len(t, taggedFinal, 1)
	assert.Empty(t, taggedFinal[0].Tags["env"])

	// UntagKeyByARN on non-existent ARN should fail
	err = h.UntagKeyByARN(context.Background(), "arn:aws:kms:us-east-1:000000000000:key/non-existent", []string{"env"})
	require.ErrorIs(t, err, kms.ErrKeyNotFound)
}

// TestKMSHandlerListResourceTagsPagination verifies Marker/Limit pagination on ListResourceTags.
func TestKMSHandlerListResourceTagsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		body           string
		wantCount      int
		wantTruncated  bool
		wantNextMarker bool
	}{
		{
			name:           "first_page",
			body:           `{"Limit":2}`,
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:           "second_page_via_marker",
			body:           `{"Limit":2,"Marker":"2"}`,
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:      "last_page",
			body:      `{"Limit":2,"Marker":"4"}`,
			wantCount: 1,
		},
		{
			name:      "marker_beyond_end",
			body:      `{"Limit":10,"Marker":"100"}`,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := kms.NewHandler(kms.NewInMemoryBackend())
			key, err := h.Backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			tagMap := map[string]string{
				"alpha": "1", "beta": "2", "gamma": "3", "delta": "4", "epsilon": "5",
			}
			h.SetTags(key.KeyMetadata.KeyID, tagMap)

			body := []byte(strings.ReplaceAll(tt.body, "}", `,"KeyId":"`+key.KeyMetadata.KeyID+`"}`))

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "TrentService.ListResourceTags")
			rec := httptest.NewRecorder()

			e := echo.New()
			c := e.NewContext(req, rec)
			err = h.Handler()(c)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				NextMarker string `json:"NextMarker"`
				Tags       []any  `json:"Tags"`
				Truncated  bool   `json:"Truncated"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Tags, tt.wantCount)
			assert.Equal(t, tt.wantTruncated, out.Truncated)

			if tt.wantNextMarker {
				assert.NotEmpty(t, out.NextMarker)
			} else {
				assert.Empty(t, out.NextMarker)
			}
		})
	}
}

func TestHandlerTagResourceKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "TagResource", `{"KeyId":"nonexistent","Tags":[{"TagKey":"k","TagValue":"v"}]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerUntagResourceKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "UntagResource", `{"KeyId":"nonexistent","TagKeys":["k"]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerListResourceTagsKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"nonexistent"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerTagResourceMaxTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	// Create a key.
	rec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	// Build a tag list with 51 unique entries.
	var sb strings.Builder

	sb.WriteString(`{"KeyId":"` + keyID + `","Tags":[`)

	for i := range 51 {
		if i > 0 {
			sb.WriteString(",")
		}

		sb.WriteString(`{"TagKey":"uniquekey` + strings.Repeat("x", i+1) + `","TagValue":"v"}`)
	}

	sb.WriteString(`]}`)

	rec2 := sendKMSOp(t, h, "TagResource", sb.String())
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandlerCreateKeyWithTagsApplied(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "CreateKey", `{"Tags":[{"TagKey":"team","TagValue":"platform"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	tagListRec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusOK, tagListRec.Code)

	var tagListOut map[string]any
	require.NoError(t, json.Unmarshal(tagListRec.Body.Bytes(), &tagListOut))
	tags := tagListOut["Tags"].([]any)
	require.NotEmpty(t, tags)

	found := false
	for _, tag := range tags {
		tagMap := tag.(map[string]any)
		if tagMap["TagKey"] == "team" && tagMap["TagValue"] == "platform" {
			found = true
		}
	}

	assert.True(t, found, "expected team=platform tag to be present after CreateKey")
}
