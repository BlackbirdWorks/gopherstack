package secretsmanager_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// tagMapFrom collapses the wire []Tag slice DescribeSecret returns into a
// map for assertion convenience.
func tagMapFrom(ts []secretsmanager.Tag) map[string]string {
	m := make(map[string]string, len(ts))
	for _, t := range ts {
		m[t.Key] = t.Value
	}

	return m
}

// ---------------------------------------------------------------------------
// TagResource / UntagResource comprehensive
// ---------------------------------------------------------------------------

func TestTagResource_AddTags(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "tag-add", SecretString: "v"},
	)
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &secretsmanager.TagResourceInput{
		SecretID: "tag-add",
		Tags:     []secretsmanager.Tag{{Key: "team", Value: "platform"}, {Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "tag-add"})
	require.NoError(t, err)
	require.NotNil(t, desc.Tags)
	tagMap := tagMapFrom(desc.Tags)
	assert.Equal(t, "platform", tagMap["team"])
	assert.Equal(t, "prod", tagMap["env"])
}

func TestTagResource_UpdateExistingTag(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tag-upd",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "staging"}},
	})
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &secretsmanager.TagResourceInput{
		SecretID: "tag-upd",
		Tags:     []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "tag-upd"})
	require.NoError(t, err)
	tagMap := tagMapFrom(desc.Tags)
	assert.Equal(t, "prod", tagMap["env"])
}

func TestTagResource_LimitEnforced(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	// Create with 48 tags
	initial := make([]secretsmanager.Tag, 48)
	for i := range initial {
		initial[i] = secretsmanager.Tag{Key: fmt.Sprintf("k%d", i), Value: "v"}
	}
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tag-limit",
		SecretString: "v",
		Tags:         initial,
	})
	require.NoError(t, err)

	// Add 3 more (would be 51 total)
	extra := []secretsmanager.Tag{{Key: "e1", Value: "v"}, {Key: "e2", Value: "v"}, {Key: "e3", Value: "v"}}
	err = b.TagResource(context.Background(), &secretsmanager.TagResourceInput{SecretID: "tag-limit", Tags: extra})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter, "must reject tags that exceed the 50-tag limit")
}

func TestUntagResource_RemoveTag(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tag-rm",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "platform"}},
	})
	require.NoError(t, err)

	err = b.UntagResource(context.Background(), &secretsmanager.UntagResourceInput{
		SecretID: "tag-rm",
		TagKeys:  []string{"env"},
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "tag-rm"})
	require.NoError(t, err)
	tagMap := tagMapFrom(desc.Tags)
	_, hasEnv := tagMap["env"]
	assert.False(t, hasEnv, "env tag must be removed")
	_, hasTeam := tagMap["team"]
	assert.True(t, hasTeam, "team tag must remain")
}

func TestTagResource_DeletedSecret(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "tag-del", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "tag-del"})
	require.NoError(t, err)

	err = b.TagResource(context.Background(), &secretsmanager.TagResourceInput{
		SecretID: "tag-del",
		Tags:     []secretsmanager.Tag{{Key: "k", Value: "v"}},
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

func TestTagResource_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	err := b.TagResource(context.Background(), &secretsmanager.TagResourceInput{
		SecretID: "missing",
		Tags:     []secretsmanager.Tag{{Key: "k", Value: "v"}},
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

// ---------------------------------------------------------------------------
// Tag-limit accounting parity case (ported from handler_parity_test.go)
// ---------------------------------------------------------------------------

// TestTagResource_UpdateExistingKeyDoesNotCountAsNew verifies that updating an
// existing tag key does not count against the 50-tag limit. Real AWS counts net new keys.
func TestTagResource_UpdateExistingKeyDoesNotCountAsNew(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	// Create a secret with 48 tags.
	tags := make([]secretsmanager.Tag, 48)
	for i := range 48 {
		tags[i] = secretsmanager.Tag{Key: fmt.Sprintf("key-%02d", i), Value: "val"}
	}

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         "near-limit-secret",
		SecretString: "v",
		Tags:         tags,
	})
	require.NoError(t, err)

	// Add 2 new tags (reaches 50 — allowed).
	err = b.TagResource(ctx, &secretsmanager.TagResourceInput{
		SecretID: "near-limit-secret",
		Tags:     []secretsmanager.Tag{{Key: "key-48", Value: "v"}, {Key: "key-49", Value: "v"}},
	})
	require.NoError(t, err)

	// Now update 3 existing tags — no net new keys, must succeed even though at limit.
	err = b.TagResource(ctx, &secretsmanager.TagResourceInput{
		SecretID: "near-limit-secret",
		Tags: []secretsmanager.Tag{
			{Key: "key-00", Value: "updated"},
			{Key: "key-01", Value: "updated"},
			{Key: "key-02", Value: "updated"},
		},
	})
	assert.NoError(t, err,
		"real AWS: updating existing tag keys must not count against the 50-tag limit")
}

// ---------------------------------------------------------------------------
// TagSecretByARN / UntagSecretByARN
// ---------------------------------------------------------------------------

// TestTagSecretByARN verifies TagSecretByARN applies tags via ARN.
func TestTagSecretByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		newTags   map[string]string
		name      string
		setupName string
		lookupID  string
		wantErr   bool
	}{
		{
			name:      "tags_applied_by_name",
			setupName: "tag-by-arn",
			lookupID:  "tag-by-arn",
			newTags:   map[string]string{"k": "v"},
		},
		{
			name:      "not_found",
			setupName: "",
			lookupID:  "nonexistent",
			newTags:   map[string]string{"k": "v"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()

			if tt.setupName != "" {
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: tt.setupName})
				require.NoError(t, err)
			}

			err := b.TagSecretByARN(context.Background(), tt.lookupID, tt.newTags)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TagResource / TaggedSecrets
// ---------------------------------------------------------------------------

// TestTagResource_Backend tests tag add/remove operations via the HTTP handler
// (distinct from the backend-level TestTagResource_AddTags / TestUntagResource_RemoveTag
// above: this exercises the full TagResource -> DescribeSecret -> UntagResource ->
// DescribeSecret cycle through the HTTP dispatch layer).
func TestTagResource_Backend(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tag-secret",
		SecretString: "value",
	})
	require.NoError(t, err)

	// TagResource via HTTP
	tagBody := `{"SecretId":"tag-secret","Tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"platform"}]}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tagBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.TagResource")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret should show tags
	desc, err := backend.DescribeSecret(
		context.Background(),
		&secretsmanager.DescribeSecretInput{SecretID: "tag-secret"},
	)
	require.NoError(t, err)
	descTags := tagMapFrom(desc.Tags)
	envVal := descTags["env"]
	assert.Equal(t, "test", envVal)
	teamVal := descTags["team"]
	assert.Equal(t, "platform", teamVal)

	// UntagResource via HTTP
	untagBody := `{"SecretId":"tag-secret","TagKeys":["env"]}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(untagBody))
	req2.Header.Set("X-Amz-Target", "secretsmanager.UntagResource")
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	desc2, err := backend.DescribeSecret(
		context.Background(),
		&secretsmanager.DescribeSecretInput{SecretID: "tag-secret"},
	)
	require.NoError(t, err)
	desc2Tags := tagMapFrom(desc2.Tags)
	_, hasEnv2 := desc2Tags["env"]
	assert.False(t, hasEnv2)
	team2Val := desc2Tags["team"]
	assert.Equal(t, "platform", team2Val)
}

// TestTaggedSecrets verifies TaggedSecrets returns ARN+tags for active secrets.
func TestTaggedSecrets(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	_, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name: "tagged-arn",
		Tags: []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "no-tags"})
	require.NoError(t, err)

	// Delete one to confirm it's excluded.
	_, err = backend.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "no-tags"})
	require.NoError(t, err)

	infos := backend.TaggedSecrets(context.Background())
	require.Len(t, infos, 1)
	assert.NotEmpty(t, infos[0].ARN)
	assert.Equal(t, "prod", infos[0].Tags["env"])
}

// TestUntagSecretByARN verifies UntagSecretByARN removes tag keys.
func TestUntagSecretByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupName string
		lookupID  string
		tagKeys   []string
		wantErr   bool
	}{
		{
			name:      "removes_tags",
			setupName: "untag-by-arn",
			lookupID:  "untag-by-arn",
			tagKeys:   []string{"env"},
		},
		{
			name:      "not_found",
			setupName: "",
			lookupID:  "nonexistent",
			tagKeys:   []string{"k"},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()

			if tt.setupName != "" {
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name: tt.setupName,
					Tags: []secretsmanager.Tag{{Key: "env", Value: "test"}},
				})
				require.NoError(t, err)
			}

			err := b.UntagSecretByARN(context.Background(), tt.lookupID, tt.tagKeys)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
