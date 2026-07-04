package cloudfront_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// streamingDistConfigXML builds a minimal StreamingDistributionConfig XML body.
func streamingDistConfigXML(callerRef, comment string, enabled bool) []byte {
	tmpl := `<StreamingDistributionConfig>` +
		`<CallerReference>%s</CallerReference>` +
		`<S3Origin><DomainName>my-bucket.s3.amazonaws.com</DomainName>` +
		`<OriginAccessIdentity></OriginAccessIdentity></S3Origin>` +
		`<Aliases><Items><CNAME>videos.example.com</CNAME></Items></Aliases>` +
		`<Comment>%s</Comment>` +
		`<TrustedSigners><Enabled>false</Enabled></TrustedSigners>` +
		`<PriceClass>PriceClass_All</PriceClass>` +
		`<Enabled>%v</Enabled>` +
		`</StreamingDistributionConfig>`

	return fmt.Appendf([]byte(nil), tmpl, callerRef, comment, enabled)
}

// TestStreamingDistributionCRUD_HTTP covers the full HTTP lifecycle: create, get, get-config,
// list, update (with If-Match enforcement), and delete (with the not-disabled guard).
func TestStreamingDistributionCRUD_HTTP(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	h := newTestHandler()

	// Create (enabled).
	createBody := streamingDistConfigXML("sd-cr-1", "my streaming distribution", true)
	createRec := doXML(t, h, http.MethodPost, prefix+"streaming-distribution", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())
	assert.Contains(t, createRec.Body.String(), "<StreamingDistribution")
	assert.NotEmpty(t, createRec.Header().Get("ETag"))
	assert.Contains(t, createRec.Header().Get("Location"), "streaming-distribution/")

	id := extractXMLID(t, createRec.Body.String())
	require.NotEmpty(t, id)

	etag := createRec.Header().Get("ETag")

	// Get.
	getRec := doXML(t, h, http.MethodGet, prefix+"streaming-distribution/"+id, nil)
	require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
	assert.Contains(t, getRec.Body.String(), id)
	assert.Contains(t, getRec.Body.String(), "Deployed")
	assert.Equal(t, etag, getRec.Header().Get("ETag"))

	// GetStreamingDistributionConfig.
	getCfgRec := doXML(t, h, http.MethodGet, prefix+"streaming-distribution/"+id+"/config", nil)
	require.Equal(t, http.StatusOK, getCfgRec.Code, getCfgRec.Body.String())
	assert.Contains(t, getCfgRec.Body.String(), "sd-cr-1")

	// List.
	listRec := doXML(t, h, http.MethodGet, prefix+"streaming-distribution", nil)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())
	assert.Contains(t, listRec.Body.String(), id)
	assert.Contains(t, listRec.Body.String(), "videos.example.com")

	// Update with wrong If-Match -> PreconditionFailed.
	updateBody := streamingDistConfigXML("sd-cr-1", "updated comment", true)
	badMatchRec := doXMLWithHeaders(
		t, h, http.MethodPut, prefix+"streaming-distribution/"+id, updateBody,
		map[string]string{"If-Match": "bogus-etag"},
	)
	assert.Equal(t, http.StatusPreconditionFailed, badMatchRec.Code, badMatchRec.Body.String())
	assert.Contains(t, badMatchRec.Body.String(), "PreconditionFailed")

	// Update with correct If-Match -> succeeds, ETag rotates.
	okMatchRec := doXMLWithHeaders(
		t, h, http.MethodPut, prefix+"streaming-distribution/"+id, updateBody,
		map[string]string{"If-Match": etag},
	)
	require.Equal(t, http.StatusOK, okMatchRec.Code, okMatchRec.Body.String())
	assert.Contains(t, okMatchRec.Body.String(), "updated comment")
	newEtag := okMatchRec.Header().Get("ETag")
	assert.NotEmpty(t, newEtag)
	assert.NotEqual(t, etag, newEtag)

	// Delete while enabled -> StreamingDistributionNotDisabled (409).
	deleteWhileEnabledRec := doXML(t, h, http.MethodDelete, prefix+"streaming-distribution/"+id, nil)
	assert.Equal(t, http.StatusConflict, deleteWhileEnabledRec.Code, deleteWhileEnabledRec.Body.String())
	assert.Contains(t, deleteWhileEnabledRec.Body.String(), "StreamingDistributionNotDisabled")

	// Disable, then delete succeeds.
	disableBody := streamingDistConfigXML("sd-cr-1", "updated comment", false)
	disableRec := doXMLWithHeaders(
		t, h, http.MethodPut, prefix+"streaming-distribution/"+id, disableBody,
		map[string]string{"If-Match": newEtag},
	)
	require.Equal(t, http.StatusOK, disableRec.Code, disableRec.Body.String())

	deleteRec := doXML(t, h, http.MethodDelete, prefix+"streaming-distribution/"+id, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code, deleteRec.Body.String())

	// Get after delete -> NoSuchStreamingDistribution (404).
	getAfterDeleteRec := doXML(t, h, http.MethodGet, prefix+"streaming-distribution/"+id, nil)
	assert.Equal(t, http.StatusNotFound, getAfterDeleteRec.Code, getAfterDeleteRec.Body.String())
	assert.Contains(t, getAfterDeleteRec.Body.String(), "NoSuchStreamingDistribution")
}

// TestStreamingDistributionCRUD_NotFound verifies 404 NoSuchStreamingDistribution on
// Get/Update/Delete for an unknown ID.
func TestStreamingDistributionCRUD_NotFound(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	h := newTestHandler()

	getRec := doXML(t, h, http.MethodGet, prefix+"streaming-distribution/NOTEXIST", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code, getRec.Body.String())
	assert.Contains(t, getRec.Body.String(), "NoSuchStreamingDistribution")

	updateRec := doXML(
		t, h, http.MethodPut, prefix+"streaming-distribution/NOTEXIST",
		streamingDistConfigXML("cr", "c", false),
	)
	assert.Equal(t, http.StatusNotFound, updateRec.Code, updateRec.Body.String())
	assert.Contains(t, updateRec.Body.String(), "NoSuchStreamingDistribution")

	deleteRec := doXML(t, h, http.MethodDelete, prefix+"streaming-distribution/NOTEXIST", nil)
	assert.Equal(t, http.StatusNotFound, deleteRec.Code, deleteRec.Body.String())
	assert.Contains(t, deleteRec.Body.String(), "NoSuchStreamingDistribution")
}

// TestCreateStreamingDistributionWithTags_HTTP verifies the WithTags create variant applies
// tags via the shared tag store, retrievable through ListTagsForResource.
func TestCreateStreamingDistributionWithTags_HTTP(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"

	h := newTestHandler()

	body := `<StreamingDistributionConfigWithTags>` +
		`<StreamingDistributionConfig>` +
		`<CallerReference>sd-with-tags</CallerReference>` +
		`<S3Origin><DomainName>bucket.s3.amazonaws.com</DomainName></S3Origin>` +
		`<Comment>tagged streaming dist</Comment>` +
		`<Enabled>false</Enabled>` +
		`</StreamingDistributionConfig>` +
		`<Tags><Tag><Key>env</Key><Value>prod</Value></Tag></Tags>` +
		`</StreamingDistributionConfigWithTags>`

	rec := doXML(t, h, http.MethodPost, prefix+"streaming-distribution?Resource=WithTags", []byte(body))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	id := extractXMLID(t, rec.Body.String())
	require.NotEmpty(t, id)

	arn := fmt.Sprintf("arn:aws:cloudfront::123456789012:streaming-distribution/%s", id)
	tagsRec := doXML(t, h, http.MethodGet, prefix+"tagging?Resource="+arn, nil)
	require.Equal(t, http.StatusOK, tagsRec.Code, tagsRec.Body.String())
	assert.Contains(t, tagsRec.Body.String(), "<Key>env</Key>")
	assert.Contains(t, tagsRec.Body.String(), "<Value>prod</Value>")
}

// TestStreamingDistributionBackend exercises the in-memory backend directly, covering the
// create->get->update->list->delete round trip, idempotent create, not-found errors, and the
// disabled-before-delete guard.
func TestStreamingDistributionBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "list_empty",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.ListStreamingDistributions())
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetStreamingDistribution("NOTEXIST")
				require.Error(t, err)
				assert.Contains(t, err.Error(), "NoSuchStreamingDistribution")
			},
		},
		{
			name: "update_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateStreamingDistribution(
					"NOTEXIST", cloudfront.StreamingDistributionConfig{CallerReference: "cr"}, nil,
				)
				require.Error(t, err)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteStreamingDistribution("NOTEXIST")
				require.Error(t, err)
			},
		},
		{
			name: "full_lifecycle",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()

				cfg := cloudfront.StreamingDistributionConfig{
					CallerReference: "cr-lifecycle",
					Comment:         "initial",
					PriceClass:      "PriceClass_100",
					S3Origin: cloudfront.StreamingDistributionS3Origin{
						DomainName: "bucket.s3.amazonaws.com",
					},
					Aliases: []string{"videos.example.com"},
					TrustedSigners: cloudfront.StreamingDistributionTrustedSigners{
						Enabled: false,
					},
					Enabled: true,
				}

				sd, err := b.CreateStreamingDistribution(cfg, []byte("<StreamingDistributionConfig/>"))
				require.NoError(t, err)
				assert.NotEmpty(t, sd.ID)
				assert.NotEmpty(t, sd.ARN)
				assert.Equal(t, "Deployed", sd.Status)
				assert.Contains(t, sd.DomainName, ".cloudfront.net")
				assert.NotEmpty(t, sd.ETag)
				assert.NotEmpty(t, sd.LastModifiedTime)

				// Idempotent create: same CallerReference returns the same distribution.
				dup, err := b.CreateStreamingDistribution(cfg, nil)
				require.NoError(t, err)
				assert.Equal(t, sd.ID, dup.ID)
				before := len(b.ListStreamingDistributions())

				// Get.
				got, err := b.GetStreamingDistribution(sd.ID)
				require.NoError(t, err)
				assert.Equal(t, sd.ID, got.ID)
				assert.Equal(t, "initial", got.Config.Comment)

				// Update.
				updateCfg := cfg
				updateCfg.Comment = "updated"
				updateCfg.PriceClass = "PriceClass_All"
				updated, err := b.UpdateStreamingDistribution(
					sd.ID,
					updateCfg,
					[]byte("<StreamingDistributionConfig/>"),
				)
				require.NoError(t, err)
				assert.Equal(t, "updated", updated.Config.Comment)
				assert.Equal(t, "PriceClass_All", updated.Config.PriceClass)
				assert.Equal(t, "cr-lifecycle", updated.Config.CallerReference) // immutable
				assert.NotEqual(t, sd.ETag, updated.ETag)

				// List.
				list := b.ListStreamingDistributions()
				assert.Len(t, list, before)
				found := false
				for _, item := range list {
					if item.ID == sd.ID {
						found = true
					}
				}
				assert.True(t, found)

				// Delete while enabled fails.
				err = b.DeleteStreamingDistribution(sd.ID)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "StreamingDistributionNotDisabled")

				// Disable then delete succeeds.
				disabledCfg := updateCfg
				disabledCfg.Enabled = false
				_, err = b.UpdateStreamingDistribution(sd.ID, disabledCfg, nil)
				require.NoError(t, err)

				err = b.DeleteStreamingDistribution(sd.ID)
				require.NoError(t, err)

				_, err = b.GetStreamingDistribution(sd.ID)
				require.Error(t, err)
			},
		},
		{
			name: "tags_via_shared_tag_store",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()

				sd, err := b.CreateStreamingDistribution(
					cloudfront.StreamingDistributionConfig{CallerReference: "cr-tags"}, nil,
				)
				require.NoError(t, err)

				err = b.TagResource(sd.ARN, map[string]string{"env": "prod"})
				require.NoError(t, err)

				tags, err := b.ListTags(sd.ARN)
				require.NoError(t, err)
				assert.Equal(t, "prod", tags["env"])

				err = b.UntagResource(sd.ARN, []string{"env"})
				require.NoError(t, err)

				tags, err = b.ListTags(sd.ARN)
				require.NoError(t, err)
				assert.NotContains(t, tags, "env")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestStreamingDistributionSnapshotRestore verifies that streaming distribution state,
// including tags, survives a Snapshot/Restore round trip.
func TestStreamingDistributionSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")

	sd, err := b.CreateStreamingDistribution(cloudfront.StreamingDistributionConfig{
		CallerReference: "cr-snap",
		Comment:         "snapshot me",
		Enabled:         true,
	}, []byte("<StreamingDistributionConfig/>"))
	require.NoError(t, err)

	require.NoError(t, b.TagResource(sd.ARN, map[string]string{"team": "video"}))

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cloudfront.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	restored, err := b2.GetStreamingDistribution(sd.ID)
	require.NoError(t, err)
	assert.Equal(t, "snapshot me", restored.Config.Comment)

	tags, err := b2.ListTags(sd.ARN)
	require.NoError(t, err)
	assert.Equal(t, "video", tags["team"])

	// Idempotent create against the restored backend should return the same distribution.
	dup, err := b2.CreateStreamingDistribution(cloudfront.StreamingDistributionConfig{
		CallerReference: "cr-snap",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, sd.ID, dup.ID)

	// Deleting requires the restored distribution to still enforce the not-disabled guard.
	err = b2.DeleteStreamingDistribution(sd.ID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "StreamingDistributionNotDisabled")
}
