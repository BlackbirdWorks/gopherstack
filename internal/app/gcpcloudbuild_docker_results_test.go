package app

import (
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gcpstoragebackend "github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestGCPCloudBuild_Docker_ManifestSourceAndStepResults(t *testing.T) {
	t.Parallel()

	e := newBuildEnv(t)

	const project = "cb-docker-results"

	_, err := e.gcs.CreateBucket(project, gcpstoragebackend.Patch{"name": []byte(`"cb-m"`)})
	require.NoError(t, err)

	for name, body := range map[string]string{
		"blob":          "FROM scratch\n",
		"manifest.json": `{"dir/Dockerfile":{"sourceUrl":"gs://cb-m/blob"}}`,
	} {
		_, err = e.gcs.PutObject("cb-m", name, []byte(body), gcpstoragebackend.Patch{}, gcpstoragebackend.Preconds{})
		require.NoError(t, err)
	}

	started := e.create(t, project, &cloudbuildpb.Build{
		Source: &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSourceManifest{
			StorageSourceManifest: &cloudbuildpb.StorageSourceManifest{Bucket: "cb-m", Object: "manifest.json"},
		}},
		Substitutions: map[string]string{"_WHO": "world"},
		Options: &cloudbuildpb.BuildOptions{
			AutomapSubstitutions: true,
			SourceProvenanceHash: []cloudbuildpb.Hash_HashType{cloudbuildpb.Hash_SHA256, cloudbuildpb.Hash_MD5},
		},
		Steps: []*cloudbuildpb.BuildStep{
			shStep("a", `printf '%s' "$$_WHO:$$PROJECT_ID" > $$BUILDER_OUTPUT/output; wc -c < dir/Dockerfile`),
		},
	})

	got := e.finished(t, started)
	require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

	require.Len(t, got.GetResults().GetBuildStepOutputs(), 1)
	assert.Equal(t, "world:"+project, string(got.GetResults().GetBuildStepOutputs()[0]))
	assert.Contains(t, e.logLines(t, project), `Step #0 - "a": 13`)

	require.Len(t, got.GetResults().GetBuildStepImages(), 1)
	assert.Regexp(t, `^sha256:[0-9a-f]{64}$`, got.GetResults().GetBuildStepImages()[0])

	assert.Equal(t, "manifest.json", got.GetSourceProvenance().GetResolvedStorageSourceManifest().GetObject())
	assert.NotZero(t, got.GetSourceProvenance().GetResolvedStorageSourceManifest().GetGeneration())
	assert.Len(t, got.GetSourceProvenance().GetFileHashes(), 1)

	assert.Contains(t, got.GetTiming(), "FETCHSOURCE")
	assert.NotNil(t, got.GetSteps()[0].GetPullTiming())
}
