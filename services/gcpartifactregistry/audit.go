package gcpartifactregistry

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func auditPolicy() gcpaudit.Policy {
	dataWrites := []string{
		"CreateTag", "UpdateTag", "DeleteTag", "DeletePackage", "UpdatePackage", "DeleteVersion", "UpdateVersion",
		"BatchDeleteVersions", "DeleteFile", "UpdateFile", "CreateAttachment", "DeleteAttachment", "CreateRule",
		"UpdateRule", "DeleteRule", "ImportAptArtifacts", "ImportYumArtifacts", "ExportArtifact",
	}

	data := make(map[string]gcpaudit.Kind, len(dataWrites))
	for _, m := range dataWrites {
		data[m] = gcpaudit.DataWrite
	}

	return gcpaudit.Policy{
		ServiceName: "artifactregistry.googleapis.com",
		AdminPrefixes: []string{
			"CreateRepository", "UpdateRepository", "DeleteRepository", "SetIamPolicy", "UpdateProjectSettings",
			"UpdateVPCSCConfig",
		},
		Data: data,
	}
}

func (b *InMemoryBackend) auditNow() time.Time {
	b.mu.RLock("auditNow")
	defer b.mu.RUnlock()

	return b.now()
}

const (
	artifactRegistryService = "artifactregistry.googleapis.com"
	cleanupMethod           = "google.devtools.artifactregistry.v1.ArtifactRegistry.BatchDeleteVersions"
	projectNumberMod        = 1_000_000_000_000
)

// cleanupPrincipal is the Artifact Registry service agent, which the cleanup-policy page names as the actor.
func cleanupPrincipal(project string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(project))

	return fmt.Sprintf("service-%012d@gcp-sa-artifactregistry.iam.gserviceaccount.com", h.Sum64()%projectNumberMod)
}

// auditCleanup records the BatchDeleteVersions call a cleanup run makes for a repository; a dry run sets
// validateOnly in the request, as docs.cloud.google.com/artifact-registry/docs/repositories/cleanup-policy says.
func (b *InMemoryBackend) auditCleanup(ctx context.Context, ref repoRef, versions []string, dryRun bool) {
	if len(versions) == 0 || !b.Audit.Enabled() {
		return
	}

	parent := ref.full() + "/packages/-"

	names := make([]any, 0, len(versions))
	for _, v := range versions {
		names = append(names, v)
	}

	req, err := structpb.NewStruct(map[string]any{"parent": parent, "names": names, "validateOnly": dryRun})
	if err != nil {
		return
	}

	b.Audit.Emit(ctx, gcpaudit.Record{
		Time: b.auditNow(), Project: ref.project, ServiceName: artifactRegistryService, MethodName: cleanupMethod,
		ResourceName: parent, Principal: cleanupPrincipal(ref.project), Permission: "artifactregistry.versions.delete",
		Kind: gcpaudit.DataWrite, Request: req,
	})
}
