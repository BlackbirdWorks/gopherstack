package gcpartifactregistry

import (
	"context"
	"sort"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API exposes the Artifact Registry service plus file bytes and cleanup previews to the dashboard proxy.
type API struct {
	artifactregistrypb.ArtifactRegistryServer
	b *InMemoryBackend
}

// API returns the in-process Artifact Registry API.
func (h *Handler) API() *API {
	return &API{ArtifactRegistryServer: &server{b: h.Backend}, b: h.Backend}
}

// FileContent returns the bytes of a file, refusing files larger than maxBytes before reading them.
func (a *API) FileContent(ctx context.Context, name string, maxBytes int64) ([]byte, error) {
	f, err := a.b.GetFile(ctx, name)
	if err != nil {
		return nil, err
	}

	if f.GetSizeBytes() > maxBytes {
		return nil, status.Error(codes.FailedPrecondition, "file is larger than the dashboard download limit")
	}

	return a.b.fileContent(ctx, name)
}

// PreviewCleanup lists the versions the repository's cleanup policies would delete now, without deleting anything.
func (a *API) PreviewCleanup(ctx context.Context, repo string) ([]string, error) {
	pb, err := a.b.GetRepository(repo)
	if err != nil {
		return nil, err
	}

	ref, err := parseRepo(repo)
	if err != nil {
		return nil, err
	}

	if len(pb.GetCleanupPolicies()) == 0 || pb.GetMode() == artifactregistrypb.Repository_VIRTUAL_REPOSITORY {
		return []string{}, nil
	}

	policies := make([]*artifactregistrypb.CleanupPolicy, 0, len(pb.GetCleanupPolicies()))
	for _, p := range pb.GetCleanupPolicies() {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].GetId() < policies[j].GetId() })

	a.b.mu.RLock("PreviewCleanup")
	now := a.b.now()
	a.b.mu.RUnlock()

	doomed := selectForDeletion(
		a.b.cleanupCandidates(ctx, ref, pb.GetFormat()), policies, now, pb.GetDockerConfig().GetImmutableTags(),
	)

	out := make([]string, 0, len(doomed))
	for _, v := range doomed {
		out = append(out, v.leaf.pkg.full()+"/versions/"+v.version)
	}

	sort.Strings(out)

	return out, nil
}
