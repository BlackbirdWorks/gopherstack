package gcpartifactregistry

import (
	"context"
	"net/http"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

// validateUpstream makes the GET that disable_upstream_validation skips: the upstream must answer, and an
// upstream that rejects the supplied credentials fails the create. It only runs when upstream fetching is on.
func (b *InMemoryBackend) validateUpstream(ctx context.Context, ref repoRef, pb *artifactregistrypb.Repository) error {
	cfg := pb.GetRemoteRepositoryConfig()
	if pb.GetMode() != artifactregistrypb.Repository_REMOTE_REPOSITORY || cfg.GetDisableUpstreamValidation() {
		return nil
	}

	b.mu.RLock("validateUpstream")
	enabled := b.remote.enabled
	b.mu.RUnlock()

	base, target, ok := validationTarget(repoCtx{ref: ref, pb: pb})
	if !enabled || !ok {
		return nil
	}

	ctx, err := b.withUpstreamAuth(ctx, repoCtx{ref: ref, pb: pb}, base)
	if err != nil {
		return errInvalid("Upstream validation failed: %v. Set disable_upstream_validation to skip it.", err)
	}

	_, code, _, err := b.fetchWith(ctx, target, "", "")
	if err != nil {
		return errInvalid("Upstream validation failed: %v. Set disable_upstream_validation to skip it.", err)
	}

	hasCreds := cfg.GetUpstreamCredentials().GetUsernamePasswordCredentials().GetUsername() != ""

	rejected := code == http.StatusUnauthorized || code == http.StatusForbidden
	if code >= http.StatusInternalServerError || (hasCreds && rejected) {
		return errInvalid(
			"Upstream validation failed: %s answered HTTP %d. Set disable_upstream_validation to skip it.",
			target,
			code,
		)
	}

	return nil
}

// validationTarget is the upstream base and the URL to probe; ok is false when the repository names no upstream.
func validationTarget(rc repoCtx) (string, string, bool) {
	if rc.pb.GetFormat() == artifactregistrypb.Repository_DOCKER {
		base, _, err := dockerUpstream(rc)

		return base, base + "/v2/", err == nil
	}

	base, err := upstreamBase(rc.pb.GetRemoteRepositoryConfig())

	return base, base, err == nil
}
