package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// materializeServiceDir checks out repoRoot's services/ecs tree exactly as
// it existed at git rev rev (git archive, not a working-tree copy) into a
// fresh temp dir, test files included -- resolveServiceModules needs them
// to find ecs's SDK import (see modresolve.go's own doc comment on why
// test files matter for module resolution).
func materializeServiceDir(t *testing.T, repoRoot, rev string) string {
	t.Helper()

	const svcRelPath = "services/ecs"

	dst := t.TempDir()

	archive := exec.CommandContext(context.Background(), "git", "archive", rev, "--", svcRelPath)
	archive.Dir = repoRoot

	pipe, err := archive.StdoutPipe()
	require.NoError(t, err)

	untar := exec.CommandContext(context.Background(), "tar", "-x", "-C", dst)
	untar.Stdin = pipe

	require.NoError(t, archive.Start())
	require.NoError(t, untar.Start())
	require.NoError(t, archive.Wait())
	require.NoError(t, untar.Wait())

	return filepath.Join(dst, svcRelPath)
}

// TestScanServiceDir_ECSValidationBar is this tool's validation bar: it
// must flag every one of the eleven error codes commit fa0e68c21 fixed in
// services/ecs (invented codes matching no real SDK type at all -- see
// main.go's doc comment) at the commit immediately before that fix, and it
// must flag NONE of them at the fix commit itself.
//
// errors.go's ServiceDeploymentAlreadyStoppedException is deliberately
// excluded from elevenCodes: fa0e68c21 never touched it, and it is NOT a
// real ecs SDK code either (ecs@v1.90.0 models
// ServiceDeploymentNotFoundException, never an "AlreadyStopped" variant) --
// a twelfth invented code the original hand sweep missed, which this tool
// still confidently flags at the fix commit. See
// TestScanServiceDir_ECSStillFlagsTwelfthCode below.
func TestScanServiceDir_ECSValidationBar(t *testing.T) {
	t.Parallel()

	repoRoot, err := repoRootDir()
	require.NoError(t, err)

	cache, err := gomodcacheDir(repoRoot)
	require.NoError(t, err)

	goModVersions, err := loadGoModVersions(filepath.Join(repoRoot, "go.mod"))
	require.NoError(t, err)

	elevenCodes := []string{
		"TaskNotFoundException",
		"ClusterAlreadyExistsException",
		"CapacityProviderNotFoundException",
		"CapacityProviderAlreadyExistsException",
		"TaskDefinitionNotFoundException",
		"ServiceAlreadyExistsException",
		"ContainerInstanceNotFoundException",
		"ExpressGatewayServiceNotFoundException",
		"ExpressGatewayServiceAlreadyExistsException",
		"AccountSettingNotFoundException",
	}

	t.Run("pre-fix flags all eleven invented codes", func(t *testing.T) {
		t.Parallel()

		dir := materializeServiceDir(t, repoRoot, "fa0e68c21^")

		findings, scanErr := scanServiceDir(dir, repoRoot, cache, goModVersions)
		require.NoError(t, scanErr)

		flagged := map[string]bool{}

		for _, f := range findings {
			if f.Confident {
				flagged[f.Code] = true
			}
		}

		for _, code := range elevenCodes {
			require.Truef(
				t,
				flagged[code],
				"expected pre-fix ecs to confidently flag %s, findings: %+v",
				code,
				findings,
			)
		}
	})

	t.Run("post-fix flags none of the eleven", func(t *testing.T) {
		t.Parallel()

		dir := materializeServiceDir(t, repoRoot, "fa0e68c21")

		findings, scanErr := scanServiceDir(dir, repoRoot, cache, goModVersions)
		require.NoError(t, scanErr)

		for _, f := range findings {
			for _, code := range elevenCodes {
				require.NotEqualf(
					t,
					code,
					f.Code,
					"post-fix ecs must not flag %s, but got: %+v",
					code,
					f,
				)
			}
		}
	})

	t.Run("post-fix flags no generic protocol codes", func(t *testing.T) {
		t.Parallel()

		dir := materializeServiceDir(t, repoRoot, "fa0e68c21")

		findings, scanErr := scanServiceDir(dir, repoRoot, cache, goModVersions)
		require.NoError(t, scanErr)

		for _, f := range findings {
			require.Falsef(
				t, genericProtocolCodes[f.Code],
				"generic protocol code %s should never reach classify as a finding: %+v", f.Code, f,
			)
		}
	})
}

// TestScanServiceDir_ECSStillFlagsTwelfthCode documents a real finding this
// tool made during calibration: services/ecs/errors.go's
// ServiceDeploymentAlreadyStoppedException is a code fa0e68c21 never
// touched (it wasn't part of that commit's diff) and that names no real
// ecs@v1.90.0 SDK type either -- confirmed by hand against
// types/errors.go, which declares ServiceDeploymentNotFoundException, never
// an "AlreadyStopped" variant. Fixing it is out of scope for this tool
// (Part 3 of its brief is report-only), but the finding must keep
// surfacing at the pinned fix commit so this regresses loudly if a future
// ground-truth change ever silently swallows it.
func TestScanServiceDir_ECSStillFlagsTwelfthCode(t *testing.T) {
	t.Parallel()

	repoRoot, err := repoRootDir()
	require.NoError(t, err)

	cache, err := gomodcacheDir(repoRoot)
	require.NoError(t, err)

	goModVersions, err := loadGoModVersions(filepath.Join(repoRoot, "go.mod"))
	require.NoError(t, err)

	dir := materializeServiceDir(t, repoRoot, "fa0e68c21")

	findings, err := scanServiceDir(dir, repoRoot, cache, goModVersions)
	require.NoError(t, err)

	for _, f := range findings {
		if f.Code == "ServiceDeploymentAlreadyStoppedException" && f.Confident {
			return
		}
	}

	t.Fatalf(
		"expected a confident finding for ServiceDeploymentAlreadyStoppedException, got: %+v",
		findings,
	)
}

// TestScanServiceDir_SkipsNoGroundTruth confirms ec2 -- whose OWN pinned
// SDK module models zero error codes at all (see moduleCodes's doc
// comment) -- never produces a CONFIDENT finding, matching commit
// fa0e68c21's own documented conclusion that ec2 needed no change because
// there was nothing to check against. It may still produce NEEDS-REVIEW
// findings: one *_test.go file imports outposts for an unrelated
// cross-service integration test, which makes resolvedModules 2 (ec2 +
// outposts) and demotes anything found there rather than silently
// checking ec2's own emissions against outposts's exception set (see
// serviceGroundTruth's doc comment) -- that demotion, not silence, is the
// behavior under test here.
func TestScanServiceDir_SkipsNoGroundTruth(t *testing.T) {
	t.Parallel()

	repoRoot, err := repoRootDir()
	require.NoError(t, err)

	cache, err := gomodcacheDir(repoRoot)
	require.NoError(t, err)

	goModVersions, err := loadGoModVersions(filepath.Join(repoRoot, "go.mod"))
	require.NoError(t, err)

	entries, err := os.ReadDir(filepath.Join(repoRoot, "services", "ec2"))
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	findings, err := scanServiceDir(
		filepath.Join(repoRoot, "services", "ec2"),
		repoRoot,
		cache,
		goModVersions,
	)
	require.NoError(t, err)

	for _, f := range findings {
		require.Falsef(
			t,
			f.Confident,
			"ec2 has no ground truth of its own to check against; got confident finding: %+v",
			f,
		)
	}
}
