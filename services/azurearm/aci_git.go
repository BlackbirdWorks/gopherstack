package azurearm

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const aciGitImage = "alpine/git"

const aciGitScript = `set -e
git config --global --add safe.directory '*'
cd /vol
d="${ACI_DIR:-$(basename "$ACI_REPO" .git)}"
git clone --quiet -- "$ACI_REPO" "$d"
if [ -n "$ACI_REV" ]; then git -C "$d" checkout --quiet "$ACI_REV"; fi
chmod -R a+rwX /vol`

// fillGitRepo clones a gitRepo volume into dir with a short-lived git container, so the host needs no git. The
// repository URL may carry credentials and is scrubbed from any failure reason.
func (e *ACIEngine) fillGitRepo(run *aciRun, v aciVolumeWire, dir string) string {
	g := v.GitRepo

	e.mu.RLock("fillGitRepo")
	limit := e.startup
	e.mu.RUnlock()

	ctx, cancel := context.WithTimeout(run.ctx, limit)
	defer cancel()

	scrub := func(s string) string { return strings.ReplaceAll(s, g.Repository, "<repository>") }

	if _, err := run.rt.Pull(ctx, aciGitImage, nil); err != nil {
		return fmt.Sprintf("gitRepo volume %q: pull %s: %s", v.Name, aciGitImage, scrub(err.Error()))
	}

	name := fmt.Sprintf("gopherstack-aci-git-%s%d", e.nonce, e.seq.Add(1))

	id, err := run.rt.Start(ctx, container.Spec{
		Image: aciGitImage, Name: name, Entrypoint: []string{"sh", "-c", aciGitScript},
		Env:    []string{"ACI_REPO=" + g.Repository, "ACI_DIR=" + g.Directory, "ACI_REV=" + g.Revision},
		Mounts: []string{dir + ":/vol"},
	})
	if err != nil {
		return fmt.Sprintf("gitRepo volume %q: start git: %s", v.Name, scrub(err.Error()))
	}

	defer func() { _ = run.rt.Remove(context.WithoutCancel(ctx), id) }()

	code, err := run.rt.Wait(ctx, id)
	if err != nil {
		return fmt.Sprintf("gitRepo volume %q: %s", v.Name, scrub(err.Error()))
	}

	if code != 0 {
		logs, _ := run.rt.Logs(ctx, id, "20")

		return fmt.Sprintf("gitRepo volume %q: git exited with code %d: %s", v.Name, code,
			strings.TrimSpace(scrub(logs)))
	}

	return ""
}
