package codebuild

import (
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// redactURL drops userinfo, query and fragment so tokens embedded in source URLs never reach the log.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparseable>"
	}

	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""

	return u.String()
}

// logSourceStatus records the commit status CodeBuild would send to the Git host; the emulator has no
// Git-host API to call.
func (r *buildRunner) logSourceStatus(state string) {
	src := r.build.Source
	if src == nil || !src.ReportBuildStatus || !isGitSourceType(src.Type) {
		return
	}

	attrs := []any{
		"buildId", r.build.ID, "sourceType", src.Type, "location", redactURL(src.Location),
		"commit", r.build.ResolvedSourceVersion, "state", state,
	}

	if c := src.BuildStatusConfig; c != nil {
		attrs = append(attrs, "context", c.Context, "targetUrl", redactURL(c.TargetURL))
	}

	logger.Load(r.ctx).InfoContext(r.ctx, "CodeBuild: would report build status to the Git host", attrs...)
}
