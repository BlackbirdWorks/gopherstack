// Package testdepthlimit is synthetic fixture data for
// TestAnalyzeDir_DelegationShapes -- a five-hop call chain proving
// chaseClaims stops recursing at chaseDepthLimit: f4 sits exactly at the
// limit, so its own "/depth-four" claim is still extracted, but its call
// into f5 is never followed, so "/depth-five" must not appear.
package testdepthlimit

type Handler struct{}

func (h *Handler) RouteMatcher() bool {
	return f1("")
}

func (h *Handler) MatchPriority() int { return 85 }

func f1(path string) bool { return f2(path) }

func f2(path string) bool { return f3(path) }

func f3(path string) bool { return f4(path) }

func f4(path string) bool {
	if path == "/depth-four" {
		return true
	}

	return f5(path)
}

func f5(path string) bool { return path == "/depth-five" }
