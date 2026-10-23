// Package registryauth supplies container-registry credentials and mirror
// rewriting for every image pull Gopherstack performs.
package registryauth

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidSetting is returned for malformed registry settings; it never
// echoes the offending value because it may carry a password.
var ErrInvalidSetting = errors.New("invalid docker registry setting")

// Settings is embedded in the root CLI.
type Settings struct {
	Auth   []string `name:"docker-registry-auth"   env:"GOPHERSTACK_DOCKER_REGISTRY_AUTH"   sep:"none" help:"Registry credentials host=user:pass (repeatable)."`                                                      //nolint:lll // config struct tags are intentionally verbose
	Mirror []string `name:"docker-registry-mirror" env:"GOPHERSTACK_DOCKER_REGISTRY_MIRROR"            help:"Pull mirrors as registry=mirror-host, e.g. docker.io=mirror.example.com (comma-separated or repeated)."` //nolint:lll // config struct tags are intentionally verbose
}

type credential struct {
	user string
	pass string
}

func splitEntries(entries []string) []string {
	var out []string

	for _, e := range entries {
		for line := range strings.SplitSeq(e, "\n") {
			if line = strings.TrimSpace(line); line != "" {
				out = append(out, line)
			}
		}
	}

	return out
}

func parseAuth(entries []string) (map[string]credential, error) {
	out := make(map[string]credential)

	for _, e := range splitEntries(entries) {
		host, cred, ok := strings.Cut(e, "=")
		user, pass, ok2 := strings.Cut(cred, ":")

		if !ok || !ok2 || strings.TrimSpace(host) == "" || user == "" {
			return nil, fmt.Errorf("%w: auth entry must be host=user:pass", ErrInvalidSetting)
		}

		out[normalizeHost(host)] = credential{user: user, pass: pass}
	}

	return out, nil
}

func parseMirrors(entries []string) (map[string]string, error) {
	out := make(map[string]string)

	for _, e := range entries {
		for part := range strings.SplitSeq(e, ",") {
			if part = strings.TrimSpace(part); part == "" {
				continue
			}

			src, dst, ok := strings.Cut(part, "=")
			dst = cleanMirror(dst)

			if !ok || strings.TrimSpace(src) == "" || dst == "" {
				return nil, fmt.Errorf("%w: mirror entry must be registry=mirror-host", ErrInvalidSetting)
			}

			out[normalizeHost(src)] = dst
		}
	}

	return out, nil
}

func cleanMirror(dst string) string {
	dst = strings.TrimSpace(dst)
	dst = strings.TrimPrefix(strings.TrimPrefix(dst, "https://"), "http://")

	return strings.TrimSuffix(dst, "/")
}
