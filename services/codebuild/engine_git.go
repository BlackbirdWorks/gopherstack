package codebuild

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	sourceTypeGitHub     = "GITHUB"
	sourceTypeGitHubEnt  = "GITHUB_ENTERPRISE"
	sourceTypeBitbucket  = "BITBUCKET"
	sourceTypeGitLab     = "GITLAB"
	sourceTypeGitLabSelf = "GITLAB_SELF_MANAGED"

	gitFetchHead   = "FETCH_HEAD"
	prVersionToken = "pr/"
	gitProtocols   = "https:http:git"
)

var (
	errGitMissing  = errors.New("git is not installed on the emulator host")
	errGitLocation = errors.New("invalid git source location")
	errGitFailed   = errors.New("git command failed")
)

func isGitSourceType(t string) bool {
	switch t {
	case sourceTypeGitHub, sourceTypeGitHubEnt, sourceTypeBitbucket, sourceTypeGitLab, sourceTypeGitLabSelf:
		return true
	default:
		return false
	}
}

// gitRef maps a CodeBuild sourceVersion onto a ref git can fetch.
func gitRef(serverType, version string) string {
	version = strings.TrimSpace(version)

	if version == "" {
		return "HEAD"
	}

	num, ok := strings.CutPrefix(version, prVersionToken)
	if !ok {
		return version
	}

	switch serverType {
	case sourceTypeGitLab, sourceTypeGitLabSelf:
		return "refs/merge-requests/" + num + "/head"
	case sourceTypeBitbucket:
		return "refs/pull-requests/" + num + "/from"
	default:
		return "refs/pull/" + num + "/head"
	}
}

func defaultGitUser(serverType string) string {
	switch serverType {
	case sourceTypeGitLab, sourceTypeGitLabSelf:
		return "oauth2"
	case sourceTypeBitbucket:
		return "x-token-auth"
	default:
		return "x-access-token"
	}
}

// gitClient runs git against one workspace; secrets only ever travel in env.
type gitClient struct {
	token string
	dir   string
	env   []string
}

type gitRequest struct {
	serverType string
	location   string
	version    string
	token      string
	username   string
	depth      int32
	submodules bool
	insecure   bool
	allowFile  bool
}

func newGitClient(dir string, req gitRequest) (*gitClient, error) {
	u, err := url.Parse(req.location)
	if err != nil || u.Scheme == "" || strings.HasPrefix(req.location, "-") {
		return nil, fmt.Errorf("%w: %q", errGitLocation, req.location)
	}

	protocols := gitProtocols
	if req.allowFile {
		protocols += ":file"
	}

	switch u.Scheme {
	case "https", "http", "git":
	case "file":
		if !req.allowFile {
			return nil, fmt.Errorf("%w: file:// sources are disabled", errGitLocation)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported scheme %q", errGitLocation, u.Scheme)
	}

	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + dir,
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_CONFIG_SYSTEM=" + os.DevNull,
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ALLOW_PROTOCOL=" + protocols,
	}

	if req.insecure {
		env = append(env, "GIT_SSL_NO_VERIFY=true")
	}

	return &gitClient{dir: dir, token: req.token, env: append(env, gitConfigEnv(u, req)...)}, nil
}

// gitConfigEnv expresses config as GIT_CONFIG_* env so the token never reaches argv.
func gitConfigEnv(u *url.URL, req gitRequest) []string {
	var kv [][2]string

	if req.token != "" && u.Scheme != "file" {
		user := req.username
		if user == "" {
			user = defaultGitUser(req.serverType)
		}

		cred := base64.StdEncoding.EncodeToString([]byte(user + ":" + req.token))
		scope := "http." + u.Scheme + "://" + u.Host + "/.extraHeader"
		kv = append(kv, [2]string{scope, "Authorization: Basic " + cred})
	}

	if req.allowFile {
		kv = append(kv, [2]string{"protocol.file.allow", "always"})
	}

	env := make([]string, 0, len(kv)*2+1)
	env = append(env, "GIT_CONFIG_COUNT="+strconv.Itoa(len(kv)))

	for i, p := range kv {
		n := strconv.Itoa(i)
		env = append(env, "GIT_CONFIG_KEY_"+n+"="+p[0], "GIT_CONFIG_VALUE_"+n+"="+p[1])
	}

	return env
}

func (g *gitClient) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = g.dir
	cmd.Env = g.env

	var out bytes.Buffer

	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return "", errGitMissing
		}

		msg := strings.TrimSpace(out.String())
		if g.token != "" {
			msg = strings.ReplaceAll(msg, g.token, "****")
		}

		return "", fmt.Errorf("%w: git %s: %s", errGitFailed, args[0], msg)
	}

	return strings.TrimSpace(out.String()), nil
}

// clone materialises the requested version in g.dir and returns the checked-out commit.
func (g *gitClient) clone(ctx context.Context, req gitRequest) (string, error) {
	if _, err := g.run(ctx, "init", "-q"); err != nil {
		return "", err
	}

	_, _ = g.run(ctx, "remote", "remove", "origin")

	if _, err := g.run(ctx, "remote", "add", "origin", req.location); err != nil {
		return "", err
	}

	fetch := []string{"fetch", "-q", "--no-tags", "--recurse-submodules=no"}
	if req.depth > 0 {
		fetch = append(fetch, "--depth", strconv.Itoa(int(req.depth)))
	}

	ref := gitRef(req.serverType, req.version)
	if strings.HasPrefix(ref, "-") {
		return "", fmt.Errorf("%w: ref %q", errGitLocation, ref)
	}

	if _, err := g.run(ctx, append(fetch, "origin", ref)...); err != nil {
		return "", err
	}

	if _, err := g.run(ctx, "checkout", "-q", "-f", "--detach", gitFetchHead); err != nil {
		return "", err
	}

	if req.submodules {
		if err := g.updateSubmodules(ctx, req.depth); err != nil {
			return "", err
		}
	}

	return g.run(ctx, "rev-parse", "HEAD")
}

func (g *gitClient) updateSubmodules(ctx context.Context, depth int32) error {
	args := []string{"submodule", "update", "--init", "--recursive"}
	if depth > 0 {
		args = append(args, "--depth", strconv.Itoa(int(depth)))
	}

	_, err := g.run(ctx, args...)

	return err
}

func (r *buildRunner) fetchGitSource(ctx context.Context, dir string) error {
	src := r.build.Source

	req := gitRequest{
		serverType: src.Type,
		location:   src.Location,
		version:    r.build.SourceVersion,
		depth:      src.GitCloneDepth,
		submodules: src.GitSubmodulesConfig != nil && src.GitSubmodulesConfig.FetchSubmodules,
		insecure:   src.InsecureSsl,
		allowFile:  r.cfg.AllowFileGitSources,
	}
	req.token, req.username, _ = r.b.sourceToken(src.Type)

	g, err := newGitClient(dir, req)
	if err != nil {
		return err
	}

	r.restoreSourceCache(dir)

	commit, err := g.clone(ctx, req)
	if err != nil {
		return fmt.Errorf("clone %s: %w", src.Location, err)
	}

	r.saveSourceCache(dir)
	r.setResolvedVersion(commit)

	return nil
}

func (r *buildRunner) setResolvedVersion(commit string) {
	r.b.mutateBuild(r.build.ID, func(bd *Build) { bd.ResolvedSourceVersion = commit })
}
