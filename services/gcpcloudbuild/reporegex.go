package gcpcloudbuild

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
)

const (
	refsFile  = "git-refs"
	lsLabel   = "Resolve revision"
	headsPfx  = "refs/heads/"
	tagsPfx   = "refs/tags/"
	peeledSfx = "^{}"
	lsScript  = `set -e
git ls-remote "$1" "$2" > ` + auxMount + `/` + refsFile
)

var errNoRefMatch = errors.New("no ref matches")

// compileRefRegex compiles a repo_source branch_name/tag_name, which the API defines as an RE2 expression.
func compileRefRegex(expr string) (*regexp.Regexp, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression %q: %w", expr, err)
	}

	return re, nil
}

// resolveRefRegex turns a repo_source branch_name or tag_name into the first ref it matches (an exact name wins,
// then names in sorted order). Other revisions are left as they are.
func (r *runner) resolveRefRegex(ctx context.Context, t gitTarget) (gitTarget, error) {
	var (
		expr, glob, pfx string
	)

	switch rev := t.repo.GetRevision().(type) {
	case *cloudbuildpb.RepoSource_BranchName:
		expr, glob, pfx = rev.BranchName, headsPfx+"*", headsPfx
	case *cloudbuildpb.RepoSource_TagName:
		expr, glob, pfx = rev.TagName, tagsPfx+"*", tagsPfx
	default:
		return t, nil
	}

	re, err := compileRefRegex(expr)
	if err != nil {
		return t, err
	}

	if err = os.Remove(filepath.Join(r.auxDir(), refsFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return t, fmt.Errorf("clear refs: %w", err)
	}

	spec := r.gitSpec(t)
	spec.Cmd = []string{"-c", lsScript, "sh", t.url, glob}

	if err = r.runHelper(ctx, lsLabel, spec); err != nil {
		return t, err
	}

	names, err := readRefNames(filepath.Join(r.auxDir(), refsFile), pfx)
	if err != nil {
		return t, err
	}

	name, ok := pickRef(names, expr, re)
	if !ok {
		return t, fmt.Errorf("%w %q in %s/%s", errNoRefMatch, expr, t.repo.GetProjectId(), t.repo.GetRepoName())
	}

	t.revision = name

	return t, checkRevision(name)
}

func checkRevision(rev string) error {
	if len(rev) > maxRevisionLen || strings.HasPrefix(rev, "-") || strings.ContainsAny(rev, " \t\r\n\x00") {
		return errGitRevision
	}

	return nil
}

func readRefNames(file, pfx string) ([]string, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("read refs: %w", err)
	}

	defer func() { _ = f.Close() }()

	var names []string

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		_, ref, ok := strings.Cut(sc.Text(), "\t")
		name, hasPfx := strings.CutPrefix(ref, pfx)

		if ok && hasPfx && !strings.HasSuffix(name, peeledSfx) {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names, sc.Err() //nolint:wrapcheck // scanner error is self-describing
}

func pickRef(names []string, expr string, re *regexp.Regexp) (string, bool) {
	for _, n := range names {
		if n == expr {
			return n, true
		}
	}

	for _, n := range names {
		if re.MatchString(n) {
			return n, true
		}
	}

	return "", false
}
