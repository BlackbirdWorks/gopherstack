package azurefunctions

import (
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// homeDirs are the directories every Linux app has directly under /home besides the site content.
const (
	filesPerRun = 2
	homeSite    = "site"
	homeLogs    = "LogFiles"
	homeData    = "data"
)

func homeDirs() []string { return []string{homeLogs, homeData, homeSite} }

type homeFile struct {
	mod  time.Time
	path string
	data string
}

// homeFiles are the files below /home outside site/wwwroot that the emulator holds: WebJob run logs.
func (b *InMemoryBackend) homeFiles(name string) []homeFile {
	runs := b.webJobRuns(name, "")
	out := make([]homeFile, 0, len(runs)*filesPerRun)

	for _, run := range runs {
		dir := "data/jobs/triggered/" + run.Job + "/" + run.ID + "/"
		out = append(out,
			homeFile{path: dir + "output_" + run.ID + ".log", data: run.Output, mod: run.Start},
			homeFile{path: dir + "error_" + run.ID + ".log", data: run.Error, mod: run.Start},
		)
	}

	for job, log := range b.continuousLogs(name) {
		out = append(
			out,
			homeFile{path: "data/jobs/continuous/" + job + "/job_log.txt", data: string(log), mod: b.now()},
		)
	}

	return out
}

func homeEntry(r *http.Request, rel, name string, isDir bool, f homeFile) vfsEntry {
	ts := f.mod.UTC().Format(time.RFC3339Nano)
	href := "http://" + r.Host + kuduVFS + rel + name

	e := vfsEntry{
		Name: name, MTime: ts, CTime: ts, MIME: mimeOf(name), Href: href, Path: vfsHomePath + rel + name,
		Size: int64(len(f.data)),
	}
	if isDir {
		e.Size, e.MIME, e.Href = 0, dirMIME, href+"/"
	}

	return e
}

// vfsHome serves /api/vfs/ paths outside site/wwwroot: the /home skeleton and the files the emulator holds.
func (b *InMemoryBackend) vfsHome(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "only site/wwwroot can be changed through the emulator's vfs", http.StatusForbidden)

		return
	}

	p := strings.Trim(strings.TrimPrefix(r.URL.Path, strings.TrimSuffix(kuduVFS, "/")), "/")
	files := b.homeFiles(name)

	for _, f := range files {
		if f.path == p {
			w.Header().Set("ETag", etagOf([]byte(f.data)))
			w.Header().Set("Content-Type", mimeOf(f.path))
			_, _ = w.Write([]byte(f.data))

			return
		}
	}

	prefix := ""
	if p != "" {
		prefix = p + "/"
	}

	seen := map[string]vfsEntry{}

	if p == "" {
		for _, d := range homeDirs() {
			seen[d] = homeEntry(r, prefix, d, true, homeFile{mod: time.Unix(0, 0)})
		}
	}

	if p == homeSite {
		seen["wwwroot"] = homeEntry(r, prefix, "wwwroot", true, homeFile{mod: time.Unix(0, 0)})
	}

	for _, f := range files {
		rest, ok := strings.CutPrefix(f.path, prefix)
		if !ok {
			continue
		}

		head, _, nested := strings.Cut(rest, "/")
		seen[head] = homeEntry(r, prefix, head, nested, f)
	}

	if len(seen) == 0 && !isHomeDir(p) {
		http.NotFound(w, r)

		return
	}

	out := make([]vfsEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}

	sort.Slice(out, func(i, j int) bool { return strs.Fold(out[i].Name) < strs.Fold(out[j].Name) })

	writeJSON(w, out)
}

func isHomeDir(p string) bool {
	top, _, _ := strings.Cut(p, "/")

	return p == "" || p == top && slices.Contains(homeDirs(), top)
}
