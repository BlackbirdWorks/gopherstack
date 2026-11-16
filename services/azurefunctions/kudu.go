package azurefunctions

import (
	"crypto/subtle"
	"encoding/json/v2"
	"errors"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	kuduZipDeploy   = "/api/zipdeploy"
	kuduPublish     = "/api/publish"
	kuduDeployments = "/api/deployments"
	kuduZip         = "/api/zip/site/wwwroot"
	kuduLatest      = "latest"
	statusSuccess   = 4
	statusFailed    = 3
)

type deploymentJSON struct {
	ID             string `json:"id"`
	StatusText     string `json:"status_text"`
	AuthorEmail    string `json:"author_email"`
	Author         string `json:"author"`
	Deployer       string `json:"deployer"`
	Message        string `json:"message"`
	Progress       string `json:"progress"`
	ReceivedTime   string `json:"received_time"`
	StartTime      string `json:"start_time"`
	EndTime        string `json:"end_time"`
	LastSuccessEnd string `json:"last_success_end_time,omitempty"`
	URL            string `json:"url"`
	LogURL         string `json:"log_url"`
	SiteName       string `json:"site_name"`
	Status         int    `json:"status"`
	Complete       bool   `json:"complete"`
	Active         bool   `json:"active"`
	IsTemp         bool   `json:"is_temp"`
	IsReadonly     bool   `json:"is_readonly"`
}

func toDeploymentJSON(r *http.Request, name string, d Deployment, active bool) deploymentJSON {
	ts := func(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
	base := "http://" + r.Host + kuduDeployments + "/" + d.ID

	out := deploymentJSON{
		ID:          d.ID,
		Status:      statusSuccess,
		AuthorEmail: "N/A",
		Author:      "N/A",
		Deployer:    d.Deployer,
		Message:     d.Message,
		ReceivedTime: ts(
			d.Received,
		),
		StartTime:  ts(d.Received),
		EndTime:    ts(d.Finished),
		Complete:   true,
		Active:     active,
		URL:        base,
		LogURL:     base + "/log",
		SiteName:   name,
		IsReadonly: true,
	}

	if d.Succeeded {
		out.LastSuccessEnd = ts(d.Finished)
	} else {
		out.Status, out.StatusText = statusFailed, "Failed"
	}

	return out
}

func writeBody(w http.ResponseWriter, v any) {
	if raw, err := json.Marshal(v, jsonx.V1()); err == nil {
		_, _ = w.Write(raw) // #nosec G705
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	raw, err := json.Marshal(v, jsonx.V1())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw) // #nosec G705
}

// authorized accepts the app's publishing credentials as Basic auth, or any Bearer token (Kudu trusts the
// ARM-issued token; the emulator does not validate it).
func (b *InMemoryBackend) authorized(r *http.Request, name string) bool {
	user, pass, ok := r.BasicAuth()
	if ok {
		wantUser, wantPass, _, err := b.PublishingCredentials(name)

		return err == nil && subtle.ConstantTimeCompare([]byte(user), []byte(wantUser)) == 1 &&
			subtle.ConstantTimeCompare([]byte(pass), []byte(wantPass)) == 1
	}

	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")

	return found && token != ""
}

func (b *InMemoryBackend) serveKudu(w http.ResponseWriter, r *http.Request, name string) {
	if !b.authorized(r, name) {
		w.Header().Set("WWW-Authenticate", `Basic realm="site"`)
		http.Error(w, "401 Unauthorized", http.StatusUnauthorized)

		return
	}

	p := strings.TrimSuffix(r.URL.Path, "/")

	if !b.routeKuduDeploy(w, r, name, p) && !b.routeKuduTools(w, r, name, p) {
		http.NotFound(w, r)
	}
}

// routeKuduDeploy serves the deployment endpoints; it reports whether the path was one of them.
func (b *InMemoryBackend) routeKuduDeploy(w http.ResponseWriter, r *http.Request, name, p string) bool {
	switch {
	case p == kuduZipDeploy:
		b.kuduDeploy(w, r, name, "ZipDeploy")
	case p == kuduPublish:
		b.kuduPublish(w, r, name)
	case p == kuduZip:
		b.kuduDownload(w, r, name)
	case p == kuduDeployments:
		b.kuduList(w, r, name)
	case strings.HasPrefix(p, kuduDeployments+"/"):
		b.kuduGet(w, r, name, strings.TrimPrefix(p, kuduDeployments+"/"))
	default:
		return false
	}

	return true
}

// routeKuduTools serves command, logstream, WebJobs and vfs; it reports whether the path was one of them.
func (b *InMemoryBackend) routeKuduTools(w http.ResponseWriter, r *http.Request, name, p string) bool {
	switch {
	case p == kuduCommand:
		b.kuduCommand(w, r, name)
	case p == kuduLogstream:
		b.kuduLogstream(w, r, name)
	case p == kuduContinuous || strings.HasPrefix(p, kuduContinuous+"/"):
		b.kuduContinuous(w, r, name, p)
	case p == kuduWebJobs || p == kuduTriggered || strings.HasPrefix(p, kuduTriggered+"/"):
		b.kuduWebJobs(w, r, name, p)
	case strings.HasPrefix(r.URL.Path, kuduVFS) || p == strings.TrimSuffix(kuduVFS, "/"):
		b.kuduVFS(w, r, name)
	default:
		return false
	}

	return true
}

func (b *InMemoryBackend) kuduPublish(w http.ResponseWriter, r *http.Request, name string) {
	q := r.URL.Query()
	t := q.Get("type")

	if t == "" && b.IsFlex(name) {
		t = "zip"
	}

	if !strings.EqualFold(t, "zip") {
		http.Error(w, "unsupported publish type "+t+"; the emulator supports type=zip", http.StatusBadRequest)

		return
	}

	b.kuduDeploy(w, r, name, "OneDeploy")
}

func (b *InMemoryBackend) kuduDeploy(w http.ResponseWriter, r *http.Request, name, deployer string) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "use POST to deploy a zip package", http.StatusMethodNotAllowed)

		return
	}

	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPackageBytes))
	if err != nil {
		http.Error(w, "read package: "+err.Error(), http.StatusRequestEntityTooLarge)

		return
	}

	data, buildLog, err := b.maybeBuild(r, name, data, deployer)
	if err != nil {
		code := http.StatusBadRequest
		switch {
		case errors.Is(err, ErrAppNotFound):
			code = http.StatusNotFound
		case errors.Is(err, errBuildFailed):
			code = http.StatusInternalServerError
		case errors.Is(err, errBuildNoExec), errors.Is(err, errNoRuntime):
			code = http.StatusServiceUnavailable
		}

		http.Error(w, err.Error()+"\n"+buildLog, code)

		return
	}

	d, err := b.deploy(name, data, deployer, "Created via a push deployment", buildLog)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, ErrAppNotFound) {
			code = http.StatusNotFound
		}

		http.Error(w, err.Error(), code)

		return
	}

	b.afterDeploy(name)

	w.Header().Set("Location", "http://"+r.Host+kuduDeployments+"/"+kuduLatest)

	if strings.EqualFold(r.URL.Query().Get("isAsync"), "true") {
		w.WriteHeader(http.StatusAccepted)

		return
	}

	writeJSON(w, toDeploymentJSON(r, name, d, true))
}

// maybeBuild runs the remote build when the deployment asks for one; a failed build is recorded as a failed
// deployment and leaves the running package alone.
func (b *InMemoryBackend) maybeBuild(
	r *http.Request, name string, data []byte, deployer string,
) ([]byte, string, error) {
	b.mu.RLock("maybeBuild")
	a, err := b.appLocked(name)

	var (
		settings map[string]string
		flex     bool
	)

	if err == nil {
		settings, flex = maps.Clone(a.Settings), a.Flex
	}
	b.mu.RUnlock()

	if err != nil {
		return nil, "", err
	}

	flag := strings.EqualFold(r.URL.Query().Get("RemoteBuild"), "true")
	if !wantsRemoteBuild(flag, settings, flex) {
		return data, "", nil
	}

	if err = validatePackage(data); err != nil {
		return nil, "", err
	}

	built, buildLog, err := b.Build(r.Context(), name, data)
	if err != nil {
		b.recordFailedDeployment(name, deployer, buildLog+"\n"+err.Error())

		return nil, buildLog, err
	}

	return built, buildLog, nil
}

func (b *InMemoryBackend) kuduDownload(w http.ResponseWriter, r *http.Request, name string) {
	data := b.Package(name)
	if data == nil {
		http.NotFound(w, r)

		return
	}

	w.Header().Set("Content-Type", "application/zip")
	_, _ = w.Write(data)
}

func activeIndex(ds []Deployment) int {
	for i, d := range slices.Backward(ds) {
		if d.Succeeded {
			return i
		}
	}

	return -1
}

func (b *InMemoryBackend) kuduList(w http.ResponseWriter, r *http.Request, name string) {
	ds, err := b.Deployments(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	active := activeIndex(ds)
	out := make([]deploymentJSON, 0, len(ds))

	for i, d := range slices.Backward(ds) {
		out = append(out, toDeploymentJSON(r, name, d, i == active))
	}

	writeJSON(w, out)
}

type logEntryJSON struct {
	DetailsURL *string `json:"details_url"`
	LogTime    string  `json:"log_time"`
	ID         string  `json:"id"`
	Message    string  `json:"message"`
	Type       int     `json:"type"`
}

func (b *InMemoryBackend) kuduGet(w http.ResponseWriter, r *http.Request, name, rest string) {
	id, wantLog := strings.CutSuffix(rest, "/log")

	ds, err := b.Deployments(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	active := activeIndex(ds)

	for i, d := range slices.Backward(ds) {
		if id != kuduLatest && d.ID != id || id == kuduLatest && i != len(ds)-1 {
			continue
		}

		if !wantLog {
			writeJSON(w, toDeploymentJSON(r, name, d, i == active))

			return
		}

		entries := []logEntryJSON{}

		for n, line := range strings.Split(d.Log, "\n") {
			if strings.TrimSpace(line) != "" {
				entries = append(entries, logEntryJSON{
					LogTime: d.Finished.UTC().Format(time.RFC3339Nano), ID: d.ID + "-" + strconv.Itoa(n), Message: line,
				})
			}
		}

		writeJSON(w, entries)

		return
	}

	http.NotFound(w, r)
}
