package azurefunctions

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

const (
	kuduZipDeploy   = "/api/zipdeploy"
	kuduPublish     = "/api/publish"
	kuduDeployments = "/api/deployments"
	kuduZip         = "/api/zip/site/wwwroot"
	kuduLatest      = "latest"
	statusSuccess   = 4
)

func isKuduPath(p string) bool {
	for _, prefix := range []string{kuduZipDeploy, kuduPublish, kuduDeployments, kuduZip} {
		if p == prefix || strings.HasPrefix(p, prefix+"/") {
			return true
		}
	}

	return false
}

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
	}

	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
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
		http.NotFound(w, r)
	}
}

func (b *InMemoryBackend) kuduPublish(w http.ResponseWriter, r *http.Request, name string) {
	if t := r.URL.Query().Get("type"); !strings.EqualFold(t, "zip") {
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

	d, err := b.Deploy(name, data, deployer, "Created via a push deployment")
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, ErrAppNotFound) {
			code = http.StatusNotFound
		}

		http.Error(w, err.Error(), code)

		return
	}

	w.Header().Set("Location", "http://"+r.Host+kuduDeployments+"/"+kuduLatest)

	if strings.EqualFold(r.URL.Query().Get("isAsync"), "true") {
		w.WriteHeader(http.StatusAccepted)

		return
	}

	writeJSON(w, http.StatusOK, toDeploymentJSON(r, name, d, true))
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

func (b *InMemoryBackend) kuduList(w http.ResponseWriter, r *http.Request, name string) {
	ds, err := b.Deployments(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	out := make([]deploymentJSON, 0, len(ds))
	for i, d := range slices.Backward(ds) {
		out = append(out, toDeploymentJSON(r, name, d, i == len(ds)-1))
	}

	writeJSON(w, http.StatusOK, out)
}

func (b *InMemoryBackend) kuduGet(w http.ResponseWriter, r *http.Request, name, id string) {
	ds, err := b.Deployments(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	for i, d := range slices.Backward(ds) {
		if id == kuduLatest && i == len(ds)-1 || d.ID == id {
			writeJSON(w, http.StatusOK, toDeploymentJSON(r, name, d, i == len(ds)-1))

			return
		}
	}

	http.NotFound(w, r)
}
