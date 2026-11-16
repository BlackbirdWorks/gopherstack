package neptune

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	systemPath          = "/system"
	actionResetDatabase = "neptune-db:ResetDatabase"
	resetInitiate       = "initiateDatabaseReset"
	resetPerform        = "performDatabaseReset"
	resetTokenTTL       = time.Hour
	formCT              = "application/x-www-form-urlencoded"
)

// resetTokens holds the single outstanding fast-reset token; it lives in memory only, as a server restart voids it.
type resetTokens struct {
	issued time.Time
	now    func() time.Time
	mu     *lockmetrics.RWMutex
	token  string
}

func newResetTokens() *resetTokens {
	return &resetTokens{mu: lockmetrics.New("neptune-reset-token"), now: time.Now}
}

func (t *resetTokens) issue() string {
	t.mu.Lock("issue")
	defer t.mu.Unlock()

	t.token, t.issued = uuid.NewString(), t.now()

	return t.token
}

// consume reports whether tok is the latest unexpired token, and voids it.
func (t *resetTokens) consume(tok string) bool {
	t.mu.Lock("consume")
	defer t.mu.Unlock()

	ok := t.token != "" && tok == t.token && t.now().Sub(t.issued) <= resetTokenTTL
	if ok {
		t.token = ""
	}

	return ok
}

func (f *cypherFront) serveSystem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeCypherError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "Bad route: "+r.URL.Path)

		return
	}

	params, err := readSystemParams(w, r)
	if err != nil {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam, err.Error())

		return
	}

	action := params["action"]
	if action == "" {
		writeCypherError(w, http.StatusBadRequest, codeMissingParam, "Missing required parameters: [action]")

		return
	}

	if action != resetInitiate && action != resetPerform {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam,
			"System command parameter 'action' has unsupported value '"+action+"'")

		return
	}

	if !authorizeActions(w, r, langSparql, actionResetDatabase) {
		return
	}

	if action == resetInitiate {
		writeCypherJSON(w, http.StatusOK, map[string]any{
			keyStatus: statusOK200, keyPayload: map[string]string{"token": f.resets.issue()},
		})

		return
	}

	f.performReset(w, params["token"])
}

func (f *cypherFront) performReset(w http.ResponseWriter, token string) {
	if token == "" {
		writeCypherError(w, http.StatusBadRequest, codeMissingParam, "Missing required parameters: [token]")

		return
	}

	if !f.resets.consume(token) {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam,
			"System command parameter 'token' : '"+token+"' does not match database reset token")

		return
	}

	if err := f.graph.Wipe(f.ctx); err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	f.rdf.store.Wipe()
	f.rdf.rdfStats.reset()
	f.rdf.pgStats.reset()
	writeCypherJSON(w, http.StatusOK, map[string]string{keyStatus: statusOK200})
}

type systemParamError string

func (e systemParamError) Error() string { return string(e) }

func readSystemParams(w http.ResponseWriter, r *http.Request) (map[string]string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, systemParamError("Unable to read the request body: " + err.Error())
	}

	if mediaType(r.Header.Get("Content-Type")) == formCT {
		vals, perr := url.ParseQuery(string(raw))
		if perr != nil {
			return nil, systemParamError("Malformed form body: " + perr.Error())
		}

		out := make(map[string]string, len(vals))

		for k, v := range vals {
			if len(v) > 1 {
				return nil, systemParamError("Too many values supplied for: " + k)
			}

			out[k] = v[0]
		}

		return out, nil
	}

	var m map[string]any
	if err = json.Unmarshal(raw, &m); err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			return nil, systemParamError("Duplicate field 'action'")
		}

		return nil, systemParamError("Malformed JSON body: " + err.Error())
	}

	out := make(map[string]string, len(m))

	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}

	return out, nil
}

func (s *statsState) reset() {
	s.mu.Lock("statsReset")
	defer s.mu.Unlock()

	s.active, s.id, s.sigs, s.insts, s.preds, s.summary = false, -1, 0, 0, 0, nil
}

func (s *statsState) metrics() (int, int) {
	s.mu.RLock("statsMetrics")
	defer s.mu.RUnlock()

	return s.preds, len(s.summary)
}
