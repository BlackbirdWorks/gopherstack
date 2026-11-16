package neptune

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	summaryPGPath      = "/propertygraph/statistics/summary"
	summaryPGShortPath = "/pg/statistics/summary"
	summaryRDFPath     = "/rdf/statistics/summary"
	actionGraphSummary = "neptune-db:GetGraphSummary"
	summaryVersion     = "v1"
	summaryModeBasic   = "basic"
	summaryModeDetail  = "detailed"
	summaryTimeLayout  = "2006-01-02T15:04:05.000Z"
	codeStatsMissing   = "StatisticsNotAvailableException"
)

func marshalSummary(v any) []byte {
	b, err := json.Marshal(v, jsonx.V1())
	if err != nil {
		return nil
	}

	return b
}

func (f *cypherFront) serveSummary(w http.ResponseWriter, r *http.Request, lang string, st *statsState) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeCypherError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "Method Not Allowed.")

		return
	}

	if !authorizeActions(w, r, lang, actionGraphSummary) {
		return
	}

	detailed, perr := parseSummaryParams(r.URL.Query())
	if perr != nil {
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam, *perr)

		return
	}

	st.mu.RLock("summary")
	active, date, raw := st.active, st.date, st.summary
	st.mu.RUnlock()

	if !active || raw == nil {
		writeCypherError(
			w,
			http.StatusBadRequest,
			codeStatsMissing,
			"Statistics are not computed yet, graph summary will be available after statistics computation is complete.",
		)

		return
	}

	var gs map[string]any
	if err := json.Unmarshal(raw, &gs, jsonx.V1()); err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	if !detailed {
		delete(gs, "nodeStructures")
		delete(gs, "edgeStructures")
		delete(gs, "subjectStructures")
	}

	writeCypherJSON(w, http.StatusOK, map[string]any{
		keyStatus: statusOK200,
		keyPayload: map[string]any{
			"version":                       summaryVersion,
			"lastStatisticsComputationTime": date.UTC().Format(summaryTimeLayout),
			"graphSummary":                  gs,
		},
	})
}

func parseSummaryParams(q url.Values) (bool, *string) {
	var unknown []string

	for k := range q {
		if k != "mode" {
			unknown = append(unknown, k)
		}
	}

	if len(unknown) > 0 {
		sort.Strings(unknown)
		msg := "Request contains unknown parameters: '" + strings.Join(unknown, ", ") + "'."

		return false, &msg
	}

	switch q.Get("mode") {
	case "", summaryModeBasic:
		return false, nil
	case summaryModeDetail:
		return true, nil
	default:
		msg := "URI query parameter 'mode' has unsupported value '" + q.Get("mode") + "'."

		return false, &msg
	}
}
