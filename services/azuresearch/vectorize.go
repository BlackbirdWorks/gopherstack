package azuresearch

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	webAPITimeout   = 30 * time.Second
	webAPIBodyLimit = 8 << 20
)

type vectorizeJob struct {
	def map[string]any
	idx int
}

// vectorizeQueries replaces text vector queries with raw vectors produced by the profile's vectorizer.
func (h *Handler) vectorizeQueries(req *request, index string, sr *searchRequest) error {
	var jobs []vectorizeJob

	err := h.Backend.read(req.svc, func(s *Service) error {
		ix, err := s.index(index)
		if err != nil {
			return err
		}

		for i, vq := range sr.Vectors {
			if vq.Kind != vecQueryText {
				continue
			}

			def, verr := ix.comp.vectorizerFor(vq)
			if verr != nil {
				return verr
			}

			jobs = append(jobs, vectorizeJob{idx: i, def: def})
		}

		return nil
	})
	if err != nil || len(jobs) == 0 {
		return err
	}

	for _, j := range jobs {
		vq := &sr.Vectors[j.idx]

		vec, verr := h.Backend.callVectorizer(req.r.Context(), j.def, vq.Text)
		if verr != nil {
			return verr
		}

		vq.Vector, vq.Kind = vec, vecQueryVector
	}

	return nil
}

func (c *compiled) vectorizerFor(vq vectorQuery) (map[string]any, error) {
	var name string

	for _, f := range splitCSV(vq.Fields) {
		fd, ok := c.byPath[f]
		if !ok || !fd.isVector() {
			return nil, badf("The field '%s' in the vector query is not a vector field.", f)
		}

		n := c.vecProfile[fd.Profile].Vectorizer
		if n == "" {
			return nil, badf("The field '%s' has no vectorizer configured in its vector search profile.", f)
		}

		if name != "" && n != name {
			return nil, badf("A text vector query requires all fields to use the same vectorizer.")
		}

		name = n
	}

	def := c.vectorizers[name]
	if k := strOf(def, "kind"); k != vecKindCustom {
		return nil, badf(
			"The vectorizer '%s' of kind '%s' needs a hosted embedding model that this service cannot reach.",
			name,
			k,
		)
	}

	return def, nil
}

// callVectorizer follows learn.microsoft.com/azure/search/vector-search-vectorizer-custom-web-api.
func (b *InMemoryBackend) callVectorizer(ctx context.Context, def map[string]any, text string) ([]float64, error) {
	p := objOf(def["customWebApiParameters"])

	body, err := json.Marshal(map[string]any{
		"values": []any{map[string]any{"recordId": "0", "data": map[string]any{"text": text}}},
	}, jsonx.V1())
	if err != nil {
		return nil, err
	}

	method := strOf(p, "httpMethod")
	if method == "" {
		method = http.MethodPost
	}

	hr, err := http.NewRequestWithContext(ctx, method, strOf(p, "uri"), bytes.NewReader(body))
	if err != nil {
		return nil, badf("The vectorizer uri is invalid.")
	}

	hr.Header.Set("Content-Type", "application/json")

	for k, v := range objOf(p["httpHeaders"]) {
		if s, ok := v.(string); ok {
			hr.Header.Set(k, s)
		}
	}

	resp, err := b.http.Do(hr)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "azuresearch: vectorizer call failed", "error", err)

		return nil, badf("The vectorizer endpoint could not be reached.")
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, webAPIBodyLimit))
	if err != nil || resp.StatusCode/100 != 2 {
		return nil, badf("The vectorizer endpoint returned status %d.", resp.StatusCode)
	}

	return parseVectorizerReply(raw)
}

func parseVectorizerReply(raw []byte) ([]float64, error) {
	out, err := decodeDefJSON(raw)
	if err != nil {
		return nil, badf("The vectorizer reply is not a JSON object.")
	}

	vals := listOf(out, "values")
	if len(vals) != 1 {
		return nil, badf("The vectorizer reply must contain one record.")
	}

	rec := objOf(vals[0])
	if errs := listOf(rec, "errors"); len(errs) > 0 {
		return nil, badf("The vectorizer reported an error: %s", fmt.Sprint(objOf(errs[0])["message"]))
	}

	var vec []float64

	for _, x := range listOf(objOf(rec["data"]), "vector") {
		f, ok := x.(float64)
		if !ok {
			return nil, badf("The vectorizer reply vector must contain only numbers.")
		}

		vec = append(vec, f)
	}

	if len(vec) == 0 || slices.ContainsFunc(vec, math.IsNaN) {
		return nil, badf("The vectorizer reply has no vector.")
	}

	return vec, nil
}
