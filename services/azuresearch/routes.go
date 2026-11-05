package azuresearch

import (
	"net/http"
	"strings"
)

func (b *InMemoryBackend) read(name string, fn func(s *Service) error) error {
	b.mu.RLock("read")
	defer b.mu.RUnlock()

	s, err := b.svc(name)
	if err != nil {
		return err
	}

	return fn(s)
}

func (b *InMemoryBackend) write(name string, fn func(s *Service) error) error {
	b.mu.Lock("write")
	defer b.mu.Unlock()

	s, err := b.svc(name)
	if err != nil {
		return err
	}

	return fn(s)
}

func etagHeader(req *request, o map[string]any) {
	if e, ok := o[etagKey].(string); ok {
		req.c.Response().Header().Set("ETag", e)
	}
}

func (req *request) host() string { return req.r.Host }

func (h *Handler) routeServiceStats(req *request) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	if req.r.Method != http.MethodGet {
		return methodNotAllowed()
	}

	var out map[string]any

	err := h.Backend.read(req.svc, func(s *Service) error {
		out = s.stats()

		return nil
	})
	if err != nil {
		return err
	}

	return h.respond(req, http.StatusOK, out)
}

func (s *Service) stats() map[string]any {
	var docs, size, vec int64

	for _, ix := range s.Indexes {
		docs += int64(len(ix.Docs))
		t, v := ix.storageSize()
		size += t
		vec += v
	}

	counter := func(n int) map[string]any { return map[string]any{"usage": n, "quota": nil} }

	return map[string]any{
		"@odata.context": "https://" + s.Name + ".search.windows.net/$metadata#Microsoft.Azure.Search.V2024_07_01.ServiceStatistics",
		"counters": map[string]any{
			"documentCount": map[string]any{"usage": docs, "quota": nil},
			"indexesCount":  map[string]any{"usage": len(s.Indexes), "quota": indexLimits[s.Config.SKU]},
			"indexersCount": counter(len(s.Objects[kindIndexers])), "dataSourcesCount": counter(len(s.Objects[kindDataSources])),
			"storageSize": map[string]any{"usage": size, "quota": nil}, "synonymMaps": counter(len(s.Objects[kindSynonymMaps])),
			"skillsetCount": counter(len(s.Objects[kindSkillsets])), "vectorIndexSize": map[string]any{"usage": vec, "quota": nil},
		},
		"limits": map[string]any{
			"maxFieldsPerIndex": maxFields, "maxFieldNestingDepthPerIndex": maxNesting,
			"maxComplexCollectionFieldsPerIndex": 40, "maxComplexObjectsInCollectionsPerDocument": 3000,
		},
	}
}

func (h *Handler) routeIndexes(req *request, t []string) error {
	switch {
	case len(t) == 0:
		return h.indexCollection(req)
	case len(t) == 1:
		return h.indexItem(req, t[0])
	}

	name, sub := t[0], t[1]

	switch sub {
	case "docs":
		return h.routeDocs(req, name, t[2:])
	case "search.stats":
		return h.indexStats(req, name)
	case "search.analyze":
		return h.indexAnalyze(req, name)
	}

	return notFound("The requested path was not found.")
}

func (h *Handler) indexCollection(req *request) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	switch req.r.Method {
	case http.MethodGet:
		var list []map[string]any

		_ = h.Backend.read(req.svc, func(s *Service) error { list = s.listIndexes(); return nil })

		return h.respond(req, http.StatusOK, map[string]any{"@odata.context": "https://" + req.host() + "/$metadata#indexes", "value": list})
	case http.MethodPost:
		def, err := decodeDefJSON(req.body)
		if err != nil {
			return err
		}

		var out map[string]any

		err = h.Backend.write(req.svc, func(s *Service) error {
			var cerr error
			out, cerr = s.createIndex(def)

			return cerr
		})
		if err != nil {
			return err
		}

		etagHeader(req, out)

		return h.respond(req, http.StatusCreated, out)
	}

	return methodNotAllowed()
}

func (h *Handler) indexItem(req *request, name string) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	switch req.r.Method {
	case http.MethodGet:
		var out map[string]any

		err := h.Backend.read(req.svc, func(s *Service) error {
			ix, gerr := s.index(name)
			if gerr == nil {
				out = cloneMap(ix.Def)
				out["@odata.context"] = "https://" + req.host() + "/$metadata#indexes/$entity"
			}

			return gerr
		})
		if err != nil {
			return err
		}

		etagHeader(req, out)

		return h.respond(req, http.StatusOK, out)
	case http.MethodPut:
		return h.putIndex(req, name)
	case http.MethodDelete:
		err := h.Backend.write(req.svc, func(s *Service) error {
			return s.deleteIndex(name, ifMatch(req))
		})
		if err != nil {
			return err
		}

		return h.respondEmpty(req, http.StatusNoContent)
	}

	return methodNotAllowed()
}

func ifMatch(req *request) string { return req.r.Header.Get("If-Match") }

func ifNone(req *request) string { return req.r.Header.Get("If-None-Match") }

func (h *Handler) putIndex(req *request, name string) error {
	def, err := decodeDefJSON(req.body)
	if err != nil {
		return err
	}

	var (
		out     map[string]any
		created bool
	)

	allow := strings.EqualFold(req.q.Get("allowIndexDowntime"), "true")

	err = h.Backend.write(req.svc, func(s *Service) error {
		var perr error
		out, created, perr = s.putIndex(name, def, allow, ifMatch(req), ifNone(req))

		return perr
	})
	if err != nil {
		return err
	}

	return h.putResponse(req, out, created)
}

func (h *Handler) putResponse(req *request, out map[string]any, created bool) error {
	etagHeader(req, out)

	switch {
	case created:
		return h.respond(req, http.StatusCreated, out)
	case strings.Contains(req.r.Header.Get("Prefer"), preferRepHdr):
		return h.respond(req, http.StatusOK, out)
	}

	return h.respondEmpty(req, http.StatusNoContent)
}

func (h *Handler) indexStats(req *request, name string) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	var out map[string]any

	err := h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(name)
		if gerr != nil {
			return gerr
		}

		total, vec := ix.storageSize()
		out = map[string]any{
			"@odata.context": "https://" + req.host() + "/$metadata#Microsoft.Azure.Search.V2024_07_01.IndexStatistics",
			"documentCount":  len(ix.Docs), "storageSize": total, "vectorIndexSize": vec,
		}

		return nil
	})
	if err != nil {
		return err
	}

	return h.respond(req, http.StatusOK, out)
}

func (h *Handler) indexAnalyze(req *request, name string) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	if req.r.Method != http.MethodPost {
		return methodNotAllowed()
	}

	body, err := decodeDefJSON(req.body)
	if err != nil {
		return err
	}

	var toks []Token

	err = h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(name)
		if gerr != nil {
			return gerr
		}

		toks, gerr = analyzeRequest(ix, body)

		return gerr
	})
	if err != nil {
		return err
	}

	out := make([]map[string]any, 0, len(toks))
	for _, t := range toks {
		out = append(out, map[string]any{"token": t.Term, "startOffset": t.Start, "endOffset": t.End, "position": t.Pos})
	}

	return h.respond(req, http.StatusOK, map[string]any{
		"@odata.context": "https://" + req.host() + "/$metadata#Microsoft.Azure.Search.V2024_07_01.AnalyzeResult", "tokens": out,
	})
}

func analyzeRequest(ix *Index, body map[string]any) ([]Token, error) {
	text := strOf(body, "text")
	if text == "" {
		return nil, badf("The request is invalid. Details: text: The text to analyze is required.")
	}

	if name := strOf(body, "analyzer"); name != "" {
		an, err := (&compiler{comp: ix.comp}).analyzer(name)
		if err != nil {
			return nil, err
		}

		return an(text), nil
	}

	tok, ok := customTokenizer(strOf(body, "tokenizer"))
	if !ok {
		return nil, badf("The request is invalid. Details: specify a supported analyzer or tokenizer.")
	}

	toks := tok(text)

	for _, f := range listOf(body, "tokenFilters") {
		name, _ := f.(string)

		fn, found := customFilter(name, stopWords["en"])
		if !found {
			return nil, badf("The request is invalid. Details: unsupported token filter '%s'.", name)
		}

		toks = fn(toks)
	}

	return toks, nil
}

func (h *Handler) routeObjects(req *request, kind string, t []string) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	if len(t) == 0 {
		return h.objectCollection(req, kind)
	}

	name := t[0]

	if len(t) > 1 {
		if kind == kindIndexers {
			return h.indexerAction(req, name, t[1])
		}

		return notFound("The requested path was not found.")
	}

	switch req.r.Method {
	case http.MethodGet:
		var out Object

		err := h.Backend.read(req.svc, func(s *Service) error {
			o, gerr := s.object(kind, name)
			if gerr == nil {
				out = redact(kind, o)
			}

			return gerr
		})
		if err != nil {
			return err
		}

		etagHeader(req, out)

		return h.respond(req, http.StatusOK, out)
	case http.MethodPut:
		def, err := decodeDefJSON(req.body)
		if err != nil {
			return err
		}

		return h.putObject(req, kind, name, def)
	case http.MethodDelete:
		err := h.Backend.write(req.svc, func(s *Service) error { return s.deleteObject(kind, name, ifMatch(req)) })
		if err != nil {
			return err
		}

		return h.respondEmpty(req, http.StatusNoContent)
	}

	return methodNotAllowed()
}

func (h *Handler) putObject(req *request, kind, name string, def Object) error {
	var (
		out     Object
		created bool
	)

	err := h.Backend.write(req.svc, func(s *Service) error {
		var perr error
		out, created, perr = s.putObject(kind, name, def, ifMatch(req), ifNone(req))

		return perr
	})
	if err != nil {
		return err
	}

	return h.putResponse(req, out, created)
}

func (h *Handler) objectCollection(req *request, kind string) error {
	switch req.r.Method {
	case http.MethodGet:
		var list []Object

		_ = h.Backend.read(req.svc, func(s *Service) error { list = s.listObjects(kind); return nil })

		if sel := splitCSV(req.q.Get("$select")); len(sel) > 0 {
			list = selectKeys(list, sel)
		}

		return h.respond(req, http.StatusOK, map[string]any{"@odata.context": "https://" + req.host() + "/$metadata#" + kind, "value": list})
	case http.MethodPost:
		def, err := decodeDefJSON(req.body)
		if err != nil {
			return err
		}

		var out Object

		err = h.Backend.write(req.svc, func(s *Service) error {
			name := strOf(def, "name")
			if _, exists := s.Objects[kind][name]; exists {
				return badf("The %s '%s' already exists.", kindLabel(kind), name)
			}

			var cerr error
			out, _, cerr = s.putObject(kind, name, def, "", "")

			return cerr
		})
		if err != nil {
			return err
		}

		etagHeader(req, out)

		return h.respond(req, http.StatusCreated, out)
	}

	return methodNotAllowed()
}

func selectKeys(list []Object, sel []string) []Object {
	out := make([]Object, 0, len(list))

	for _, o := range list {
		m := Object{}

		for _, k := range sel {
			if v, ok := o[k]; ok {
				m[k] = v
			}
		}

		out = append(out, m)
	}

	return out
}

func (h *Handler) indexerAction(req *request, name, action string) error {
	switch action {
	case "search.run", "search.reset":
		if req.r.Method != http.MethodPost {
			return methodNotAllowed()
		}

		var err error
		if action == "search.run" {
			err = h.Backend.RunIndexer(req.svc, name)
		} else {
			err = h.Backend.ResetIndexer(req.svc, name)
		}

		if err != nil {
			return objectErr(err)
		}

		status := http.StatusNoContent
		if action == "search.run" {
			status = http.StatusAccepted
		}

		return h.respondEmpty(req, status)
	case "search.status":
		var out map[string]any

		err := h.Backend.read(req.svc, func(s *Service) error {
			if _, gerr := s.object(kindIndexers, name); gerr != nil {
				return gerr
			}

			out = s.indexerStatus(name)

			return nil
		})
		if err != nil {
			return err
		}

		out["@odata.context"] = "https://" + req.host() + "/$metadata#Microsoft.Azure.Search.V2024_07_01.IndexerExecutionInfo"

		return h.respond(req, http.StatusOK, out)
	}

	return notFound("The requested path was not found.")
}

func objectErr(err error) error { return err }
