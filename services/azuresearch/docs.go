package azuresearch

import (
	"net/http"
	"strconv"
)

func (h *Handler) routeDocs(req *request, index string, t []string) error {
	m := req.r.Method

	if len(t) == 0 {
		if m != http.MethodGet {
			return methodNotAllowed()
		}

		return h.searchGet(req, index)
	}

	switch t[0] {
	case "$count":
		return h.docCount(req, index)
	case "search.post.search":
		return h.searchPost(req, index)
	case "search.index":
		return h.indexBatch(req, index)
	case "search.suggest", pathPostSuggest:
		return h.suggestOp(req, index, t[0] == pathPostSuggest, false)
	case "search.autocomplete", pathPostAutocomplet:
		return h.suggestOp(req, index, t[0] == pathPostAutocomplet, true)
	}

	return h.lookup(req, index, t[0])
}

func (h *Handler) searchGet(req *request, index string) error {
	sr, err := searchFromQuery(req.q)
	if err != nil {
		return err
	}

	return h.runSearch(req, index, sr, false)
}

func (h *Handler) searchPost(req *request, index string) error {
	if req.r.Method != http.MethodPost {
		return methodNotAllowed()
	}

	sr, err := searchFromBody(req.body)
	if err != nil {
		return err
	}

	return h.runSearch(req, index, sr, true)
}

func (h *Handler) runSearch(req *request, index string, sr *searchRequest, post bool) error {
	var out *ordered

	err := h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(index)
		if gerr != nil {
			return gerr
		}

		now := h.Backend.now()

		res, gerr := s.search(ix, sr, req.version, now)
		if gerr != nil {
			return gerr
		}

		out, gerr = s.searchResponse(ix, sr, res, baseURL(req.host()), post, now)

		return gerr
	})
	if err != nil {
		return err
	}

	return h.respond(req, http.StatusOK, out)
}

func (h *Handler) docCount(req *request, index string) error {
	if req.r.Method != http.MethodGet {
		return methodNotAllowed()
	}

	var n int

	err := h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(index)
		if gerr == nil {
			n = len(ix.Docs)
		}

		return gerr
	})
	if err != nil {
		return err
	}

	req.c.Response().Header().Set("Content-Type", "text/plain; charset=utf-8")

	return req.c.String(http.StatusOK, strconv.Itoa(n))
}

func (h *Handler) lookup(req *request, index, key string) error {
	if req.r.Method != http.MethodGet {
		return methodNotAllowed()
	}

	var out *ordered

	err := h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(index)
		if gerr != nil {
			return gerr
		}

		d, ok := ix.Docs[key]
		if !ok {
			return notFoundDoc()
		}

		sel := splitCSV(req.q.Get("$select"))
		if gerr = validateSelect(ix.comp, sel); gerr != nil {
			return gerr
		}

		out = newOrdered()
		out.set(fieldContext, contextURL(baseURL(req.host()), index)+"/$entity")

		proj := project(ix.comp.fields, d.Fields, sel, "")
		for _, k := range proj.keys {
			out.set(k, proj.vals[k])
		}

		return nil
	})
	if err != nil {
		return err
	}

	return h.respond(req, http.StatusOK, out)
}

func notFoundDoc() error { return wrapf(ErrDocumentNotFound, "Document not found.") }

func (h *Handler) suggestOp(req *request, index string, post, auto bool) error {
	var (
		sr  *suggestRequest
		err error
	)

	if post {
		if req.r.Method != http.MethodPost {
			return methodNotAllowed()
		}

		sr, err = suggestFromBody(req.body, auto)
	} else {
		sr, err = suggestFromQuery(req.q, auto)
	}

	if err != nil {
		return err
	}

	var out *ordered

	err = h.Backend.read(req.svc, func(s *Service) error {
		ix, gerr := s.index(index)
		if gerr != nil {
			return gerr
		}

		if auto {
			out, gerr = s.autocomplete(ix, sr)
		} else {
			out, gerr = s.suggest(ix, sr)
		}

		return gerr
	})
	if err != nil {
		return err
	}

	ctx := "https://" + req.host() + "/indexes('" + index + "')/$metadata#"
	if auto {
		ctx += "Microsoft.Azure.Search.V2024_07_01.AutocompleteResult"
	} else {
		ctx += "docs(*)"
	}

	body := newOrdered()
	body.set(fieldContext, ctx)

	for _, k := range out.keys {
		body.set(k, out.vals[k])
	}

	return h.respond(req, http.StatusOK, body)
}

func (h *Handler) indexBatch(req *request, index string) error {
	if err := req.requireAdmin(); err != nil {
		return err
	}

	if req.r.Method != http.MethodPost {
		return methodNotAllowed()
	}

	var raw map[string]any
	if err := decodeJSON(req.body, &raw); err != nil {
		return err
	}

	list := listOf(raw, fieldValue)
	if len(list) == 0 {
		return badf(
			"The request is invalid. Details: actions : No indexing actions found in the request. "+
				"Please include between 1 and %d indexing actions in your request.",
			maxBatchActions,
		)
	}

	if len(list) > maxBatchActions {
		return wrapf(ErrTooLarge, "The request contains more than %d indexing actions.", maxBatchActions)
	}

	actions := make([]indexAction, 0, len(list))

	for _, a := range list {
		m := objOf(a)
		act := strOf(m, "@search.action")

		if act == "" {
			act = actionUpload
		}

		actions = append(actions, indexAction{action: act, fields: m})
	}

	var results []actionResult

	err := h.Backend.write(req.svc, func(s *Service) error {
		ix, gerr := s.index(index)
		if gerr != nil {
			return gerr
		}

		results = ix.applyBatch(actions, req.version)

		return nil
	})
	if err != nil {
		return err
	}

	return h.respondBatch(req, results)
}

func (h *Handler) respondBatch(req *request, results []actionResult) error {
	status := http.StatusOK
	rows := make([]map[string]any, 0, len(results))

	for _, r := range results {
		var msg any
		if r.Err != "" {
			msg = r.Err
		}

		if !r.OK {
			status = http.StatusMultiStatus
		}

		rows = append(
			rows,
			map[string]any{
				fieldKey:          nilIfEmpty(r.Key),
				fieldStatus:       r.OK,
				fieldErrorMessage: msg,
				"statusCode":      r.Code,
			},
		)
	}

	return h.respond(req, status, map[string]any{fieldValue: rows})
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}
