package rolesanywhere

import (
	"context"
	"net/http"
	"time"
)

// ---- Subject handlers ----

func (h *Handler) handleGetSubject(ctx context.Context, path string) (any, int, error) {
	id := extractID(path, pathSubject)

	s, err := h.Backend.GetSubject(ctx, id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keySubject: subjectToJSON(s)}, http.StatusOK, nil
}

func (h *Handler) handleListSubjects(ctx context.Context, query string) (any, int, error) {
	pageToken, maxResults, ppErr := parsePageParams(query)
	if ppErr != nil {
		return nil, 0, ppErr
	}

	all, next, err := h.Backend.ListSubjects(ctx, pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, s := range all {
		list = append(list, subjectToJSON(s))
	}

	resp := map[string]any{keySubjects: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func subjectToJSON(s *Subject) map[string]any {
	m := map[string]any{
		"subjectId":   s.SubjectID,
		"subjectArn":  s.SubjectArn,
		"x509Subject": s.X509Subject,
		"enabled":     s.Enabled,                        //nolint:goconst // existing issue.
		"createdAt":   s.CreatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
		"updatedAt":   s.UpdatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
	}

	if !s.LastSeenAt.IsZero() {
		m["lastSeenAt"] = s.LastSeenAt.Format(time.RFC3339)
	}

	return m
}
