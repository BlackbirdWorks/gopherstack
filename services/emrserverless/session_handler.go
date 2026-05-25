package emrserverless

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

type startSessionBody struct {
	ConfigurationOverrides map[string]any    `json:"configurationOverrides"`
	Tags                   map[string]string `json:"tags"`
	ClientToken            string            `json:"clientToken"`
	ExecutionRoleArn       string            `json:"executionRoleArn"`
	Name                   string            `json:"name"`
	IdleTimeoutMinutes     int64             `json:"idleTimeoutMinutes"`
}

func (h *Handler) handleStartSession(c *echo.Context, applicationID string, body []byte) error {
	var in startSessionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	session, err := h.Backend.StartSession(
		applicationID, in.ClientToken, in.ExecutionRoleArn, in.Name,
		in.IdleTimeoutMinutes, in.ConfigurationOverrides, in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyApplicationID: session.ApplicationID,
		keyArn:           session.Arn,
		keySessionID:     session.SessionID,
	})
}

func sessionToMap(session *Session) map[string]any {
	out := map[string]any{
		keyApplicationID:   session.ApplicationID,
		keySessionID:       session.SessionID,
		keyArn:             session.Arn,
		keyName:            session.Name,
		keyState:           session.State,
		keyStateDetails:    session.StateDetails,
		"createdBy":        session.CreatedBy,
		"executionRoleArn": session.ExecutionRoleArn,
		keyReleaseLabel:    session.ReleaseLabel,
		keyCreatedAt:       epochSeconds(session.CreatedAt),
		keyUpdatedAt:       epochSeconds(session.UpdatedAt),
		"startedAt":        epochSeconds(session.StartedAt),
		keyTags:            session.Tags,
	}
	if !session.EndedAt.IsZero() {
		out["endedAt"] = epochSeconds(session.EndedAt)
	}
	if session.IdleTimeoutMinutes > 0 {
		out["idleTimeoutMinutes"] = session.IdleTimeoutMinutes
	}
	if session.ConfigurationOverrides != nil {
		out["configurationOverrides"] = session.ConfigurationOverrides
	}

	return out
}

func (h *Handler) handleGetSession(c *echo.Context, applicationID, sessionID string) error {
	session, err := h.Backend.GetSession(applicationID, sessionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"session": sessionToMap(session)})
}

func (h *Handler) handleListSessions(c *echo.Context, applicationID string) error {
	nextToken, maxResults, after, before, states, err := parseSessionListQuery(c)
	if err != nil {
		return h.handleError(c, err)
	}
	sessions, outToken, err := h.Backend.ListSessions(applicationID, nextToken, maxResults, after, before, states...)
	if err != nil {
		return h.handleError(c, err)
	}
	list := make([]map[string]any, 0, len(sessions))
	for _, session := range sessions {
		list = append(list, sessionToMap(session))
	}
	resp := map[string]any{"sessions": list}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func parseSessionListQuery(c *echo.Context) (string, int, time.Time, time.Time, []string, error) {
	q := c.Request().URL.Query()
	maxResults := 0
	if value := q.Get("maxResults"); value != "" {
		number, err := strconv.Atoi(value)
		if err != nil || number <= 0 {
			return "", 0, time.Time{}, time.Time{}, nil, fmt.Errorf("%w: invalid maxResults", ErrValidation)
		}
		maxResults = number
	}
	after, err := parseQueryTime(q.Get("createdAtAfter"))
	if err != nil {
		return "", 0, time.Time{}, time.Time{}, nil, err
	}
	before, err := parseQueryTime(q.Get("createdAtBefore"))
	if err != nil {
		return "", 0, time.Time{}, time.Time{}, nil, err
	}
	var states []string
	for _, value := range q["states"] {
		for state := range strings.SplitSeq(value, ",") {
			if state = strings.TrimSpace(state); state != "" {
				states = append(states, state)
			}
		}
	}

	return q.Get("nextToken"), maxResults, after, before, states, nil
}

func parseQueryTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid date-time filter", ErrValidation)
	}

	return parsed, nil
}

func (h *Handler) handleTerminateSession(c *echo.Context, applicationID, sessionID string) error {
	session, err := h.Backend.TerminateSession(applicationID, sessionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyApplicationID: session.ApplicationID,
		keySessionID:     session.SessionID,
	})
}

func (h *Handler) handleGetResourceDashboard(c *echo.Context, applicationID string) error {
	q := c.Request().URL.Query()
	dashboardURL, err := h.Backend.GetResourceDashboard(applicationID, q.Get("resourceId"), q.Get("resourceType"))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"url": dashboardURL})
}

func (h *Handler) handleGetSessionEndpoint(c *echo.Context, applicationID, sessionID string) error {
	endpoint, token, expiresAt, err := h.Backend.GetSessionEndpoint(applicationID, sessionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyApplicationID:     applicationID,
		keySessionID:         sessionID,
		"endpoint":           endpoint,
		"authToken":          token,
		"authTokenExpiresAt": epochSeconds(expiresAt),
	})
}
