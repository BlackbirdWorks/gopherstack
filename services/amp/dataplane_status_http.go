package amp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"runtime"
	"slices"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

const (
	defaultQueryTimeout = 2 * time.Minute
	buildVersion        = "2.55.1"
	maxPostedAlerts     = 10000
	defaultResolve      = 5 * time.Minute
)

// queryContext applies the request's timeout parameter, capped at the default query timeout.
func queryContext(ctx context.Context, vals url.Values) (context.Context, context.CancelFunc, error) {
	d := defaultQueryTimeout

	if raw := vals.Get("timeout"); raw != "" {
		parsed, err := parseStep(raw)
		if err != nil || parsed < 0 {
			return nil, nil, errBadParam
		}

		d = min(parsed, defaultQueryTimeout)
	}

	qctx, cancel := context.WithTimeout(ctx, d)

	return qctx, cancel, nil
}

func (h *Handler) serveStatus(c *echo.Context, op, wsID string, vals url.Values) error {
	ctx := c.Request().Context()

	var (
		data any
		err  error
	)

	switch op {
	case opRules:
		data, err = h.rulesData(ctx, wsID, vals)
	case opAlerts:
		data, err = h.alertsData(ctx, wsID)
	case opMetadata:
		data, err = h.metadataData(ctx, wsID, vals)
	case opAMAlerts:
		out, aerr := h.Backend.alertManagerAlerts(ctx, wsID)
		if aerr != nil {
			return h.server.WriteErr(c, aerr)
		}

		return c.JSON(http.StatusOK, out)
	case opBuildInfo:
		data = map[string]string{
			"version": buildVersion, "revision": "", "branch": "", "buildUser": "", "buildDate": "",
			"goVersion": runtime.Version(),
		}
	default:
		err = h.Backend.withStorage(ctx, wsID, func(promql.Storage) {})
		data = []any{}
	}

	if err != nil {
		if perr, ok := errors.AsType[promParamError](err); ok {
			return promError(c, http.StatusBadRequest, "bad_data", perr.Error())
		}

		return h.server.WriteErr(c, err)
	}

	return c.JSON(http.StatusOK, promResponse{Status: statusSuccess, Data: data})
}

type promParamError string

func (e promParamError) Error() string { return string(e) }

func (h *Handler) alertsData(ctx context.Context, wsID string) (any, error) {
	alerts, err := h.Backend.alertsStatus(ctx, wsID)

	return map[string]any{"alerts": alerts}, err
}

func (h *Handler) rulesData(ctx context.Context, wsID string, vals url.Values) (any, error) {
	typ := vals.Get("type")
	if typ != "" && typ != "alert" && typ != "record" {
		return nil, promParamError("invalid query parameter \"type\"")
	}

	groups, err := h.Backend.rulesStatus(ctx, wsID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"groups": filterGroups(groups, typ, vals)}, nil
}

func contains(vals []string, s string) bool {
	if len(vals) == 0 {
		return true
	}

	return slices.Contains(vals, s)
}

func filterGroups(groups []groupJSON, typ string, vals url.Values) []groupJSON {
	out := make([]groupJSON, 0, len(groups))

	for _, g := range groups {
		if !contains(vals["file[]"], g.File) || !contains(vals["rule_group[]"], g.Name) {
			continue
		}

		rules := make([]ruleJSON, 0, len(g.Rules))

		for _, r := range g.Rules {
			if !contains(vals["rule_name[]"], r.Name) || (typ == "alert" && r.Type != "alerting") ||
				(typ == "record" && r.Type != "recording") {
				continue
			}

			rules = append(rules, r)
		}

		g.Rules = rules
		out = append(out, g)
	}

	return out
}

func (h *Handler) metadataData(ctx context.Context, wsID string, vals url.Values) (any, error) {
	limit := 0

	if raw := vals.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return nil, promParamError("limit must be a non-negative integer")
		}

		limit = n
	}

	return h.Backend.metadataFamilies(ctx, wsID, vals.Get("metric"), limit)
}

type postableAlert struct {
	StartsAt     *time.Time        `json:"startsAt"`
	EndsAt       *time.Time        `json:"endsAt"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generatorURL"`
}

func (h *Handler) postAlerts(c *echo.Context, wsID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "cannot read request body")
	}

	var in []postableAlert
	if jerr := json.Unmarshal(body, &in); jerr != nil || len(in) > maxPostedAlerts {
		return promError(c, http.StatusBadRequest, "bad_data", "body must be a JSON array of alerts")
	}

	now := h.Backend.now()
	alerts := make([]Alert, 0, len(in))

	for _, p := range in {
		if len(p.Labels) == 0 {
			return promError(c, http.StatusBadRequest, "bad_data", "alert needs at least one label")
		}

		a := Alert{Labels: p.Labels, Annotations: p.Annotations, GeneratorURL: p.GeneratorURL, StartsAt: now}
		if p.StartsAt != nil {
			a.StartsAt = *p.StartsAt
		}

		a.EndsAt = a.StartsAt.Add(defaultResolve)
		if p.EndsAt != nil && !p.EndsAt.IsZero() {
			a.EndsAt = *p.EndsAt
		}

		alerts = append(alerts, a)
	}

	if rerr := h.Backend.ReceiveAlerts(c.Request().Context(), wsID, alerts); rerr != nil {
		return h.server.WriteErr(c, rerr)
	}

	return c.NoContent(http.StatusOK)
}
