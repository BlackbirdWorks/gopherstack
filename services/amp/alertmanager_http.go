package amp

import (
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

func parseFilters(raw []string) ([]*promql.Matcher, error) {
	out := make([]*promql.Matcher, 0, len(raw))

	for _, f := range raw {
		m, err := parseMatcher(f)
		if err != nil {
			return nil, err
		}

		out = append(out, m)
	}

	return out, nil
}

func boolParam(vals url.Values, name string) (bool, error) {
	raw := vals.Get(name)
	if raw == "" {
		return true, nil
	}

	switch raw {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}

	return false, errBadParam
}

func alertQueryFrom(vals url.Values) (amAlertQuery, error) {
	var (
		q   amAlertQuery
		err error
	)

	if q.active, err = boolParam(vals, "active"); err != nil {
		return q, err
	}

	if q.silenced, err = boolParam(vals, "silenced"); err != nil {
		return q, err
	}

	if q.inhibit, err = boolParam(vals, "inhibited"); err != nil {
		return q, err
	}

	if q.filters, err = parseFilters(vals["filter"]); err != nil {
		return q, err
	}

	if r := vals.Get("receiver"); r != "" {
		if q.receiver, err = regexp.Compile("^(?:" + r + ")$"); err != nil {
			return q, err
		}
	}

	return q, nil
}

func (h *Handler) serveAlertManager(
	c *echo.Context, op, wsID string, params map[string]string, vals url.Values,
) error {
	ctx := c.Request().Context()

	var (
		out any
		err error
	)

	switch op {
	case opAMAlerts, opAMGroups:
		q, qerr := alertQueryFrom(vals)
		if qerr != nil {
			return promError(c, http.StatusBadRequest, "bad_data", qerr.Error())
		}

		if op == opAMAlerts {
			out, err = h.Backend.alertManagerAlerts(ctx, wsID, q)
		} else {
			out, err = h.Backend.alertManagerGroups(ctx, wsID, q)
		}
	case opAMReceivers:
		out, err = h.Backend.alertManagerReceivers(ctx, wsID)
	case opAMStatus:
		out, err = h.Backend.alertManagerStatus(ctx, wsID)
	case opAMSilences:
		filters, ferr := parseFilters(vals["filter"])
		if ferr != nil {
			return promError(c, http.StatusBadRequest, "bad_data", ferr.Error())
		}

		out, err = h.Backend.listSilences(ctx, wsID, filters)
	case opAMGetSilnce:
		out, err = h.Backend.getSilence(ctx, wsID, params["silenceId"])
	default:
		if err = h.Backend.expireSilence(ctx, wsID, params["silenceId"]); err == nil {
			return c.NoContent(http.StatusOK)
		}
	}

	if err != nil {
		return h.server.WriteErr(c, err)
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) putSilence(c *echo.Context, wsID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "cannot read request body")
	}

	var in silenceInput
	if jerr := json.Unmarshal(body, &in); jerr != nil {
		return promError(c, http.StatusBadRequest, "bad_data", "body must be a JSON silence")
	}

	id, perr := h.Backend.putSilence(c.Request().Context(), wsID, in)
	if perr != nil {
		return h.server.WriteErr(c, perr)
	}

	return c.JSON(http.StatusOK, map[string]string{"silenceID": id})
}
