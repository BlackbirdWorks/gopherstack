package chaos

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

const (
	maxFaultStatus = 599
	minFaultStatus = 100
)

var (
	errInvalidFault   = errors.New("invalid fault rule")
	errInvalidEffects = errors.New("invalid effects document")
)

// localstackEffects is the LocalStack effects document; latency is its only effect.
type localstackEffects struct {
	Latency int `json:"latency"`
}

// RegisterLocalStackRoutes mounts the LocalStack Chaos API under /_localstack/chaos.
func RegisterLocalStackRoutes(e *echo.Echo, store *FaultStore) {
	h := &lsHandler{store: store}

	e.GET("/_localstack/chaos/faults", h.getFaults)
	e.POST("/_localstack/chaos/faults", h.postFaults)
	e.PATCH("/_localstack/chaos/faults", h.patchFaults)
	e.DELETE("/_localstack/chaos/faults", h.deleteFaults)
	e.GET("/_localstack/chaos/effects", h.getEffects)
	e.POST("/_localstack/chaos/effects", h.postEffects)
}

type lsHandler struct{ store *FaultStore }

func (h *lsHandler) rules() []FaultRule { return h.store.GetRules() }

func decodeRules(c *echo.Context) ([]FaultRule, error) {
	var rules []FaultRule
	if err := json.NewDecoder(c.Request().Body).Decode(&rules); err != nil {
		return nil, err
	}

	if rules == nil {
		rules = []FaultRule{}
	}

	for _, r := range rules {
		if r.Probability != nil && (*r.Probability < 0 || *r.Probability > 1) {
			return nil, errInvalidFault
		}

		if r.Error != nil && r.Error.StatusCode != 0 &&
			(r.Error.StatusCode < minFaultStatus || r.Error.StatusCode > maxFaultStatus) {
			return nil, errInvalidFault
		}
	}

	return rules, nil
}

func badRequest(c *echo.Context, err error) error {
	return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func (h *lsHandler) getFaults(c *echo.Context) error {
	return c.JSON(http.StatusOK, h.rules())
}

func (h *lsHandler) postFaults(c *echo.Context) error {
	rules, err := decodeRules(c)
	if err != nil {
		return badRequest(c, err)
	}

	h.store.SetRules(rules)

	return c.JSON(http.StatusOK, h.rules())
}

func (h *lsHandler) patchFaults(c *echo.Context) error {
	rules, err := decodeRules(c)
	if err != nil {
		return badRequest(c, err)
	}

	h.store.AppendRules(rules)

	return c.JSON(http.StatusOK, h.rules())
}

// deleteFaults removes rules equal in every field to a listed rule, as LocalStack does.
func (h *lsHandler) deleteFaults(c *echo.Context) error {
	rules, err := decodeRules(c)
	if err != nil {
		return badRequest(c, err)
	}

	h.store.DeleteExactRules(rules)

	return c.JSON(http.StatusOK, h.rules())
}

func (h *lsHandler) getEffects(c *echo.Context) error {
	return c.JSON(http.StatusOK, localstackEffects{Latency: h.store.GetEffects().Latency})
}

func (h *lsHandler) postEffects(c *echo.Context) error {
	var in localstackEffects
	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil || in.Latency < 0 {
		return badRequest(c, errInvalidEffects)
	}

	h.store.SetEffects(NetworkEffects{Latency: in.Latency})

	return c.JSON(http.StatusOK, in)
}
