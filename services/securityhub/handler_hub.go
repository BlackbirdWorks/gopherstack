package securityhub

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// pathHubV2Feature is the URI prefix for the per-feature opt-in endpoints
// (EnableSecurityHubFeatureV2/DisableSecurityHubFeatureV2), e.g.
// "/hubv2/feature/NETWORK_SCANNING".
const pathHubV2Feature = pathHubV2 + "/feature/"

func classifyHubPath(method, _ string) (string, string) {
	switch method {
	case http.MethodPost:
		return opEnableSecurityHub, ""
	case http.MethodDelete:
		return opDisableSecurityHub, ""
	case http.MethodGet:
		return opDescribeHub, ""
	case http.MethodPatch:
		return opUpdateSecurityHubCfg, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleEnableHub(c *echo.Context, body map[string]any) error {
	enableDefault := true

	if v, ok := body["EnableDefaultStandards"].(bool); ok {
		enableDefault = v
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.EnableHub(enableDefault, tags); err != nil {
		if errors.Is(err, ErrHubAlreadyExists) {
			return c.JSON(http.StatusConflict, map[string]any{
				keyMessage: "SecurityHub is already enabled",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableHub(c *echo.Context) error {
	if err := h.Backend.DisableHub(); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeHub(c *echo.Context) error {
	hub, err := h.Backend.DescribeHub()
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: "SecurityHub is not subscribed",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"HubArn":                  hub.HubArn,
		"SubscribedAt":            hub.SubscribedAt,
		"AutoEnableControls":      hub.AutoEnableControls,
		"AutoEnableStandards":     hub.AutoEnableStandards,
		"ControlFindingGenerator": hub.ControlFindingGenerator,
	})
}

func (h *Handler) handleUpdateHubConfig(c *echo.Context, body map[string]any) error {
	var autoEnableControls *bool

	if v, ok := body["AutoEnableControls"].(bool); ok {
		autoEnableControls = &v
	}

	var autoEnableStandards *string

	if v, ok := body["AutoEnableStandards"].(string); ok {
		autoEnableStandards = &v
	}

	var controlFindingGenerator *string

	if v, ok := body["ControlFindingGenerator"].(string); ok {
		controlFindingGenerator = &v
	}

	err := h.Backend.UpdateHubConfiguration(autoEnableControls, autoEnableStandards, controlFindingGenerator)
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func classifyHubV2Path(method, path string) (string, string) {
	if featureName, ok := strings.CutPrefix(path, pathHubV2Feature); ok {
		switch method {
		case http.MethodPost:
			return opEnableSecurityHubFeatureV2, featureName
		case http.MethodDelete:
			return opDisableSecurityHubFeatureV2, featureName
		}

		return opUnknown, ""
	}

	switch {
	case method == http.MethodPost && path == pathHubV2:
		return opEnableSecurityHubV2, ""
	case method == http.MethodDelete && path == pathHubV2:
		return opDisableSecurityHubV2, ""
	case method == http.MethodGet && path == pathHubV2:
		return opDescribeSecurityHubV2, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleEnableSecurityHubV2(c *echo.Context, body map[string]any) error {
	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.EnableSecurityHubV2(tags); err != nil {
		if errors.Is(err, ErrHubAlreadyExists) {
			return c.JSON(http.StatusConflict, map[string]any{
				keyMessage: "SecurityHub V2 is already enabled",
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableSecurityHubV2(c *echo.Context) error {
	if err := h.Backend.DisableSecurityHubV2(); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubNotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// msgHubV2NotEnabled is reused by EnableSecurityHubFeatureV2/
// DisableSecurityHubFeatureV2, since both require the same underlying V2 hub
// state as DescribeSecurityHubV2 itself.
const msgHubV2NotEnabled = "SecurityHub V2 is not enabled"

func (h *Handler) handleDescribeSecurityHubV2(c *echo.Context) error {
	hub, err := h.Backend.DescribeSecurityHubV2()
	if err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubV2NotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	features := make(map[string]any, len(hub.Features))

	for name, f := range hub.Features {
		features[name] = map[string]any{
			"FeatureStatus": f.FeatureStatus,
			keyUpdatedAt:    f.UpdatedAt,
		}
	}

	// Real DescribeSecurityHubV2Output shape is {Features, HubV2Arn,
	// SubscribedAt} -- no UpdatedAt/CreatedAt field. hub.CreatedAt is reused
	// as SubscribedAt's value (the timestamp SecurityHub V2 was enabled);
	// hub.UpdatedAt is internal bookkeeping only and intentionally not
	// echoed on the wire.
	return c.JSON(http.StatusOK, map[string]any{
		"HubV2Arn":     hub.HubV2Arn,
		"SubscribedAt": hub.CreatedAt,
		"Features":     features,
	})
}

func (h *Handler) handleEnableSecurityHubFeatureV2(c *echo.Context, featureName string) error {
	if err := h.Backend.EnableSecurityHubFeatureV2(featureName); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubV2NotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDisableSecurityHubFeatureV2(c *echo.Context, featureName string) error {
	if err := h.Backend.DisableSecurityHubFeatureV2(featureName); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgHubV2NotEnabled})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// hubOpHandlers returns the Hub (V1 + V2) operation dispatch table for
// handleREST.
func (h *Handler) hubOpHandlers(c *echo.Context, resource string, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opEnableSecurityHub:    func() error { return h.handleEnableHub(c, body) },
		opDisableSecurityHub:   func() error { return h.handleDisableHub(c) },
		opDescribeHub:          func() error { return h.handleDescribeHub(c) },
		opUpdateSecurityHubCfg: func() error { return h.handleUpdateHubConfig(c, body) },
		opEnableSecurityHubV2:  func() error { return h.handleEnableSecurityHubV2(c, body) },
		opDisableSecurityHubV2: func() error { return h.handleDisableSecurityHubV2(c) },
		opDescribeSecurityHubV2: func() error {
			return h.handleDescribeSecurityHubV2(c)
		},
		opEnableSecurityHubFeatureV2:  func() error { return h.handleEnableSecurityHubFeatureV2(c, resource) },
		opDisableSecurityHubFeatureV2: func() error { return h.handleDisableSecurityHubFeatureV2(c, resource) },
	}
}
