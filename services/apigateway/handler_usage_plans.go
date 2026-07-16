package apigateway

import (
	"encoding/json"
	"net/http"
)

type getUsagePlansPageInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type getUsagePlanInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type deleteUsagePlanInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type getUsagePlanKeyInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
}

type getUsagePlanKeysInput struct {
	UsagePlanID string `json:"usagePlanId"`
}

type deleteUsagePlanKeyInput struct {
	UsagePlanID string `json:"usagePlanId"`
	KeyID       string `json:"keyId"`
}

// parseAPIGWUsagePlansPath handles /usageplans/... paths.
func parseAPIGWUsagePlansPath(method string, segs []string, n int) (string, map[string]string, bool) {
	switch n {
	case pathDepth1:
		switch method {
		case http.MethodGet:
			return opGetUsagePlans, nil, true
		case http.MethodPost:
			return opCreateUsagePlan, nil, true
		}
	case pathDepth2:
		planParam := map[string]string{keyUsagePlanID: segs[1]}
		switch method {
		case http.MethodGet:
			return opGetUsagePlan, planParam, true
		case http.MethodDelete:
			return opDeleteUsagePlan, planParam, true
		case http.MethodPatch:
			return opUpdateUsagePlan, planParam, true
		}
	case pathDepth3:
		return parseAPIGWUsagePlansDepth3(method, segs)
	case pathDepth4:
		return parseAPIGWUsagePlansDepth4(method, segs)
	case pathDepth5:
		return parseAPIGWUsagePlansDepth5(method, segs)
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth3 handles /usageplans/{id}/{sub} paths.
func parseAPIGWUsagePlansDepth3(method string, segs []string) (string, map[string]string, bool) {
	planParam := map[string]string{keyUsagePlanID: segs[1]}

	switch segs[2] {
	case apiGWSegUsage:
		if method == http.MethodGet {
			return opGetUsage, planParam, true
		}
	case apiGWSegUsagePlanKeys:
		switch method {
		case http.MethodGet:
			return opGetUsagePlanKeys, planParam, true
		case http.MethodPost:
			return opCreateUsagePlanKey, planParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth4 handles /usageplans/{id}/keys/{keyId} paths.
func parseAPIGWUsagePlansDepth4(method string, segs []string) (string, map[string]string, bool) {
	if segs[2] != apiGWSegUsagePlanKeys {
		return apiGWUnknownOp, nil, false
	}

	params := map[string]string{keyUsagePlanID: segs[1], keyKeyID: segs[3]}

	switch method {
	case http.MethodGet:
		return opGetUsagePlanKey, params, true
	case http.MethodDelete:
		return opDeleteUsagePlanKey, params, true
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWUsagePlansDepth5 handles /usageplans/{id}/keys/{keyId}/usage paths.
func parseAPIGWUsagePlansDepth5(method string, segs []string) (string, map[string]string, bool) {
	if segs[2] != apiGWSegUsagePlanKeys || segs[4] != apiGWSegUsage {
		return apiGWUnknownOp, nil, false
	}

	if method == http.MethodPatch {
		params := map[string]string{keyUsagePlanID: segs[1], keyKeyID: segs[3]}

		return opUpdateUsage, params, true
	}

	return apiGWUnknownOp, nil, false
}

// usagePlanActions returns the action map for usage plan and usage plan key
// CRUD operations.
func (h *Handler) usagePlanActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateUsagePlan:    h.createUsagePlanAction,
		opCreateUsagePlanKey: h.createUsagePlanKeyAction,
		opGetUsagePlan:       h.getUsagePlanAction,
		opGetUsagePlans:      h.getUsagePlansAction,
		opUpdateUsagePlan:    h.updateUsagePlanAction,
		opDeleteUsagePlan:    h.deleteUsagePlanAction,
		opGetUsagePlanKey:    h.getUsagePlanKeyAction,
		opGetUsagePlanKeys:   h.getUsagePlanKeysAction,
		opDeleteUsagePlanKey: h.deleteUsagePlanKeyAction,
	}
}

func (h *Handler) createUsagePlanAction(b []byte) (int, any, error) {
	var input CreateUsagePlanInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	plan, err := h.Backend.CreateUsagePlan(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, plan, nil
}

func (h *Handler) createUsagePlanKeyAction(b []byte) (int, any, error) {
	var input CreateUsagePlanKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	upk, err := h.Backend.CreateUsagePlanKey(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, upk, nil
}

func (h *Handler) getUsagePlanAction(b []byte) (int, any, error) {
	var input getUsagePlanInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	p, err := h.Backend.GetUsagePlan(input.UsagePlanID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, p, nil
}

func (h *Handler) getUsagePlansAction(b []byte) (int, any, error) {
	var input getUsagePlansPageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		ps, err := h.Backend.GetUsagePlans()
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{keyItem: ps}, nil
	}
	ps, position, err := h.Backend.GetUsagePlansPage(input.Limit, input.Position)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: ps, keyPosition: position}, nil
}

func (h *Handler) updateUsagePlanAction(b []byte) (int, any, error) {
	var input UpdateUsagePlanInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateUsagePlan(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) deleteUsagePlanAction(b []byte) (int, any, error) {
	var input deleteUsagePlanInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteUsagePlan(input.UsagePlanID); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) getUsagePlanKeyAction(b []byte) (int, any, error) {
	var input getUsagePlanKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	k, err := h.Backend.GetUsagePlanKey(input.UsagePlanID, input.KeyID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, k, nil
}

func (h *Handler) getUsagePlanKeysAction(b []byte) (int, any, error) {
	var input getUsagePlanKeysInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	ks, err := h.Backend.GetUsagePlanKeys(input.UsagePlanID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: ks}, nil
}

func (h *Handler) deleteUsagePlanKeyAction(b []byte) (int, any, error) {
	var input deleteUsagePlanKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteUsagePlanKey(input.UsagePlanID, input.KeyID); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}
