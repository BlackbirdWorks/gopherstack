package apigateway

import (
	"encoding/json"
	"net/http"
)

type createRequestValidatorInput struct {
	RestAPIID                 string `json:"restApiId"`
	Name                      string `json:"name"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

type getRequestValidatorInput struct {
	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

type getRequestValidatorsInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

type updateRequestValidatorInput struct {
	UpdateRequestValidatorInput

	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

type deleteRequestValidatorInput struct {
	RestAPIID   string `json:"restApiId"`
	ValidatorID string `json:"requestValidatorId"`
}

func (h *Handler) requestValidatorActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateRequestValidator: h.createRequestValidatorAction,
		opGetRequestValidator:    h.getRequestValidatorAction,
		opGetRequestValidators:   h.getRequestValidatorsAction,
		opUpdateRequestValidator: h.updateRequestValidatorAction,
		opDeleteRequestValidator: h.deleteRequestValidatorAction,
	}
}

func (h *Handler) createRequestValidatorAction(b []byte) (int, any, error) {
	var input createRequestValidatorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	rv, err := h.Backend.CreateRequestValidator(input.RestAPIID, CreateRequestValidatorInput{
		Name:                      input.Name,
		ValidateRequestBody:       input.ValidateRequestBody,
		ValidateRequestParameters: input.ValidateRequestParameters,
	})
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, rv, nil
}

func (h *Handler) getRequestValidatorAction(b []byte) (int, any, error) {
	var input getRequestValidatorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	rv, err := h.Backend.GetRequestValidator(input.RestAPIID, input.ValidatorID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, rv, nil
}

func (h *Handler) getRequestValidatorsAction(b []byte) (int, any, error) {
	var input getRequestValidatorsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	rvs, err := h.Backend.GetRequestValidators(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: rvs}, nil
	}
	page, position := paginatePageByKey(rvs, input.Limit, input.Position,
		func(rv RequestValidator) string { return rv.ID })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) updateRequestValidatorAction(b []byte) (int, any, error) {
	var input updateRequestValidatorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	rv, err := h.Backend.UpdateRequestValidator(
		input.RestAPIID,
		input.ValidatorID,
		input.UpdateRequestValidatorInput,
	)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, rv, nil
}

func (h *Handler) deleteRequestValidatorAction(b []byte) (int, any, error) {
	var input deleteRequestValidatorInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteRequestValidator(input.RestAPIID, input.ValidatorID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}
