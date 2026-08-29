package apigateway

import (
	"encoding/json"
	"maps"
	"net/http"
)

type createDeploymentInput struct {
	Variables        map[string]string `json:"variables,omitempty"`
	RestAPIID        string            `json:"restApiId"`
	StageName        string            `json:"stageName"`
	Description      string            `json:"description"`
	StageDescription string            `json:"stageDescription,omitempty"`
	TracingEnabled   bool              `json:"tracingEnabled,omitempty"`
}

type getDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type getDeploymentsInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

type deleteDeploymentInput struct {
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

type updateDeploymentHandlerInput struct {
	UpdateDeploymentInput
	RestAPIID    string `json:"restApiId"`
	DeploymentID string `json:"deploymentId"`
}

// deploymentActions returns the action map for deployment and stage operations.
func (h *Handler) deploymentActions() map[string]actionFn {
	m := make(map[string]actionFn)
	maps.Copy(m, h.deploymentCRUDActions())
	maps.Copy(m, h.stageActions())

	return m
}

// createDeploymentAction handles CreateDeployment incl. inline stage update.
func (h *Handler) createDeploymentAction(b []byte) (int, any, error) {
	var input createDeploymentInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	depl, err := h.Backend.CreateDeployment(input.RestAPIID, input.StageName, input.Description)
	if err != nil {
		return 0, nil, err
	}
	h.applyInlineStageUpdate(input)

	return http.StatusCreated, depl, nil
}

func (h *Handler) applyInlineStageUpdate(input createDeploymentInput) {
	if input.StageName == "" {
		return
	}
	if input.StageDescription == "" && len(input.Variables) == 0 && !input.TracingEnabled {
		return
	}
	stageUpd := UpdateStageInput{
		Description: input.StageDescription,
		Variables:   input.Variables,
	}
	if input.TracingEnabled {
		t := true
		stageUpd.TracingEnabled = &t
	}
	_, _ = h.Backend.UpdateStage(input.RestAPIID, input.StageName, stageUpd)
}

// deploymentCRUDActions returns actions for deployment CRUD operations.
func (h *Handler) deploymentCRUDActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateDeployment: h.createDeploymentAction,
		opGetDeployment:    h.getDeploymentAction,
		opGetDeployments:   h.getDeploymentsAction,
		opDeleteDeployment: h.deleteDeploymentAction,
		opUpdateDeployment: h.updateDeploymentAction,
	}
}

func (h *Handler) getDeploymentAction(b []byte) (int, any, error) {
	var input getDeploymentInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	depl, err := h.Backend.GetDeployment(input.RestAPIID, input.DeploymentID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, depl, nil
}

func (h *Handler) getDeploymentsAction(b []byte) (int, any, error) {
	var input getDeploymentsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	depls, err := h.Backend.GetDeployments(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: depls}, nil
	}
	page, position := paginatePageByKey(depls, input.Limit, input.Position,
		func(d Deployment) string { return d.ID })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) deleteDeploymentAction(b []byte) (int, any, error) {
	var input deleteDeploymentInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteDeployment(input.RestAPIID, input.DeploymentID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}

func (h *Handler) updateDeploymentAction(b []byte) (int, any, error) {
	var input updateDeploymentHandlerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	depl, err := h.Backend.UpdateDeployment(input.RestAPIID, input.DeploymentID, input.UpdateDeploymentInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, depl, nil
}
