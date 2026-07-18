package serverlessrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createCFTemplateRequest is the request body for CreateCloudFormationTemplate.
type createCFTemplateRequest struct {
	SemanticVersion string `json:"semanticVersion"`
}

func (h *Handler) handleCreateCloudFormationTemplate(
	ctx context.Context,
	req *http.Request,
	body []byte,
) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var createReq createCFTemplateRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	t, backendErr := h.Backend.CreateCloudFormationTemplate(appName, createReq.SemanticVersion)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created CloudFormation template",
		"app", appName, "templateId", t.TemplateID)

	resp := map[string]any{
		keyApplicationID: t.ApplicationID,
		"templateId":     t.TemplateID,
		"status":         t.Status,
		keyCreationTime:  isoTimestamp(t.CreationTime),
		"expirationTime": isoTimestamp(t.ExpirationTime),
		keyTemplateURL:   t.TemplateURL,
	}
	if t.SemanticVersion != "" {
		resp[keySemanticVersion] = t.SemanticVersion
	}

	b, marshalErr := json.Marshal(resp)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleGetCloudFormationTemplate(req *http.Request) ([]byte, error) {
	appName, templateID, err := extractPathExtra(req)
	if err != nil {
		return nil, err
	}

	if appName == "" {
		return nil, fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	if templateID == "" {
		return nil, fmt.Errorf("%w: templateId is required", errInvalidRequest)
	}

	t, backendErr := h.Backend.GetCloudFormationTemplate(appName, templateID)
	if backendErr != nil {
		return nil, backendErr
	}

	// Dynamically compute the status: real SAR returns EXPIRED once past the expiration time.
	status := t.Status
	if status == templateStatusActive && time.Now().After(t.ExpirationTime) {
		status = templateStatusExpired
	}

	resp := map[string]any{
		keyApplicationID: t.ApplicationID,
		"templateId":     t.TemplateID,
		"status":         status,
		keyCreationTime:  isoTimestamp(t.CreationTime),
		"expirationTime": isoTimestamp(t.ExpirationTime),
		keyTemplateURL:   t.TemplateURL,
	}
	if t.SemanticVersion != "" {
		resp[keySemanticVersion] = t.SemanticVersion
	}

	return json.Marshal(resp)
}

// createCFChangeSetRequest is the request body for CreateCloudFormationChangeSet.
type createCFChangeSetRequest struct {
	StackName       string   `json:"stackName"`
	ChangeSetName   string   `json:"changeSetName"`
	SemanticVersion string   `json:"semanticVersion"`
	TemplateID      string   `json:"templateId"`
	Capabilities    []string `json:"capabilities"`
	Tags            []Tag    `json:"tags"`
}

func (h *Handler) handleCreateCloudFormationChangeSet(
	ctx context.Context,
	req *http.Request,
	body []byte,
) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var createReq createCFChangeSetRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	if createReq.StackName == "" {
		return nil, fmt.Errorf("%w: stackName is required", errInvalidRequest)
	}

	cs, backendErr := h.Backend.CreateCloudFormationChangeSetWithOptions(
		appName,
		createReq.StackName,
		createReq.ChangeSetName,
		createReq.SemanticVersion,
		CreateCloudFormationChangeSetOptions{
			Capabilities: createReq.Capabilities,
			Tags:         createReq.Tags,
		},
	)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"serverlessrepo: created CloudFormation change set",
		"app",
		appName,
		"changeSetId",
		cs.ChangeSetID,
	)

	csResp := map[string]any{
		keyApplicationID: cs.ApplicationID,
		"changeSetId":    cs.ChangeSetID,
		"stackId":        cs.StackID,
	}
	if cs.SemanticVersion != "" {
		csResp[keySemanticVersion] = cs.SemanticVersion
	}

	b, marshalErr := json.Marshal(csResp)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}
