package apigateway

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	awsKindAction = "action"
	awsKindPath   = "path"
)

// AWSServiceRequest is an AWS-integration call routed to an in-process service.
// Kind is "action" (Spec is the action name) or "path" (Spec is the request path).
type AWSServiceRequest struct {
	Region  string
	Service string
	Kind    string
	Spec    string
	Method  string
	Body    []byte
}

// AWSServiceResponse is the target service's raw response.
type AWSServiceResponse struct {
	Body   []byte
	Status int
}

// AWSServiceInvoker routes an AWS-integration call through the in-process service registry.
type AWSServiceInvoker interface {
	// Supports reports whether service is served by the registry.
	Supports(service string) bool
	InvokeAWSService(ctx context.Context, req AWSServiceRequest) (AWSServiceResponse, error)
}

func (h *Handler) canInvokeAWSService(service, kind, spec string) bool {
	if h.awsInvoker == nil || service == "" || spec == "" || (kind != awsKindAction && kind != awsKindPath) {
		return false
	}

	if service == "lambda" || h.canDispatchToTarget(service, kind, spec) {
		return false
	}

	return h.awsInvoker.Supports(service)
}

func (h *Handler) handleGenericAWSServiceIntegration(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName string,
	resource *Resource,
	stageVars map[string]string,
	integration *Integration,
	region, service, kind, spec string,
) {
	payload, vtlCtx, readErr := h.buildAWSIntegrationPayload(w, r, apiID, stageName, resource, stageVars, integration)
	if readErr != nil {
		writeAWSIntegrationReadError(ctx, w, readErr)

		return
	}

	method := strings.ToUpper(integration.HTTPMethod)
	if method == "" {
		method = r.Method
	}

	resp, err := h.awsInvoker.InvokeAWSService(ctx, AWSServiceRequest{
		Region: region, Service: service, Kind: kind, Spec: spec, Method: method, Body: payload,
	})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway AWS integration: target service call failed",
			"uri", integration.URI, "service", service, "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)

		return
	}

	body, status := h.applyResponseTemplateMatching(
		resp.Body, strconv.Itoa(resp.Status), resp.Status, integration, vtlCtx.RequestID,
	)
	body = maybeCompressResponse(w, r, body, h.minCompressSize(apiID, stageName))
	w.Header().Set("Content-Type", contentTypeJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
