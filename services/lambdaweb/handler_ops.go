package lambdaweb

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (h *Handler) dispatchFunctionOps(
	ctx context.Context, c *echo.Context, region string, t target, body []byte,
) (bool, error) {
	switch t.op {
	case opCreateWebFunction:
		return true, h.handleCreateFunction(ctx, c, region, body)
	case opGetWebFunction:
		f, err := h.Backend.GetWebFunction(region, t.function)
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusOK, functionSummary(f))
	case opListWebFunctions:
		return true, h.handleListFunctions(c, region, body)
	case opDeleteWebFunction:
		return true, h.noContent(c, h.Backend.DeleteWebFunction(region, t.function))
	}

	return false, nil
}

func (h *Handler) noContent(c *echo.Context, err error) error {
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateFunction(ctx context.Context, c *echo.Context, region string, body []byte) error {
	var req createFunctionRequest
	if err := decode(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	in := CreateFunctionInput{Name: req.FunctionName, Tags: req.Tags}

	if req.RevisionConfig != nil {
		rev := revisionInputFromDTO(req.RevisionConfig)
		in.Revision = &rev
	}

	if req.EndpointConfig != nil {
		ep := endpointInputFromDTO(req.EndpointConfig)
		in.Endpoint = &ep
	}

	out, err := h.Backend.CreateWebFunction(ctx, region, in)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	f := out.Function
	resp := createFunctionResponse{
		CreatedAt: fmtTime(f.CreatedAt), FunctionArn: f.ARN, FunctionName: f.Name, State: f.State,
		StateReason: f.StateReason, UpdatedAt: fmtTime(f.UpdatedAt), Tags: f.Tags,
	}

	if out.Revision != nil {
		s := revisionSummary(out.Revision)
		resp.Revision = &s
	}

	if out.Endpoint != nil {
		s := endpointSummary(out.Endpoint)
		resp.Endpoint = &s
	}

	return c.JSON(http.StatusAccepted, resp)
}

func listOptions(req listRequest) ListOptions {
	return ListOptions{NextToken: req.NextToken, MaxResults: req.MaxResults, Filters: filtersFromDTO(req.Filters)}
}

func (h *Handler) handleListFunctions(c *echo.Context, region string, body []byte) error {
	var req listRequest
	if err := decode(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	pg, err := h.Backend.ListWebFunctions(region, listOptions(req))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listFunctionsResponse{Functions: mapPage(pg, functionSummary), NextToken: pg.Next})
}

func mapPage[T, D any](pg page.Page[*T], f func(*T) D) []D {
	out := make([]D, 0, len(pg.Data))
	for _, v := range pg.Data {
		out = append(out, f(v))
	}

	return out
}

func (h *Handler) dispatchRevisionOps(
	ctx context.Context, c *echo.Context, region string, t target, body []byte,
) (bool, error) {
	switch t.op {
	case opCreateWebFunctionRevision:
		var req createRevisionRequest
		if err := decode(body, &req); err != nil {
			return true, h.writeInvalidBody(c)
		}

		in := revisionInputFromDTO(&revisionConfigDTO{
			BuildConfig: req.BuildConfig, ServiceConfig: req.ServiceConfig, Description: req.Description,
			KmsKeyArn: req.KmsKeyArn,
		})

		rev, err := h.Backend.CreateWebFunctionRevision(ctx, region, t.function, in)
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusAccepted, revisionResponseOf(rev))
	case opGetWebFunctionRevision:
		rev, err := h.Backend.GetWebFunctionRevision(region, t.function, t.child)
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusOK, revisionResponseOf(rev))
	case opListWebFunctionRevisions:
		var req listRequest
		if err := decode(body, &req); err != nil {
			return true, h.writeInvalidBody(c)
		}

		pg, err := h.Backend.ListWebFunctionRevisions(region, t.function, listOptions(req))
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusOK, listRevisionsResponse{
			Revisions: mapPage(pg, revisionSummary), NextToken: pg.Next,
		})
	case opDeleteWebFunctionRevision:
		return true, h.noContent(c, h.Backend.DeleteWebFunctionRevision(region, t.function, t.child))
	}

	return false, nil
}

func (h *Handler) dispatchEndpointOps(c *echo.Context, region string, t target, body []byte) (bool, error) {
	switch t.op {
	case opCreateWebFunctionEndpoint:
		return true, h.handleCreateEndpoint(c, region, t.function, body)
	case opGetWebFunctionEndpoint:
		e, err := h.Backend.GetWebFunctionEndpoint(region, t.function, t.child)
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusOK, endpointResponseOf(e))
	case opListWebFunctionEndpoints:
		var req listRequest
		if err := decode(body, &req); err != nil {
			return true, h.writeInvalidBody(c)
		}

		pg, err := h.Backend.ListWebFunctionEndpoints(region, t.function, listOptions(req))
		if err != nil {
			return true, h.writeBackendError(c, err)
		}

		return true, c.JSON(http.StatusOK, listEndpointsResponse{
			Endpoints: mapPage(pg, endpointSummary),
			NextToken: pg.Next,
		})
	case opUpdateWebFunctionEndpoint:
		return true, h.handleUpdateEndpoint(c, region, t, body)
	case opDeleteWebFunctionEndpoint:
		return true, h.noContent(c, h.Backend.DeleteWebFunctionEndpoint(region, t.function, t.child))
	}

	return false, nil
}

func (h *Handler) handleCreateEndpoint(c *echo.Context, region, fn string, body []byte) error {
	var req createEndpointRequest
	if err := decode(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	in := endpointInputFromDTO(&endpointConfigDTO{
		ScalingConfig: req.ScalingConfig, ThrottleConfig: req.ThrottleConfig, EndpointName: req.EndpointName,
		Description: req.Description, EndpointType: req.EndpointType, AuthType: req.AuthType,
		AutoDeploymentMode: req.AutoDeploymentMode, Regions: req.Regions,
	})
	in.Weights = weightsFromDTO(req.RevisionWeights)

	e, err := h.Backend.CreateWebFunctionEndpoint(region, fn, in)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusAccepted, endpointResponseOf(e))
}

func (h *Handler) handleUpdateEndpoint(c *echo.Context, region string, t target, body []byte) error {
	var req updateEndpointRequest
	if err := decode(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	up := EndpointUpdate{
		Description: req.Description,
		Scaling:     scalingFromDTO(req.ScalingConfig),
		Throttle: throttleFromDTO(
			req.ThrottleConfig,
		),
		AuthType:           req.AuthType,
		AutoDeploymentMode: req.AutoDeploymentMode,
	}

	if req.RevisionWeights != nil {
		up.WeightsSet, up.Weights = true, weightsFromDTO(*req.RevisionWeights)
	}

	e, err := h.Backend.UpdateWebFunctionEndpoint(region, t.function, t.child, up)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusAccepted, endpointResponseOf(e))
}

func (h *Handler) dispatchMiscOps(c *echo.Context, region string, t target, body []byte) (bool, error) {
	switch t.op {
	case opGetWebAccountSettings:
		s := h.Backend.AccountSettings(region)

		return true, c.JSON(http.StatusOK, accountSettingsResponse{
			AccountQuotas: accountQuotasDTO{
				MaxEndpointsPerFunction: s.MaxEndpointsPerFunction, MaxRevisionsPerFunction: s.MaxRevisionsPerFunction,
				MaxTotalArmVCpus: s.MaxTotalArmVCpus, MaxTotalRateLimit: s.MaxTotalRateLimit,
			},
			AccountUsage: accountUsageDTO{FunctionCount: s.FunctionCount},
		})
	case opPutResourcePolicy, opGetResourcePolicy, opDeleteResourcePolicy:
		return true, h.handlePolicyOp(c, t, body)
	case opTagResource, opUntagResource, opListTags:
		return true, h.handleTagOp(c, t, body)
	}

	return false, nil
}

func (h *Handler) handlePolicyOp(c *echo.Context, t target, body []byte) error {
	switch t.op {
	case opPutResourcePolicy:
		var req putPolicyRequest
		if err := decode(body, &req); err != nil {
			return h.writeInvalidBody(c)
		}

		p, err := h.Backend.PutResourcePolicy(t.resource, req.Policy, req.RevisionID)
		if err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusOK, policyResponse{Policy: p.Policy, RevisionID: p.RevisionID})
	case opGetResourcePolicy:
		p, err := h.Backend.GetResourcePolicy(t.resource)
		if err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusOK, policyResponse{Policy: p.Policy, RevisionID: p.RevisionID})
	default:
		return h.noContent(c, h.Backend.DeleteResourcePolicy(t.resource, c.Request().URL.Query().Get("RevisionId")))
	}
}

func (h *Handler) handleTagOp(c *echo.Context, t target, body []byte) error {
	switch t.op {
	case opTagResource:
		var req tagRequest
		if err := decode(body, &req); err != nil {
			return h.writeInvalidBody(c)
		}

		return h.noContent(c, h.Backend.TagResource(t.resource, req.Tags))
	case opUntagResource:
		return h.noContent(c, h.Backend.UntagResource(t.resource, c.Request().URL.Query()["tagKeys"]))
	default:
		tags, err := h.Backend.ListTags(t.resource)
		if err != nil {
			return h.writeBackendError(c, err)
		}

		return c.JSON(http.StatusOK, tagsResponse{Tags: tags})
	}
}
