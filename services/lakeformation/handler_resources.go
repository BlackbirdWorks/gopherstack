package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleRegisterResource(_ context.Context, c *echo.Context, body []byte) error {
	var in registerResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.ResourceArn) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "ResourceArn is required")
	}

	if in.UseServiceLinkedRole && in.RoleArn != "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException",
			"RoleArn must not be specified when UseServiceLinkedRole is true")
	}

	roleArn := in.RoleArn
	if in.UseServiceLinkedRole {
		roleArn = "arn:aws:iam::123456789012:role/aws-service-role/" +
			"lakeformation.amazonaws.com/AWSServiceRoleForLakeFormationDataAccess"
	}

	if err := h.Backend.RegisterResource(in.ResourceArn, roleArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, registerResourceOutput{})
}

func (h *Handler) handleDeregisterResource(_ context.Context, c *echo.Context, body []byte) error {
	var in deregisterResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeregisterResource(in.ResourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deregisterResourceOutput{})
}

func (h *Handler) handleDescribeResource(_ context.Context, c *echo.Context, body []byte) error {
	var in describeResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	info, err := h.Backend.DescribeResource(in.ResourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeResourceOutput{ResourceInfo: toResourceInfoWire(info)})
}

func (h *Handler) handleListResources(_ context.Context, c *echo.Context, body []byte) error {
	var in listResourcesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	resources, nextToken := h.Backend.ListResources(in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listResourcesOutput{
		ResourceInfoList: toResourceInfoWireList(resources),
		NextToken:        nextToken,
	})
}

func (h *Handler) handleUpdateResource(_ context.Context, c *echo.Context, body []byte) error {
	var in updateResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.UpdateResource(in.ResourceArn, in.RoleArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateResourceOutput{})
}
