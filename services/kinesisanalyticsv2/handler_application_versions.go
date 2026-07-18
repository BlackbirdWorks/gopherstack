package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type describeApplicationOperationInput struct {
	ApplicationName string `json:"ApplicationName"`
	OperationID     string `json:"OperationId"`
}

// applicationOperationInfoDetails mirrors real AWS's
// ApplicationOperationInfoDetails shape (DescribeApplicationOperation): it
// does NOT carry OperationId (the caller already supplied it in the
// request) but does require StartTime/EndTime, both awsjson1.1 epoch-seconds
// numbers (see pkgs/awstime).
type applicationOperationInfoDetails struct {
	Operation       string  `json:"Operation"`
	OperationStatus string  `json:"OperationStatus"`
	StartTime       float64 `json:"StartTime"`
	EndTime         float64 `json:"EndTime"`
}

type describeApplicationOperationOutput struct {
	ApplicationOperationInfoDetails applicationOperationInfoDetails `json:"ApplicationOperationInfoDetails"`
}

type listApplicationOperationsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

// applicationOperationInfo mirrors real AWS's ApplicationOperationInfo shape
// (ListApplicationOperations): unlike applicationOperationInfoDetails it does
// carry OperationId, since list items need it to identify which operation to
// describe next.
type applicationOperationInfo struct {
	OperationID     string  `json:"OperationId,omitempty"`
	Operation       string  `json:"Operation,omitempty"`
	OperationStatus string  `json:"OperationStatus,omitempty"`
	StartTime       float64 `json:"StartTime,omitempty"`
	EndTime         float64 `json:"EndTime,omitempty"`
}

type listApplicationOperationsOutput struct {
	NextToken                    string                     `json:"NextToken,omitempty"`
	ApplicationOperationInfoList []applicationOperationInfo `json:"ApplicationOperationInfoList"`
}

type describeApplicationVersionInput struct {
	ApplicationName      string `json:"ApplicationName"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type describeApplicationVersionOutput struct {
	ApplicationVersionDetail applicationDetailOutput `json:"ApplicationVersionDetail"`
}

type listApplicationVersionsInput struct {
	ApplicationName string `json:"ApplicationName"`
	NextToken       string `json:"NextToken,omitempty"`
}

type applicationVersionSummaryOutput struct {
	ApplicationStatus    string `json:"ApplicationStatus"`
	ApplicationVersionID int64  `json:"ApplicationVersionId"`
}

type listApplicationVersionsOutput struct {
	NextToken                   string                            `json:"NextToken,omitempty"`
	ApplicationVersionSummaries []applicationVersionSummaryOutput `json:"ApplicationVersionSummaries"`
}

type rollbackApplicationInput struct {
	ApplicationName             string `json:"ApplicationName"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type rollbackApplicationOutput struct {
	OperationID       string                  `json:"OperationId,omitempty"`
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

func (h *Handler) handleDescribeApplicationOperation(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeApplicationOperationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	op, err := h.Backend.DescribeApplicationOperation(ctx, in.ApplicationName, in.OperationID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationOperationOutput{
		ApplicationOperationInfoDetails: applicationOperationInfoDetails{
			Operation:       op.Operation,
			OperationStatus: op.OperationStatus,
			StartTime:       awstime.Epoch(op.StartTimestamp),
			EndTime:         awstime.Epoch(op.EndTimestamp),
		},
	})
}

func (h *Handler) handleListApplicationOperations(ctx context.Context, c *echo.Context, body []byte) error {
	var in listApplicationOperationsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	ops, outToken, err := h.Backend.ListApplicationOperations(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]applicationOperationInfo, 0, len(ops))
	for _, op := range ops {
		items = append(items, applicationOperationInfo{
			OperationID:     op.OperationID,
			Operation:       op.Operation,
			OperationStatus: op.OperationStatus,
			StartTime:       awstime.Epoch(op.StartTimestamp),
			EndTime:         awstime.Epoch(op.EndTimestamp),
		})
	}

	return c.JSON(http.StatusOK, listApplicationOperationsOutput{
		ApplicationOperationInfoList: items,
		NextToken:                    outToken,
	})
}

func (h *Handler) handleDescribeApplicationVersion(ctx context.Context, c *echo.Context, body []byte) error {
	var in describeApplicationVersionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.DescribeApplicationVersion(ctx, in.ApplicationName, in.ApplicationVersionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationVersionOutput{
		ApplicationVersionDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleListApplicationVersions(ctx context.Context, c *echo.Context, body []byte) error {
	var in listApplicationVersionsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	vers, outToken, err := h.Backend.ListApplicationVersions(ctx, in.ApplicationName, in.NextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]applicationVersionSummaryOutput, 0, len(vers))
	for _, v := range vers {
		summaries = append(summaries, applicationVersionSummaryOutput{
			ApplicationVersionID: v.ApplicationVersionID,
			ApplicationStatus:    v.ApplicationStatus,
		})
	}

	return c.JSON(http.StatusOK, listApplicationVersionsOutput{
		ApplicationVersionSummaries: summaries,
		NextToken:                   outToken,
	})
}

func (h *Handler) handleRollbackApplication(ctx context.Context, c *echo.Context, body []byte) error {
	var in rollbackApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, opID, err := h.Backend.RollbackApplication(ctx, in.ApplicationName, in.CurrentApplicationVersionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, rollbackApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
		OperationID:       opID,
	})
}
