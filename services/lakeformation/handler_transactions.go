package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCancelTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in cancelTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	if err := h.Backend.CancelTransaction(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, cancelTransactionOutput{})
}

func (h *Handler) handleCommitTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in commitTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	status, err := h.Backend.CommitTransaction(in.TransactionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, commitTransactionOutput{TransactionStatus: status})
}

func (h *Handler) handleStartTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in startTransactionInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	id := h.Backend.StartTransaction(in.TransactionType)

	return c.JSON(http.StatusOK, startTransactionOutput{TransactionID: id})
}

func (h *Handler) handleDescribeTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in describeTransactionInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if strings.TrimSpace(in.TransactionID) == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "TransactionId is required")
	}

	tx, err := h.Backend.DescribeTransaction(in.TransactionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, describeTransactionOutput{TransactionDescription: toTransactionWire(tx)})
}

func (h *Handler) handleListTransactions(_ context.Context, c *echo.Context, body []byte) error {
	var in listTransactionsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	txns, nextToken := h.Backend.ListTransactions(in.StatusFilter, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listTransactionsOutput{
		Transactions: toTransactionWireList(txns),
		NextToken:    nextToken,
	})
}

func (h *Handler) handleDeleteObjectsOnCancel(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteObjectsOnCancelInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}
	if err := h.Backend.DeleteObjectsOnCancel(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteObjectsOnCancelOutput{})
}

func (h *Handler) handleExtendTransaction(_ context.Context, c *echo.Context, body []byte) error {
	var in extendTransactionInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	if err := h.Backend.ExtendTransaction(in.TransactionID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, extendTransactionOutput{})
}
