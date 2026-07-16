package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborPutDashboard(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "DashboardName")
	if name == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"DashboardName is required",
		)
	}

	body := cborStr(input, "DashboardBody")

	if err := h.Backend.PutDashboard(name, body); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{"DashboardValidationMessages": cbor.List{}})
}

func (h *Handler) cborGetDashboard(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "DashboardName")
	if name == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"DashboardName is required",
		)
	}

	entry, body, err := h.Backend.GetDashboard(name)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{
		"DashboardArn":  cbor.String(entry.DashboardArn),
		"DashboardBody": cbor.String(body),
		"DashboardName": cbor.String(entry.DashboardName),
	})
}

func (h *Handler) cborListDashboards(input cbor.Map, c *echo.Context) error {
	prefix := cborStr(input, "DashboardNamePrefix")
	nextToken := cborStr(input, "NextToken")

	p, err := h.Backend.ListDashboards(prefix, nextToken)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	entries := make(cbor.List, 0, len(p.Data))
	for _, e := range p.Data {
		entries = append(entries, cbor.Map{
			"DashboardArn":  cbor.String(e.DashboardArn),
			"DashboardName": cbor.String(e.DashboardName),
			"LastModified":  cborFromTime(e.LastModified),
			"Size": cbor.Uint(
				uint64(e.Size), //nolint:gosec // Size is always non-negative (len of body)
			),
		})
	}

	resp := cbor.Map{"DashboardEntries": entries}
	if p.Next != "" {
		resp["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborDeleteDashboards(input cbor.Map, c *echo.Context) error {
	names := cborStrList(input, "DashboardNames")

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, a := range b.GetDashboardARNs(names) {
			h.deleteResourceTags(a)
		}
	}

	if err := h.Backend.DeleteDashboards(names); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}
