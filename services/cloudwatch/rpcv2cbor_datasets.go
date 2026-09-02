package cloudwatch

import (
	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborGetDataset(input cbor.Map, c *echo.Context) error {
	ds, err := h.Backend.GetDataset(cborStr(input, "DatasetIdentifier"))
	if err != nil {
		status, code := datasetCBORErrorStatus(err)

		return h.cborError(c, status, code, err.Error())
	}

	resp := cbor.Map{
		keyArn:      cbor.String(ds.Arn),
		"DatasetId": cbor.String(ds.DatasetID),
	}
	if ds.KmsKeyArn != "" {
		resp["KmsKeyArn"] = cbor.String(ds.KmsKeyArn)
	}

	return writeCBOR(c, resp)
}

func (h *Handler) cborAssociateDatasetKmsKey(input cbor.Map, c *echo.Context) error {
	err := h.Backend.AssociateDatasetKmsKey(
		cborStr(input, "DatasetIdentifier"),
		cborStr(input, "KmsKeyArn"),
	)
	if err != nil {
		status, code := datasetCBORErrorStatus(err)

		return h.cborError(c, status, code, err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborDisassociateDatasetKmsKey(input cbor.Map, c *echo.Context) error {
	if err := h.Backend.DisassociateDatasetKmsKey(cborStr(input, "DatasetIdentifier")); err != nil {
		status, code := datasetCBORErrorStatus(err)

		return h.cborError(c, status, code, err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}
