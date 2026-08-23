package cloudwatch

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// datasetErrorStatus maps a dataset backend error to its HTTP status/code.
func datasetErrorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrDatasetNotFound):
		return http.StatusBadRequest, errResourceNotFoundException
	case errors.Is(err, ErrValidation):
		return http.StatusBadRequest, "InvalidParameterValue"
	default:
		return http.StatusInternalServerError, "InternalFailure"
	}
}

func (h *Handler) handleGetDataset(form url.Values, c *echo.Context) error {
	ds, err := h.Backend.GetDataset(form.Get("DatasetIdentifier"))
	if err != nil {
		status, code := datasetErrorStatus(err)

		return h.xmlError(c, status, code, err.Error())
	}

	type getDatasetResult struct {
		Arn       string `xml:"Arn"`
		DatasetID string `xml:"DatasetId"`
		KmsKeyArn string `xml:"KmsKeyArn,omitempty"`
	}
	type response struct {
		XMLName   xml.Name         `xml:"GetDatasetResponse"`
		Xmlns     string           `xml:"xmlns,attr"`
		RequestID string           `xml:"ResponseMetadata>RequestId"`
		Result    getDatasetResult `xml:"GetDatasetResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result: getDatasetResult{
			Arn:       ds.Arn,
			DatasetID: ds.DatasetID,
			KmsKeyArn: ds.KmsKeyArn,
		},
	})
}

func (h *Handler) handleAssociateDatasetKmsKey(form url.Values, c *echo.Context) error {
	err := h.Backend.AssociateDatasetKmsKey(
		form.Get("DatasetIdentifier"),
		form.Get("KmsKeyArn"),
	)
	if err != nil {
		status, code := datasetErrorStatus(err)

		return h.xmlError(c, status, code, err.Error())
	}

	type response struct {
		XMLName   xml.Name       `xml:"AssociateDatasetKmsKeyResponse"`
		Result    xmlEmptyResult `xml:"AssociateDatasetKmsKeyResult"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDisassociateDatasetKmsKey(form url.Values, c *echo.Context) error {
	if err := h.Backend.DisassociateDatasetKmsKey(form.Get("DatasetIdentifier")); err != nil {
		status, code := datasetErrorStatus(err)

		return h.xmlError(c, status, code, err.Error())
	}

	type response struct {
		XMLName   xml.Name       `xml:"DisassociateDatasetKmsKeyResponse"`
		Result    xmlEmptyResult `xml:"DisassociateDatasetKmsKeyResult"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
