package support

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
)

const (
	linkPathPrefix     = "/_gopherstack/support/attachments/"
	uploadLinkPath     = linkPathPrefix + "upload/"
	downloadLinkPath   = linkPathPrefix + "download/"
	schemeHTTPS        = "https"
	linkTimestampLayer = "2006-01-02T15:04:05.000Z"
)

//nolint:gochecknoglobals // ctxval key, package-private by construction
var requestBaseKey = ctxval.NewKey[string]("support.requestBase")

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == schemeHTTPS {
		scheme = schemeHTTPS
	}

	return scheme + "://" + r.Host
}

func linkURL(ctx context.Context, path, token string) string {
	return requestBaseKey.GetOr(ctx, "") + path + token
}

func linkTimestamp(t time.Time) string { return t.UTC().Format(linkTimestampLayer) }

type uploadRangeJSON struct {
	StartIndex *int32 `json:"startIndex"`
	EndIndex   *int32 `json:"endIndex,omitempty"`
}

type getAttachmentUploadLinksInput struct {
	FileSizeBytes *int64           `json:"fileSizeBytes,omitempty"`
	UploadRange   *uploadRangeJSON `json:"uploadRange,omitempty"`
	FileName      string           `json:"fileName"`
	UploadID      string           `json:"uploadId,omitempty"`
	DryRun        bool             `json:"dryRun,omitempty"`
}

type uploadURLView struct {
	ExpiryDate string `json:"expiryDate"`
	URL        string `json:"url"`
	PartIndex  int    `json:"partIndex"`
}

type getAttachmentUploadLinksOutput struct {
	UploadID      string          `json:"uploadId"`
	UploadURLs    []uploadURLView `json:"uploadUrls"`
	PartSizeBytes int64           `json:"partSizeBytes"`
	TotalParts    int             `json:"totalParts"`
	NextIndex     int             `json:"nextIndex,omitempty"`
}

func (h *Handler) handleGetAttachmentUploadLinks(
	ctx context.Context,
	in *getAttachmentUploadLinksInput,
) (*getAttachmentUploadLinksOutput, error) {
	req := UploadLinksInput{
		FileName: in.FileName, FileSizeBytes: in.FileSizeBytes, UploadID: in.UploadID, DryRun: in.DryRun,
	}

	if in.UploadRange != nil {
		if in.UploadRange.StartIndex == nil {
			return nil, fmt.Errorf("%w: uploadRange.startIndex is required", ErrValidation)
		}

		req.Range = &UploadRangeInput{Start: int(*in.UploadRange.StartIndex)}
		if in.UploadRange.EndIndex != nil {
			end := int(*in.UploadRange.EndIndex)
			req.Range.End = &end
		}
	}

	res, err := h.Backend.GetAttachmentUploadLinks(req)
	if err != nil {
		return nil, err
	}

	urls := make([]uploadURLView, 0, len(res.Links))
	for _, l := range res.Links {
		urls = append(urls, uploadURLView{
			PartIndex: l.PartIndex, ExpiryDate: linkTimestamp(l.Expiry), URL: linkURL(ctx, uploadLinkPath, l.Token),
		})
	}

	return &getAttachmentUploadLinksOutput{
		UploadID: res.UploadID, UploadURLs: urls, PartSizeBytes: res.PartSizeBytes,
		TotalParts: res.TotalParts, NextIndex: res.NextIndex,
	}, nil
}

type completedUploadJSON struct {
	PartIndex *int32 `json:"partIndex"`
	ETag      string `json:"eTag"`
}

type completeAttachmentUploadInput struct {
	UploadID         string                `json:"uploadId"`
	CompletedUploads []completedUploadJSON `json:"completedUploads"`
	DryRun           bool                  `json:"dryRun,omitempty"`
}

type completeAttachmentUploadOutput struct {
	UploadStatus string `json:"uploadStatus"`
}

func (h *Handler) handleCompleteAttachmentUpload(
	_ context.Context,
	in *completeAttachmentUploadInput,
) (*completeAttachmentUploadOutput, error) {
	parts := make([]CompletedPart, 0, len(in.CompletedUploads))

	for _, p := range in.CompletedUploads {
		if p.PartIndex == nil {
			return nil, fmt.Errorf("%w: completedUploads.partIndex is required", ErrValidation)
		}

		parts = append(parts, CompletedPart{PartIndex: int(*p.PartIndex), ETag: p.ETag})
	}

	status, err := h.Backend.CompleteAttachmentUpload(CompleteUploadInput{
		UploadID: in.UploadID, Parts: parts, DryRun: in.DryRun,
	})
	if err != nil {
		return nil, err
	}

	return &completeAttachmentUploadOutput{UploadStatus: status}, nil
}

type describeAttachmentUploadStatusInput struct {
	UploadID string `json:"uploadId"`
	DryRun   bool   `json:"dryRun,omitempty"`
}

type uploadProgressView struct {
	CompletedPartsCount int `json:"completedPartsCount"`
	TotalParts          int `json:"totalParts"`
}

type describeAttachmentUploadStatusOutput struct {
	FileName       string             `json:"fileName"`
	UploadStatus   string             `json:"uploadStatus"`
	UploadProgress uploadProgressView `json:"uploadProgress"`
}

func (h *Handler) handleDescribeAttachmentUploadStatus(
	_ context.Context,
	in *describeAttachmentUploadStatusInput,
) (*describeAttachmentUploadStatusOutput, error) {
	res, err := h.Backend.DescribeAttachmentUploadStatus(in.UploadID, in.DryRun)
	if err != nil {
		return nil, err
	}

	return &describeAttachmentUploadStatusOutput{
		FileName: res.FileName, UploadStatus: res.Status,
		UploadProgress: uploadProgressView{CompletedPartsCount: res.CompletedParts, TotalParts: res.TotalParts},
	}, nil
}

type getAttachmentDownloadLinkInput struct {
	AttachmentID string `json:"attachmentId"`
	DryRun       bool   `json:"dryRun,omitempty"`
}

type downloadURLView struct {
	ExpiryDate string `json:"expiryDate"`
	URL        string `json:"url"`
}

type getAttachmentDownloadLinkOutput struct {
	FileName    string          `json:"fileName"`
	DownloadURL downloadURLView `json:"downloadUrl"`
}

func (h *Handler) handleGetAttachmentDownloadLink(
	ctx context.Context,
	in *getAttachmentDownloadLinkInput,
) (*getAttachmentDownloadLinkOutput, error) {
	res, err := h.Backend.GetAttachmentDownloadLink(in.AttachmentID, in.DryRun)
	if err != nil {
		return nil, err
	}

	return &getAttachmentDownloadLinkOutput{
		FileName: res.FileName,
		DownloadURL: downloadURLView{
			ExpiryDate: linkTimestamp(res.Expiry), URL: linkURL(ctx, downloadLinkPath, res.Token),
		},
	}, nil
}

// RegisterInspectionRoutes mounts the emulator-hosted attachment upload and
// download endpoints that the presigned-URL operations hand out.
func (h *Handler) RegisterInspectionRoutes(e *echo.Echo) {
	e.PUT(uploadLinkPath+":token", h.servePutPart)
	e.GET(downloadLinkPath+":token", h.serveDownload)
}

func writeLinkError(c *echo.Context, status int, code, message string) error {
	return c.XML(status, struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: message})
}

func writeLinkFailure(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrLinkDenied):
		return writeLinkError(c, http.StatusForbidden, "AccessDenied", "Access Denied")
	case errors.Is(err, ErrValidation):
		return writeLinkError(c, http.StatusBadRequest, "InvalidRequest", errorMessage("ValidationError", err))
	default:
		return writeLinkError(c, http.StatusInternalServerError, "InternalError", "internal error")
	}
}

func (h *Handler) servePutPart(c *echo.Context) error {
	token := c.Param("token")

	want, err := h.Backend.PrepareUploadPart(token)
	if err != nil {
		return writeLinkFailure(c, err)
	}

	r := c.Request()
	data, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, want+1))

	if err != nil || int64(len(data)) != want {
		return writeLinkError(c, http.StatusBadRequest, "InvalidRequest",
			fmt.Sprintf("part body must be exactly %d bytes", want))
	}

	etag, err := h.Backend.PutUploadPart(token, data)
	if err != nil {
		return writeLinkFailure(c, err)
	}

	c.Response().Header().Set("ETag", etag)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) serveDownload(c *echo.Context) error {
	name, data, err := h.Backend.OpenDownload(c.Param("token"))
	if err != nil {
		return writeLinkFailure(c, err)
	}

	c.Response().Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}
