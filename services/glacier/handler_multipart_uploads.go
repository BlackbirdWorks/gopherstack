package glacier

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleInitiateMultipartUpload(c *echo.Context, vaultName string, _ []byte) error {
	description := c.Request().Header.Get("X-Amz-Archive-Description")
	if err := validateDescription(description); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	partSizeStr := c.Request().Header.Get("X-Amz-Part-Size")
	if partSizeStr == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"X-Amz-Part-Size header is required for InitiateMultipartUpload")
	}

	partSize, err := parseInt64Header(partSizeStr)
	if err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid X-Amz-Part-Size header",
		)
	}

	up, err := h.Backend.InitiateMultipartUpload(
		h.AccountID,
		h.DefaultRegion,
		vaultName,
		description,
		partSize,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/multipart-uploads/" + up.MultipartUploadID
	c.Response().Header().Set("Location", location)
	c.Response().Header().Set("X-Amz-Multipart-Upload-Id", up.MultipartUploadID)

	// AWS returns the chosen part size in the response so clients can verify.
	if up.PartSizeInBytes > 0 {
		c.Response().Header().Set("X-Amz-Part-Size", strconv.FormatInt(up.PartSizeInBytes, 10))
	}

	return c.JSON(http.StatusCreated, initiateMultipartUploadResponse{
		Location:          location,
		MultipartUploadID: up.MultipartUploadID,
	})
}

func (h *Handler) handleUploadMultipartPart(
	c *echo.Context,
	vaultName, uploadID string,
	_ []byte,
) error {
	rangeHeader := c.Request().Header.Get("Content-Range")
	if rangeHeader == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Content-Range header is required for UploadMultipartPart")
	}

	// AWS requires format "bytes START-END/*"
	if !isValidMultipartRange(rangeHeader) {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Content-Range must be in the form \"bytes START-END/*\"")
	}

	checksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")

	if err := h.Backend.UploadMultipartPart(
		h.AccountID, h.DefaultRegion, vaultName, uploadID, rangeHeader, checksum,
	); err != nil {
		return h.writeBackendError(c, err)
	}

	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", checksum)

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCompleteMultipartUpload(
	c *echo.Context,
	vaultName, uploadID string,
	_ []byte,
) error {
	archiveSizeStr := c.Request().Header.Get("X-Amz-Archive-Size")
	if archiveSizeStr == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"X-Amz-Archive-Size header is required for CompleteMultipartUpload")
	}

	checksum := c.Request().Header.Get("X-Amz-Sha256-Tree-Hash")
	if checksum == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"X-Amz-Sha256-Tree-Hash header is required for CompleteMultipartUpload")
	}

	archiveSize, err := parseInt64Header(archiveSizeStr)
	if err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid X-Amz-Archive-Size header",
		)
	}

	a, err := h.Backend.CompleteMultipartUpload(
		h.AccountID, h.DefaultRegion, vaultName, uploadID, checksum, archiveSize,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/archives/" + a.ArchiveID
	c.Response().Header().Set("X-Amz-Archive-Id", a.ArchiveID)
	c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", a.SHA256TreeHash)
	c.Response().Header().Set("Location", location)

	return c.JSON(http.StatusCreated, completeMultipartUploadResponse{
		ArchiveID: a.ArchiveID,
		Checksum:  a.SHA256TreeHash,
		Location:  location,
	})
}

func (h *Handler) handleAbortMultipartUpload(c *echo.Context, vaultName, uploadID string) error {
	if err := h.Backend.AbortMultipartUpload(h.AccountID, h.DefaultRegion, vaultName, uploadID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListMultipartUploads(c *echo.Context, vaultName string) error {
	ups := h.Backend.ListMultipartUploads(h.AccountID, h.DefaultRegion, vaultName)
	items := make([]MultipartUpload, 0, len(ups))

	for _, up := range ups {
		items = append(items, *up)
	}

	items, nextMarker, pErr := paginateUploadList(c, items)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	return c.JSON(http.StatusOK, listMultipartUploadsResponse{
		Marker:      nextMarker,
		UploadsList: items,
	})
}

// paginateUploadList applies marker+limit pagination to a multipart-upload slice.
func paginateUploadList( //nolint:dupl // three typed paginate funcs share identical structure
	c *echo.Context,
	items []MultipartUpload,
) ([]MultipartUpload, *string, error) {
	marker := c.QueryParam("marker")
	if marker != "" {
		marker = decodeMarker(marker)
	}

	if marker != "" {
		start := 0

		for start < len(items) && items[start].MultipartUploadID != marker {
			start++
		}

		if start < len(items) {
			items = items[start+1:]
		} else {
			items = items[:0]
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return items, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListUploadsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListUploadsLimit,
		)
	}

	if n >= len(items) {
		return items, nil, nil
	}

	last := encodeMarker(items[n-1].MultipartUploadID)

	return items[:n], &last, nil
}

func (h *Handler) handleListParts(c *echo.Context, vaultName, uploadID string) error {
	resp, err := h.Backend.ListParts(h.AccountID, h.DefaultRegion, vaultName, uploadID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Apply marker+limit pagination to the parts list.
	parts, nextMarker, pErr := paginatePartList(c, resp.Parts)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	resp.Parts = parts
	resp.Marker = nextMarker

	return c.JSON(http.StatusOK, resp)
}

// paginatePartList applies marker+limit pagination to a parts slice.
// Marker is compared to RangeInBytes of each part.
func paginatePartList(
	c *echo.Context, parts []MultipartPart,
) ([]MultipartPart, *string, error) {
	if marker := c.QueryParam("marker"); marker != "" {
		start := 0

		for start < len(parts) && parts[start].RangeInBytes != marker {
			start++
		}

		if start < len(parts) {
			parts = parts[start+1:]
		} else {
			parts = parts[:0]
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return parts, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListUploadsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListUploadsLimit,
		)
	}

	if n >= len(parts) {
		return parts, nil, nil
	}

	last := parts[n-1].RangeInBytes

	return parts[:n], &last, nil
}

// parseInt64Header parses an integer value from a header string.
func parseInt64Header(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}

// isValidMultipartRange reports whether rangeHeader is in the AWS multipart upload
// Content-Range format: "bytes START-END/*" where START and END are non-negative integers.
func isValidMultipartRange(rangeHeader string) bool {
	const prefix = "bytes "
	if !strings.HasPrefix(rangeHeader, prefix) {
		return false
	}

	rest := rangeHeader[len(prefix):]

	const suffix = "/*"
	if !strings.HasSuffix(rest, suffix) {
		return false
	}

	rangePart := rest[:len(rest)-len(suffix)]
	dashIdx := strings.IndexByte(rangePart, '-')

	if dashIdx <= 0 || dashIdx == len(rangePart)-1 {
		return false
	}

	startStr := rangePart[:dashIdx]
	endStr := rangePart[dashIdx+1:]

	start, err1 := strconv.ParseInt(startStr, 10, 64)
	end, err2 := strconv.ParseInt(endStr, 10, 64)

	return err1 == nil && err2 == nil && start >= 0 && end >= start
}
