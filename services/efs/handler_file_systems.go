package efs

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createFileSystemBody struct {
	CreationToken            string     `json:"CreationToken"`
	PerformanceMode          string     `json:"PerformanceMode"`
	ThroughputMode           string     `json:"ThroughputMode"`
	KmsKeyID                 string     `json:"KmsKeyId"`
	AvailabilityZoneName     string     `json:"AvailabilityZoneName"`
	Tags                     []tagEntry `json:"Tags"`
	ProvisionedThroughputMib float64    `json:"ProvisionedThroughputInMibps"`
	Encrypted                bool       `json:"Encrypted"`
}

func (h *Handler) handleCreateFileSystem(c *echo.Context, body []byte) error {
	var in createFileSystemBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.CreationToken == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "CreationToken is required"))
	}

	req := CreateFileSystemRequest{
		CreationToken:            in.CreationToken,
		PerformanceMode:          in.PerformanceMode,
		ThroughputMode:           in.ThroughputMode,
		KmsKeyID:                 in.KmsKeyID,
		AvailabilityZoneName:     in.AvailabilityZoneName,
		ProvisionedThroughputMib: in.ProvisionedThroughputMib,
		Encrypted:                in.Encrypted,
		Tags:                     tagsFromEntries(in.Tags),
	}

	fs, err := h.Backend.CreateFileSystem(h.contextWithRegion(c), req)
	if err != nil {
		if errors.Is(err, ErrCreationTokenExists) {
			// Identical token with identical args: return existing fs with 200 OK.
			return c.JSON(http.StatusOK, fsToResponse(fs))
		}
		if errors.Is(err, ErrAlreadyExists) {
			// Different args: return 409 with file system ID in body.
			c.Response().Header().Set("x-amzn-ErrorType", "FileSystemAlreadyExists")
			resp := map[string]string{
				"ErrorCode":    "FileSystemAlreadyExists",
				"Message":      err.Error(),
				"FileSystemId": fs.FileSystemID,
			}

			return c.JSON(http.StatusConflict, resp)
		}

		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, fsToResponse(fs))
}

func (h *Handler) handleDescribeFileSystems(c *echo.Context, fileSystemID string) error {
	// Also accept ?FileSystemId= query param.
	if fileSystemID == "" {
		fileSystemID = c.Request().URL.Query().Get(keyFileSystemID)
	}

	creationToken := c.Request().URL.Query().Get("CreationToken")
	marker := c.Request().URL.Query().Get("Marker")
	maxItems := queryInt(c, "MaxItems", defaultMaxItems)

	fsList, nextMarker, err := h.Backend.DescribeFileSystems(
		h.contextWithRegion(c), fileSystemID, creationToken, marker, maxItems,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(fsList))
	for _, fs := range fsList {
		items = append(items, fsToResponse(fs))
	}

	resp := map[string]any{
		"FileSystems": items,
	}
	if nextMarker != "" {
		resp["NextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteFileSystem(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteFileSystem(h.contextWithRegion(c), fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func fsToResponse(fs *FileSystem) map[string]any {
	resp := map[string]any{
		keyFileSystemID:        fs.FileSystemID,
		"FileSystemArn":        fs.FileSystemArn,
		"CreationToken":        fs.CreationToken,
		"PerformanceMode":      fs.PerformanceMode,
		"ThroughputMode":       fs.ThroughputMode,
		keyLifeCycleState:      fs.LifeCycleState,
		"Encrypted":            fs.Encrypted,
		"NumberOfMountTargets": fs.NumberOfMountTargets,
		keyOwnerID:             fs.AccountID,
		keyTags:                tagsToEntries(fs.Tags.Clone()),
		"CreationTime":         float64(fs.CreationTime.Unix()),
		"SizeInBytes": map[string]any{
			"Value":           0,
			"ValueInIA":       0,
			"ValueInStandard": 0,
			"ValueInArchive":  0,
			"Timestamp":       float64(fs.CreationTime.Unix()),
		},
		"FileSystemProtection": map[string]any{
			"ReplicationOverwriteProtection": fs.ReplicationOverwriteProtection,
		},
	}
	if fs.Name != "" {
		resp["Name"] = fs.Name
	}
	if fs.KmsKeyID != "" {
		resp["KmsKeyId"] = fs.KmsKeyID
	}
	if fs.AvailabilityZoneName != "" {
		resp["AvailabilityZoneName"] = fs.AvailabilityZoneName
		resp["AvailabilityZoneId"] = fs.AvailabilityZoneID
	}
	if fs.ProvisionedThroughputMib > 0 {
		resp["ProvisionedThroughputInMibps"] = fs.ProvisionedThroughputMib
	}

	return resp
}

type updateFileSystemBody struct {
	ThroughputMode           string  `json:"ThroughputMode,omitempty"`
	ProvisionedThroughputMib float64 `json:"ProvisionedThroughputInMibps,omitempty"`
}

func (h *Handler) handleUpdateFileSystem(c *echo.Context, fileSystemID string, body []byte) error {
	var in updateFileSystemBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	req := UpdateFileSystemRequest(in)

	fs, err := h.Backend.UpdateFileSystem(h.contextWithRegion(c), fileSystemID, req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusAccepted, fsToResponse(fs))
}
