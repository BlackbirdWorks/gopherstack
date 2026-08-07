package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type instanceView struct {
	InstanceArn     string  `json:"InstanceArn"`
	OwnerAccountID  string  `json:"OwnerAccountId"`
	IdentityStoreID string  `json:"IdentityStoreId"`
	Name            string  `json:"Name"`
	Status          string  `json:"Status"`
	CreatedDate     float64 `json:"CreatedDate,omitempty"`
}

func (h *Handler) handleListInstances(c *echo.Context, body []byte) error {
	var req struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	// Body is optional for ListInstances; ignore unmarshal errors on empty/garbage.
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	instances := h.Backend.ListInstances()
	views := make([]instanceView, 0, len(instances))
	for _, inst := range instances {
		views = append(views, instanceView{
			InstanceArn:     inst.InstanceArn,
			OwnerAccountID:  inst.OwnerAccountID,
			IdentityStoreID: inst.IdentityStoreID,
			Name:            inst.Name,
			Status:          inst.Status,
			CreatedDate:     float64(inst.CreatedDate.Unix()),
		})
	}

	page, next := paginateBy(views, req.MaxResults, req.NextToken, func(v instanceView) string {
		return v.InstanceArn
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"Instances":  page,
		keyNextToken: next,
	})
}

func (h *Handler) handleCreateInstance(c *echo.Context, body []byte) error {
	var req struct {
		Name            string `json:"Name"`
		OwnerAccountID  string `json:"OwnerAccountId"`
		IdentityStoreID string `json:"IdentityStoreId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	inst, err := h.Backend.CreateInstance(req.Name, req.OwnerAccountID, req.IdentityStoreID)
	if err != nil {
		return handleBackendError(c, err, "failed to create instance")
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyInstanceArn: inst.InstanceArn,
	})
}

func (h *Handler) handleDescribeInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	inst, err := h.Backend.DescribeInstance(req.InstanceArn)
	if err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	// Real DescribeInstanceOutput has no Tags member (gopherstack previously
	// invented one here) -- tags are fetched separately via
	// ListTagsForResource, matching every other taggable ssoadmin resource;
	// see awsAwsjson11_deserializeOpDocumentDescribeInstanceOutput in the real
	// SDK's deserializers.go.
	//
	// EncryptionConfigurationDetails: this SDK version has no
	// Put/UpdateInstanceEncryptionConfiguration op at all -- encryption
	// configuration is read-only via this API -- so every instance this
	// backend can produce has the one true default state real AWS documents
	// for an instance that was never (and can never be, via this API)
	// switched to a customer-managed KMS key: ENABLED / AWS_OWNED_KMS_KEY,
	// no KmsKeyArn. This is a real constant, not per-instance fabricated data.
	//
	// StatusReason (top-level, distinct from EncryptionConfigurationDetails'
	// own EncryptionStatusReason) is documented as "particularly useful when
	// an instance is in a non-ACTIVE state" -- this backend's instances are
	// always ACTIVE (no CREATE_FAILED/DELETING-with-error path modeled), so
	// omitting it is wire-correct, not a gap.
	return writeJSON(c, http.StatusOK, map[string]any{
		keyInstanceArn:    inst.InstanceArn,
		"OwnerAccountId":  inst.OwnerAccountID,
		"IdentityStoreId": inst.IdentityStoreID,
		keyName:           inst.Name,
		keyStatus:         inst.Status,
		"CreatedDate":     float64(inst.CreatedDate.Unix()),
		"EncryptionConfigurationDetails": map[string]any{
			"EncryptionStatus": "ENABLED",
			"KeyType":          "AWS_OWNED_KMS_KEY",
		},
	})
}

func (h *Handler) handleDeleteInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	if err := h.Backend.DeleteInstance(req.InstanceArn); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleUpdateInstance(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		Name        string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if err := h.Backend.UpdateInstance(req.InstanceArn, req.Name); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}
