package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ErrUnknownOperation is returned when the requested KMS operation is not supported.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// defaultKMSTagsLimit is the default maximum number of tags returned by ListResourceTags.
const defaultKMSTagsLimit int32 = 50

type listResourceTagsInput struct {
	Limit  *int32 `json:"Limit,omitempty"`
	KeyID  string `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
	Marker string `json:"Marker,omitempty"`
}

type kmsTagEntry struct {
	TagKey   string `json:"TagKey"`
	TagValue string `json:"TagValue"`
}

type tagResourceInput struct {
	KeyID string        `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
	Tags  []kmsTagEntry `json:"Tags"`
}

type listResourceTagsOutput struct {
	NextMarker string        `json:"NextMarker,omitempty"`
	Tags       []kmsTagEntry `json:"Tags"`
	Truncated  bool          `json:"Truncated"`
}

type untagResourceInput struct {
	KeyID   string   `json:"KeyId"` //nolint:tagliatelle // AWS API uses KeyId
	TagKeys []string `json:"TagKeys"`
}

// Handler is the Echo HTTP handler for KMS operations.
type Handler struct {
	Backend       StorageBackend
	actions       map[string]kmsActionFn
	janitor       *Janitor
	tags          map[string]*tags.Tags
	tagsMu        *lockmetrics.RWMutex
	DefaultRegion string
}

// NewHandler creates a new KMS handler with the given storage backend and logger.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
		tags:    make(map[string]*tags.Tags),
		tagsMu:  lockmetrics.New("kms.tags"),
	}
	h.actions = h.buildDispatchTable()

	return h
}

// WithJanitor attaches a background key-deletion janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}
		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if one is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Reset clears all state in the backend and the handler's tag store.
// It is used by the POST /_gopherstack/reset endpoint for CI pipelines.
func (h *Handler) Reset() {
	type resetter interface{ Reset() }
	if r, ok := h.Backend.(resetter); ok {
		r.Reset()
	}

	h.tagsMu.Lock("Reset")
	defer h.tagsMu.Unlock()

	for _, t := range h.tags {
		if t != nil {
			t.Close()
		}
	}

	h.tags = make(map[string]*tags.Tags)
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = tags.New("kms." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "KMS"
}

// GetSupportedOperations returns the list of supported KMS operations (sorted alphabetically).
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CancelKeyDeletion",
		"ConnectCustomKeyStore",
		"CreateAlias",
		"CreateCustomKeyStore",
		"CreateGrant",
		"CreateKey",
		"Decrypt",
		"DeleteAlias",
		"DeleteCustomKeyStore",
		"DeleteImportedKeyMaterial",
		"DeriveSharedSecret",
		"DescribeCustomKeyStores",
		"DescribeKey",
		"DisableKey",
		"DisableKeyRotation",
		"DisconnectCustomKeyStore",
		"EnableKey",
		"EnableKeyRotation",
		"Encrypt",
		"GenerateDataKey",
		"GenerateDataKeyPair",
		"GenerateDataKeyPairWithoutPlaintext",
		"GenerateDataKeyWithoutPlaintext",
		"GenerateMac",
		"GenerateRandom",
		"GetKeyPolicy",
		"GetKeyRotationStatus",
		"GetParametersForImport",
		"GetPublicKey",
		"ImportKeyMaterial",
		"ListAliases",
		"ListGrants",
		"ListKeyPolicies",
		"ListKeyRotations",
		"ListKeys",
		"ListResourceTags",
		"ListRetirableGrants",
		"PutKeyPolicy",
		"ReEncrypt",
		"ReplicateKey",
		"RetireGrant",
		"RevokeGrant",
		"RotateKeyOnDemand",
		"ScheduleKeyDeletion",
		"Sign",
		"TagResource",
		"UntagResource",
		"UpdateAlias",
		"UpdateCustomKeyStore",
		"UpdateKeyDescription",
		"UpdatePrimaryRegion",
		"Verify",
		"VerifyMac",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kms" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this KMS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a matcher that identifies KMS requests by the X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "TrentService")
	}
}

// MatchPriority returns the routing priority for the KMS handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderPartial
}

// ExtractOperation extracts the KMS operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource returns the key ID from the request body when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if keyID, ok := data["KeyId"].(string); ok {
		return keyID
	}

	return ""
}

// Handler returns the Echo handler function for KMS operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"KMS", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type kmsActionFn func(region string, body []byte) (any, error)

// buildDispatchTable merges key lifecycle, crypto, alias/rotation, and tag actions into a single lookup map.
func (h *Handler) buildDispatchTable() map[string]kmsActionFn {
	table := h.buildKeyLifecycleActions()
	maps.Copy(table, h.buildCryptoActions())
	maps.Copy(table, h.buildAliasRotationActions())
	maps.Copy(table, h.buildGrantPolicyActions())
	maps.Copy(table, h.buildTagActions())
	maps.Copy(table, h.buildNewOpsActions())

	return table
}

// buildKeyLifecycleActions returns dispatch entries for key creation, description, listing and deletion.
func (h *Handler) buildKeyLifecycleActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateKey": func(region string, b []byte) (any, error) {
			var input CreateKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			input.Region = region

			return h.Backend.CreateKey(&input)
		},
		"DescribeKey": func(_ string, b []byte) (any, error) {
			var input DescribeKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeKey(&input)
		},
		"ListKeys": func(_ string, b []byte) (any, error) {
			var input ListKeysInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeys(&input)
		},
		"DisableKey": func(_ string, b []byte) (any, error) {
			var input DisableKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKey(&input)
		},
		"EnableKey": func(_ string, b []byte) (any, error) {
			var input EnableKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKey(&input)
		},
		"ScheduleKeyDeletion": func(_ string, b []byte) (any, error) {
			var input ScheduleKeyDeletionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ScheduleKeyDeletion(&input)
		},
		"CancelKeyDeletion": func(_ string, b []byte) (any, error) {
			var input CancelKeyDeletionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CancelKeyDeletion(&input)
		},
		"ImportKeyMaterial": func(_ string, b []byte) (any, error) {
			var input ImportKeyMaterialInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.ImportKeyMaterial(&input)
		},
		"DeleteImportedKeyMaterial": func(_ string, b []byte) (any, error) {
			var input DeleteImportedKeyMaterialInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteImportedKeyMaterial(&input)
		},
	}
}

// buildCryptoActions returns dispatch entries for encrypt, decrypt, sign, verify, and data-key operations.
func (h *Handler) buildCryptoActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"Encrypt": func(_ string, b []byte) (any, error) {
			var input EncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Encrypt(&input)
		},
		"Decrypt": func(_ string, b []byte) (any, error) {
			var input DecryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Decrypt(&input)
		},
		"GenerateDataKey": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKey(&input)
		},
		"GenerateDataKeyWithoutPlaintext": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyWithoutPlaintext(&input)
		},
		"ReEncrypt": func(_ string, b []byte) (any, error) {
			var input ReEncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReEncrypt(&input)
		},
		"Sign": func(_ string, b []byte) (any, error) {
			var input SignInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Sign(&input)
		},
		"Verify": func(_ string, b []byte) (any, error) {
			var input VerifyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Verify(&input)
		},
		"GetPublicKey": func(_ string, b []byte) (any, error) {
			var input GetPublicKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPublicKey(&input)
		},
	}
}

// buildAliasRotationActions returns dispatch entries for alias management and key rotation.
func (h *Handler) buildAliasRotationActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateAlias": func(_ string, b []byte) (any, error) {
			var input CreateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateAlias(&input)
		},
		"UpdateAlias": func(_ string, b []byte) (any, error) {
			var input UpdateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateAlias(&input)
		},
		"DeleteAlias": func(_ string, b []byte) (any, error) {
			var input DeleteAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteAlias(&input)
		},
		"ListAliases": func(_ string, b []byte) (any, error) {
			var input ListAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAliases(&input)
		},
		"EnableKeyRotation": func(_ string, b []byte) (any, error) {
			var input EnableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKeyRotation(&input)
		},
		"DisableKeyRotation": func(_ string, b []byte) (any, error) {
			var input DisableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKeyRotation(&input)
		},
		"GetKeyRotationStatus": func(_ string, b []byte) (any, error) {
			var input GetKeyRotationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyRotationStatus(&input)
		},
	}
}

// buildGrantPolicyActions returns dispatch entries for grant and key policy operations.
func (h *Handler) buildGrantPolicyActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateGrant": func(_ string, b []byte) (any, error) {
			var input CreateGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateGrant(&input)
		},
		"ListGrants": func(_ string, b []byte) (any, error) {
			var input ListGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListGrants(&input)
		},
		"RevokeGrant": func(_ string, b []byte) (any, error) {
			var input RevokeGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RevokeGrant(&input)
		},
		"RetireGrant": func(_ string, b []byte) (any, error) {
			var input RetireGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RetireGrant(&input)
		},
		"ListRetirableGrants": func(_ string, b []byte) (any, error) {
			var input ListRetirableGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListRetirableGrants(&input)
		},
		"PutKeyPolicy": func(_ string, b []byte) (any, error) {
			var input PutKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.PutKeyPolicy(&input)
		},
		"GetKeyPolicy": func(_ string, b []byte) (any, error) {
			var input GetKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyPolicy(&input)
		},
	}
}

// buildTagActions returns dispatch entries for KMS resource tag operations.
func (h *Handler) buildTagActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"ListResourceTags": func(_ string, b []byte) (any, error) {
			var input listResourceTagsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			tagMap := h.getTags(input.KeyID)
			tagList := make([]kmsTagEntry, 0, len(tagMap))
			for k, v := range tagMap {
				tagList = append(tagList, kmsTagEntry{TagKey: k, TagValue: v})
			}

			sort.Slice(tagList, func(i, j int) bool { return tagList[i].TagKey < tagList[j].TagKey })

			startIdx := parseMarker(input.Marker)
			limit := defaultKMSTagsLimit

			if input.Limit != nil && *input.Limit > 0 {
				limit = *input.Limit
			}

			if startIdx >= len(tagList) {
				return &listResourceTagsOutput{Tags: []kmsTagEntry{}, Truncated: false}, nil
			}

			end := startIdx + int(limit)

			var nextMarker string
			if end < len(tagList) {
				nextMarker = strconv.Itoa(end)
			} else {
				end = len(tagList)
			}

			return &listResourceTagsOutput{
				Tags:       tagList[startIdx:end],
				NextMarker: nextMarker,
				Truncated:  nextMarker != "",
			}, nil
		},
		"TagResource": func(_ string, b []byte) (any, error) {
			var input tagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			kv := make(map[string]string, len(input.Tags))
			for _, t := range input.Tags {
				kv[t.TagKey] = t.TagValue
			}
			h.setTags(input.KeyID, kv)

			return struct{}{}, nil
		},
		"UntagResource": func(_ string, b []byte) (any, error) {
			var input untagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.KeyID, input.TagKeys)

			return struct{}{}, nil
		},
	}
}

// buildNewOpsActions returns dispatch entries for newly implemented KMS operations.
// Delegates to sub-builders to stay within gocognit limits.
func (h *Handler) buildNewOpsActions() map[string]kmsActionFn {
	m := make(map[string]kmsActionFn)
	maps.Copy(m, h.buildCustomKeyStoreActions())
	maps.Copy(m, h.buildGenerateAndMacActions())
	maps.Copy(m, h.buildReplicationAndMaintenanceActions())

	return m
}

// buildCustomKeyStoreActions returns dispatch entries for custom key store and ECDH operations.
func (h *Handler) buildCustomKeyStoreActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateCustomKeyStore": func(_ string, b []byte) (any, error) {
			var input CreateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateCustomKeyStore(&input)
		},
		"DeleteCustomKeyStore": func(_ string, b []byte) (any, error) {
			var input DeleteCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteCustomKeyStore(&input)
		},
		"DescribeCustomKeyStores": func(_ string, b []byte) (any, error) {
			var input DescribeCustomKeyStoresInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeCustomKeyStores(&input)
		},
		"ConnectCustomKeyStore": func(_ string, b []byte) (any, error) {
			var input ConnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.ConnectCustomKeyStore(&input)
		},
		"DisconnectCustomKeyStore": func(_ string, b []byte) (any, error) {
			var input DisconnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisconnectCustomKeyStore(&input)
		},
		"DeriveSharedSecret": func(_ string, b []byte) (any, error) {
			var input DeriveSharedSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeriveSharedSecret(&input)
		},
	}
}

// buildGenerateAndMacActions returns dispatch entries for data key pair, MAC, and random operations.
func (h *Handler) buildGenerateAndMacActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"GenerateDataKeyPair": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyPairInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPair(&input)
		},
		"GenerateDataKeyPairWithoutPlaintext": func(_ string, b []byte) (any, error) {
			var input GenerateDataKeyPairWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPairWithoutPlaintext(&input)
		},
		"GenerateMac": func(_ string, b []byte) (any, error) {
			var input GenerateMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateMac(&input)
		},
		"GenerateRandom": func(_ string, b []byte) (any, error) {
			var input GenerateRandomInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateRandom(&input)
		},
		"VerifyMac": func(_ string, b []byte) (any, error) {
			var input VerifyMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.VerifyMac(&input)
		},
	}
}

func (h *Handler) buildReplicationAndMaintenanceActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"GetParametersForImport": func(_ string, b []byte) (any, error) {
			var input GetParametersForImportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParametersForImport(&input)
		},
		"ListKeyPolicies": func(_ string, b []byte) (any, error) {
			var input ListKeyPoliciesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyPolicies(&input)
		},
		"ListKeyRotations": func(_ string, b []byte) (any, error) {
			var input ListKeyRotationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyRotations(&input)
		},
		"ReplicateKey": func(_ string, b []byte) (any, error) {
			var input ReplicateKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReplicateKey(&input)
		},
		"RotateKeyOnDemand": func(_ string, b []byte) (any, error) {
			var input RotateKeyOnDemandInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RotateKeyOnDemand(&input)
		},
		"UpdateCustomKeyStore": func(_ string, b []byte) (any, error) {
			var input UpdateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateCustomKeyStore(&input)
		},
		"UpdateKeyDescription": func(_ string, b []byte) (any, error) {
			var input UpdateKeyDescriptionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateKeyDescription(&input)
		},
		"UpdatePrimaryRegion": func(_ string, b []byte) (any, error) {
			var input UpdatePrimaryRegionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdatePrimaryRegion(&input)
		},
	}
}

// dispatch routes the KMS operation to the appropriate backend method.
func (h *Handler) dispatch(_ context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	fn, ok := h.actions[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(region, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a structured error response for a KMS operation failure.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errorType string

	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrKeyNotFound), errors.Is(reqErr, ErrAliasNotFound), errors.Is(reqErr, ErrGrantNotFound):
		errorType = "NotFoundException"
	case errors.Is(reqErr, ErrCustomKeyStoreNotFound):
		errorType = "CustomKeyStoreNotFoundException"
	case errors.Is(reqErr, ErrKeyDisabled):
		errorType = "DisabledException"
	case errors.Is(reqErr, ErrKeyInvalidState):
		errorType = "KMSInvalidStateException"
	case errors.Is(reqErr, ErrInvalidKeyUsage):
		errorType = "InvalidKeyUsageException"
	case errors.Is(reqErr, ErrAliasAlreadyExists):
		errorType = "AlreadyExistsException"
	case errors.Is(reqErr, ErrCustomKeyStoreAlreadyExists):
		errorType = "CustomKeyStoreNameInUseException"
	case errors.Is(reqErr, ErrInvalidCiphertext), errors.Is(reqErr, ErrCiphertextTooShort):
		errorType = "InvalidCiphertextException"
	case errors.Is(reqErr, ErrInvalidSignature):
		errorType = "KMSInvalidSignatureException"
	case errors.Is(reqErr, ErrUnsupportedOrigin):
		errorType = "UnsupportedOperationException"
	case errors.Is(reqErr, ErrValidation), errors.Is(reqErr, ErrInvalidDataKeySize):
		errorType = "ValidationException"
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = "UnknownOperationException"
	default:
		errorType = "InternalServiceError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "KMS internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "KMS request error", "error", reqErr, "action", action)
	}

	payload, _ := json.Marshal(ErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	})

	return c.JSONBlob(statusCode, payload)
}

// TaggedKeyInfo contains a KMS key's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedKeyInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedKeys returns a snapshot of all KMS keys with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (h *Handler) TaggedKeys() []TaggedKeyInfo {
	out, err := h.Backend.ListKeys(&ListKeysInput{})
	if err != nil {
		return nil
	}

	h.tagsMu.RLock("TaggedKeys")
	defer h.tagsMu.RUnlock()

	result := make([]TaggedKeyInfo, 0, len(out.Keys))

	for _, k := range out.Keys {
		var tagMap map[string]string
		if t := h.tags[k.KeyID]; t != nil {
			tagMap = t.Clone()
		}

		result = append(result, TaggedKeyInfo{ARN: k.KeyArn, Tags: tagMap})
	}

	return result
}

// TagKeyByARN applies tags to the KMS key identified by its ARN.
func (h *Handler) TagKeyByARN(keyARN string, newTags map[string]string) error {
	out, err := h.Backend.ListKeys(&ListKeysInput{})
	if err != nil {
		return err
	}

	for _, k := range out.Keys {
		if k.KeyArn == keyARN {
			h.setTags(k.KeyID, newTags)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrKeyNotFound, keyARN)
}

// UntagKeyByARN removes the specified tag keys from the KMS key identified by its ARN.
func (h *Handler) UntagKeyByARN(keyARN string, tagKeys []string) error {
	out, err := h.Backend.ListKeys(&ListKeysInput{})
	if err != nil {
		return err
	}

	for _, k := range out.Keys {
		if k.KeyArn == keyARN {
			h.removeTags(k.KeyID, tagKeys)

			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrKeyNotFound, keyARN)
}
