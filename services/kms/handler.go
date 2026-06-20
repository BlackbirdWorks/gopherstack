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

const (
	opCreateGrant                         = "CreateGrant"
	opDecrypt                             = "Decrypt"
	opDeriveSharedSecret                  = "DeriveSharedSecret"
	opDescribeKey                         = "DescribeKey"
	opEncrypt                             = "Encrypt"
	opGenerateDataKey                     = "GenerateDataKey"
	opGenerateDataKeyPair                 = "GenerateDataKeyPair"
	opGenerateDataKeyPairWithoutPlaintext = "GenerateDataKeyPairWithoutPlaintext"
	opGenerateDataKeyWithoutPlaintext     = "GenerateDataKeyWithoutPlaintext"
	opGenerateMac                         = "GenerateMac"
	opGetPublicKey                        = "GetPublicKey"
	opRetireGrant                         = "RetireGrant"
	opSign                                = "Sign"
	opVerify                              = "Verify"
	opVerifyMac                           = "VerifyMac"
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

// copyTagsToReplica copies the source key's tags to the replica and overlays any input tags.
func (h *Handler) copyTagsToReplica(sourceKeyID, replicaKeyID string, inputTags []Tag) {
	if sourceKeyID != "" {
		if srcTags := h.getTags(sourceKeyID); len(srcTags) > 0 {
			h.setTags(replicaKeyID, srcTags)
		}
	}

	if len(inputTags) > 0 {
		kv := make(map[string]string, len(inputTags))
		for _, t := range inputTags {
			kv[t.TagKey] = t.TagValue
		}

		h.setTags(replicaKeyID, kv)
	}
}

// applyInputTags converts a []Tag slice to a map and stores it for the given resource ID.
// Returns an error if any tag key/value fails validation.
func (h *Handler) applyInputTags(resourceID string, inputTags []Tag) error {
	if len(inputTags) == 0 {
		return nil
	}

	kv := make(map[string]string, len(inputTags))
	for _, t := range inputTags {
		if err := validateTag(t.TagKey, t.TagValue); err != nil {
			return err
		}

		kv[t.TagKey] = t.TagValue
	}

	h.setTags(resourceID, kv)

	return nil
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
		opCreateGrant,
		"CreateKey",
		opDecrypt,
		"DeleteAlias",
		"DeleteCustomKeyStore",
		"DeleteImportedKeyMaterial",
		opDeriveSharedSecret,
		"DescribeCustomKeyStores",
		opDescribeKey,
		"DisableKey",
		"DisableKeyRotation",
		"DisconnectCustomKeyStore",
		"EnableKey",
		"EnableKeyRotation",
		opEncrypt,
		opGenerateDataKey,
		opGenerateDataKeyPair,
		opGenerateDataKeyPairWithoutPlaintext,
		opGenerateDataKeyWithoutPlaintext,
		opGenerateMac,
		"GenerateRandom",
		"GetKeyLastUsage",
		"GetKeyPolicy",
		"GetKeyRotationStatus",
		"GetParametersForImport",
		opGetPublicKey,
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
		opRetireGrant,
		"RevokeGrant",
		"RotateKeyOnDemand",
		"ScheduleKeyDeletion",
		opSign,
		"TagResource",
		"UntagResource",
		"UpdateAlias",
		"UpdateCustomKeyStore",
		"UpdateKeyDescription",
		"UpdatePrimaryRegion",
		opVerify,
		opVerifyMac,
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

type kmsActionFn func(ctx context.Context, body []byte) (any, error)

// buildDispatchTable merges key lifecycle, crypto, alias/rotation, and tag actions into a single lookup map.
func (h *Handler) buildDispatchTable() map[string]kmsActionFn {
	table := h.buildKeyLifecycleActions()
	maps.Copy(table, h.buildCryptoActions())
	maps.Copy(table, h.buildAliasRotationActions())
	maps.Copy(table, h.buildGrantPolicyActions())
	maps.Copy(table, h.buildTagActions())
	maps.Copy(table, h.buildNewOpsActions())
	table["GetKeyLastUsage"] = unmarshalAction(func(ctx context.Context, i *GetKeyLastUsageInput) (any, error) {
		return h.Backend.GetKeyLastUsage(ctx, i)
	})

	return table
}

// buildKeyLifecycleActions returns dispatch entries for key creation, description, listing and deletion.
func (h *Handler) buildKeyLifecycleActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateKey": h.createKeyAction,
		opDescribeKey: unmarshalAction(
			func(ctx context.Context, i *DescribeKeyInput) (any, error) { return h.Backend.DescribeKey(ctx, i) },
		),
		"ListKeys": unmarshalAction(func(ctx context.Context, i *ListKeysInput) (any, error) {
			return h.Backend.ListKeys(ctx, i)
		}),
		"DisableKey": unmarshalAction(
			func(ctx context.Context, i *DisableKeyInput) (any, error) {
				return struct{}{}, h.Backend.DisableKey(ctx, i)
			},
		),
		"EnableKey": unmarshalAction(
			func(ctx context.Context, i *EnableKeyInput) (any, error) {
				return struct{}{}, h.Backend.EnableKey(ctx, i)
			},
		),
		"ScheduleKeyDeletion": unmarshalAction(
			func(ctx context.Context, i *ScheduleKeyDeletionInput) (any, error) {
				return h.Backend.ScheduleKeyDeletion(ctx, i)
			},
		),
		"CancelKeyDeletion": unmarshalAction(
			func(ctx context.Context, i *CancelKeyDeletionInput) (any, error) {
				return h.Backend.CancelKeyDeletion(ctx, i)
			},
		),
		"ImportKeyMaterial": unmarshalAction(
			func(ctx context.Context, i *ImportKeyMaterialInput) (any, error) {
				return struct{}{}, h.Backend.ImportKeyMaterial(ctx, i)
			},
		),
		"DeleteImportedKeyMaterial": unmarshalAction(
			func(ctx context.Context, i *DeleteImportedKeyMaterialInput) (any, error) {
				return struct{}{}, h.Backend.DeleteImportedKeyMaterial(ctx, i)
			},
		),
	}
}

// unmarshalAction is a generic helper that creates a kmsActionFn from a strongly-typed backend call.
func unmarshalAction[T any](fn func(context.Context, *T) (any, error)) kmsActionFn {
	return func(ctx context.Context, b []byte) (any, error) {
		var input T
		if err := json.Unmarshal(b, &input); err != nil {
			return nil, err
		}

		return fn(ctx, &input)
	}
}

// createKeyAction handles CreateKey dispatch, including tag validation.
func (h *Handler) createKeyAction(ctx context.Context, b []byte) (any, error) {
	var input CreateKeyInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	out, err := h.Backend.CreateKey(ctx, &input)
	if err != nil {
		return nil, err
	}

	if tagErr := h.applyInputTags(out.KeyMetadata.KeyID, input.Tags); tagErr != nil {
		return nil, tagErr
	}

	return out, nil
}

// buildCryptoActions returns dispatch entries for encrypt, decrypt, sign, verify, and data-key operations.
func (h *Handler) buildCryptoActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opEncrypt: func(ctx context.Context, b []byte) (any, error) {
			var input EncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Encrypt(ctx, &input)
		},
		opDecrypt: func(ctx context.Context, b []byte) (any, error) {
			var input DecryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Decrypt(ctx, &input)
		},
		opGenerateDataKey: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKey(ctx, &input)
		},
		opGenerateDataKeyWithoutPlaintext: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyWithoutPlaintext(ctx, &input)
		},
		"ReEncrypt": func(ctx context.Context, b []byte) (any, error) {
			var input ReEncryptInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReEncrypt(ctx, &input)
		},
		opSign: func(ctx context.Context, b []byte) (any, error) {
			var input SignInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Sign(ctx, &input)
		},
		opVerify: func(ctx context.Context, b []byte) (any, error) {
			var input VerifyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.Verify(ctx, &input)
		},
		opGetPublicKey: func(ctx context.Context, b []byte) (any, error) {
			var input GetPublicKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetPublicKey(ctx, &input)
		},
	}
}

// buildAliasRotationActions returns dispatch entries for alias management and key rotation.
func (h *Handler) buildAliasRotationActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"CreateAlias": func(ctx context.Context, b []byte) (any, error) {
			var input CreateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.CreateAlias(ctx, &input)
		},
		"UpdateAlias": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateAlias(ctx, &input)
		},
		"DeleteAlias": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteAlias(ctx, &input)
		},
		"ListAliases": func(ctx context.Context, b []byte) (any, error) {
			var input ListAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListAliases(ctx, &input)
		},
		"EnableKeyRotation": func(ctx context.Context, b []byte) (any, error) {
			var input EnableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.EnableKeyRotation(ctx, &input)
		},
		"DisableKeyRotation": func(ctx context.Context, b []byte) (any, error) {
			var input DisableKeyRotationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisableKeyRotation(ctx, &input)
		},
		"GetKeyRotationStatus": func(ctx context.Context, b []byte) (any, error) {
			var input GetKeyRotationStatusInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyRotationStatus(ctx, &input)
		},
	}
}

// buildGrantPolicyActions returns dispatch entries for grant and key policy operations.
func (h *Handler) buildGrantPolicyActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opCreateGrant: func(ctx context.Context, b []byte) (any, error) {
			var input CreateGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateGrant(ctx, &input)
		},
		"ListGrants": func(ctx context.Context, b []byte) (any, error) {
			var input ListGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListGrants(ctx, &input)
		},
		"RevokeGrant": func(ctx context.Context, b []byte) (any, error) {
			var input RevokeGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RevokeGrant(ctx, &input)
		},
		opRetireGrant: func(ctx context.Context, b []byte) (any, error) {
			var input RetireGrantInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RetireGrant(ctx, &input)
		},
		"ListRetirableGrants": func(ctx context.Context, b []byte) (any, error) {
			var input ListRetirableGrantsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListRetirableGrants(ctx, &input)
		},
		"PutKeyPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input PutKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			// AWS KMS only supports the "default" policy name.
			if input.PolicyName != "" && input.PolicyName != defaultKeyPolicyName {
				return nil, fmt.Errorf(
					"%w: PolicyName must be %q; got %q",
					ErrValidation, defaultKeyPolicyName, input.PolicyName,
				)
			}

			if input.PolicyName == "" {
				input.PolicyName = defaultKeyPolicyName
			}

			return struct{}{}, h.Backend.PutKeyPolicy(ctx, &input)
		},
		"GetKeyPolicy": func(ctx context.Context, b []byte) (any, error) {
			var input GetKeyPolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetKeyPolicy(ctx, &input)
		},
	}
}

// listResourceTags handles the ListResourceTags operation.
func (h *Handler) listResourceTags(b []byte) (any, error) {
	var input listResourceTagsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if _, descErr := h.Backend.DescribeKey(
		context.Background(), &DescribeKeyInput{KeyID: input.KeyID},
	); descErr != nil {
		return nil, descErr
	}

	tagMap := h.getTags(input.KeyID)
	tagList := make([]kmsTagEntry, 0, len(tagMap))

	for k, v := range tagMap {
		tagList = append(tagList, kmsTagEntry{TagKey: k, TagValue: v})
	}

	sort.Slice(tagList, func(i, j int) bool { return tagList[i].TagKey < tagList[j].TagKey })

	return paginateTagList(tagList, input.Marker, input.Limit), nil
}

// paginateTagList returns a paginated listResourceTagsOutput slice from a full tag list.
func paginateTagList(tagList []kmsTagEntry, marker string, limit *int32) *listResourceTagsOutput {
	startIdx := parseMarker(marker)
	pageLimit := defaultKMSTagsLimit

	if limit != nil && *limit > 0 {
		pageLimit = *limit
	}

	if startIdx >= len(tagList) {
		return &listResourceTagsOutput{Tags: []kmsTagEntry{}, Truncated: false}
	}

	end := startIdx + int(pageLimit)

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
	}
}

// tagResource handles the TagResource operation, validating key existence and tag count.
func (h *Handler) tagResource(b []byte) (any, error) {
	var input tagResourceInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	desc, descErr := h.Backend.DescribeKey(context.Background(), &DescribeKeyInput{KeyID: input.KeyID})
	if descErr != nil {
		return nil, descErr
	}

	if desc.KeyMetadata.KeyState == KeyStatePendingDeletion {
		return nil, fmt.Errorf("%w: key %q is pending deletion", ErrKeyInvalidState, desc.KeyMetadata.KeyID)
	}

	newTags := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		if err := validateTag(t.TagKey, t.TagValue); err != nil {
			return nil, err
		}

		newTags[t.TagKey] = t.TagValue
	}

	if err := h.validateTagCount(input.KeyID, newTags); err != nil {
		return nil, err
	}

	h.setTags(input.KeyID, newTags)

	return struct{}{}, nil
}

// validateTag checks that a tag key and value satisfy AWS KMS length and content constraints.
func validateTag(key, value string) error {
	if key == "" {
		return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
	}

	if len(key) > maxTagKeyLength {
		return fmt.Errorf(
			"%w: tag key %q exceeds maximum length of %d characters",
			ErrValidation, key, maxTagKeyLength,
		)
	}

	if strings.HasPrefix(key, "aws:") {
		return fmt.Errorf(
			"%w: tag key %q must not start with the reserved prefix 'aws:'",
			ErrValidation, key,
		)
	}

	if len(value) > maxTagValueLength {
		return fmt.Errorf(
			"%w: tag value for key %q exceeds maximum length of %d characters",
			ErrValidation, key, maxTagValueLength,
		)
	}

	return nil
}

// validateTagCount returns an error if adding newTags to keyID would exceed the max tag limit.
func (h *Handler) validateTagCount(keyID string, newTags map[string]string) error {
	existing := h.getTags(keyID)

	netNew := 0
	for k := range newTags {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			netNew++
		}
	}

	if len(existing)+netNew > maxTagsPerKey {
		return fmt.Errorf(
			"%w: tagging key %q would exceed the maximum of %d tags",
			ErrValidation, keyID, maxTagsPerKey,
		)
	}

	return nil
}

// untagResource handles the UntagResource operation.
func (h *Handler) untagResource(b []byte) (any, error) {
	var input untagResourceInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	desc, descErr := h.Backend.DescribeKey(context.Background(), &DescribeKeyInput{KeyID: input.KeyID})
	if descErr != nil {
		return nil, descErr
	}

	if desc.KeyMetadata.KeyState == KeyStatePendingDeletion {
		return nil, fmt.Errorf("%w: key %q is pending deletion", ErrKeyInvalidState, desc.KeyMetadata.KeyID)
	}

	h.removeTags(input.KeyID, input.TagKeys)

	return struct{}{}, nil
}

// buildTagActions returns dispatch entries for KMS resource tag operations.
func (h *Handler) buildTagActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"ListResourceTags": func(_ context.Context, b []byte) (any, error) {
			return h.listResourceTags(b)
		},
		"TagResource": func(_ context.Context, b []byte) (any, error) {
			return h.tagResource(b)
		},
		"UntagResource": func(_ context.Context, b []byte) (any, error) {
			return h.untagResource(b)
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
		"CreateCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input CreateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateCustomKeyStore(ctx, &input)
		},
		"DeleteCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input DeleteCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DeleteCustomKeyStore(ctx, &input)
		},
		"DescribeCustomKeyStores": func(ctx context.Context, b []byte) (any, error) {
			var input DescribeCustomKeyStoresInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeCustomKeyStores(ctx, &input)
		},
		"ConnectCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input ConnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.ConnectCustomKeyStore(ctx, &input)
		},
		"DisconnectCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input DisconnectCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.DisconnectCustomKeyStore(ctx, &input)
		},
		opDeriveSharedSecret: func(ctx context.Context, b []byte) (any, error) {
			var input DeriveSharedSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeriveSharedSecret(ctx, &input)
		},
	}
}

// buildGenerateAndMacActions returns dispatch entries for data key pair, MAC, and random operations.
func (h *Handler) buildGenerateAndMacActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		opGenerateDataKeyPair: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyPairInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPair(ctx, &input)
		},
		opGenerateDataKeyPairWithoutPlaintext: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateDataKeyPairWithoutPlaintextInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateDataKeyPairWithoutPlaintext(ctx, &input)
		},
		opGenerateMac: func(ctx context.Context, b []byte) (any, error) {
			var input GenerateMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateMac(ctx, &input)
		},
		"GenerateRandom": func(ctx context.Context, b []byte) (any, error) {
			var input GenerateRandomInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GenerateRandom(ctx, &input)
		},
		opVerifyMac: func(ctx context.Context, b []byte) (any, error) {
			var input VerifyMacInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.VerifyMac(ctx, &input)
		},
	}
}

func (h *Handler) buildReplicationAndMaintenanceActions() map[string]kmsActionFn {
	return map[string]kmsActionFn{
		"GetParametersForImport": func(ctx context.Context, b []byte) (any, error) {
			var input GetParametersForImportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParametersForImport(ctx, &input)
		},
		"ListKeyPolicies": func(ctx context.Context, b []byte) (any, error) {
			var input ListKeyPoliciesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyPolicies(ctx, &input)
		},
		"ListKeyRotations": func(ctx context.Context, b []byte) (any, error) {
			var input ListKeyRotationsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListKeyRotations(ctx, &input)
		},
		"ReplicateKey": func(ctx context.Context, b []byte) (any, error) {
			var input ReplicateKeyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			// Capture source key ID for tag copying before we replicate.
			var sourceKeyID string
			if desc, descErr := h.Backend.DescribeKey(ctx, &DescribeKeyInput{KeyID: input.KeyID}); descErr == nil {
				sourceKeyID = desc.KeyMetadata.KeyID
			}

			out, err := h.Backend.ReplicateKey(ctx, &input)
			if err != nil {
				return nil, err
			}

			h.copyTagsToReplica(sourceKeyID, out.ReplicaKeyMetadata.KeyID, input.Tags)

			return out, nil
		},
		"RotateKeyOnDemand": func(ctx context.Context, b []byte) (any, error) {
			var input RotateKeyOnDemandInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RotateKeyOnDemand(ctx, &input)
		},
		"UpdateCustomKeyStore": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateCustomKeyStoreInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateCustomKeyStore(ctx, &input)
		},
		"UpdateKeyDescription": func(ctx context.Context, b []byte) (any, error) {
			var input UpdateKeyDescriptionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdateKeyDescription(ctx, &input)
		},
		"UpdatePrimaryRegion": func(ctx context.Context, b []byte) (any, error) {
			var input UpdatePrimaryRegionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UpdatePrimaryRegion(ctx, &input)
		},
	}
}

// dispatch routes the KMS operation to the appropriate backend method.
func (h *Handler) dispatch(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)
	ctx = context.WithValue(ctx, regionContextKey{}, region)

	fn, ok := h.actions[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a structured error response for a KMS operation failure.
const (
	awsErrNotFound          = "NotFoundException"
	awsErrValidation        = "ValidationException"
	awsErrInvalidCiphertext = "InvalidCiphertextException"
)

// kmsErrorEntry maps a sentinel error to its AWS error type string.
type kmsErrorEntry struct {
	sentinel error
	awsType  string
}

// kmsErrorTable returns the ordered error-to-AWS-type mapping for KMS error classification.
// Defined as a function to satisfy gochecknoglobals.
func kmsErrorTable() []kmsErrorEntry {
	return []kmsErrorEntry{
		{ErrKeyNotFound, awsErrNotFound},
		{ErrAliasNotFound, awsErrNotFound},
		{ErrGrantNotFound, awsErrNotFound},
		{ErrCustomKeyStoreNotFound, "CustomKeyStoreNotFoundException"},
		{ErrKeyDisabled, "DisabledException"},
		{ErrKeyInvalidState, "KMSInvalidStateException"},
		{ErrInvalidKeyUsage, "InvalidKeyUsageException"},
		{ErrAliasAlreadyExists, "AlreadyExistsException"},
		{ErrCustomKeyStoreAlreadyExists, "CustomKeyStoreNameInUseException"},
		{ErrIncorrectKey, "IncorrectKeyException"},
		{ErrInvalidCiphertext, awsErrInvalidCiphertext},
		{ErrCiphertextTooShort, awsErrInvalidCiphertext},
		{ErrInvalidSignature, "KMSInvalidSignatureException"},
		{ErrUnsupportedOrigin, "UnsupportedOperationException"},
		{ErrValidation, awsErrValidation},
		{ErrInvalidDataKeySize, awsErrValidation},
		{ErrInvalidGrantToken, "InvalidGrantTokenException"},
		{ErrLimitExceeded, "LimitExceededException"},
		{ErrInvalidAlgorithm, "InvalidAlgorithmException"},
		{ErrUnknownOperation, "UnknownOperationException"},
	}
}

func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	errorType, statusCode := classifyKMSError(reqErr)

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

// classifyKMSError returns the AWS error type string and HTTP status code for reqErr.
func classifyKMSError(reqErr error) (string, int) {
	for _, m := range kmsErrorTable() {
		if errors.Is(reqErr, m.sentinel) {
			return m.awsType, http.StatusBadRequest
		}
	}

	return "InternalServiceError", http.StatusInternalServerError
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
	out, err := h.Backend.ListKeys(context.Background(), &ListKeysInput{})
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
	out, err := h.Backend.ListKeys(context.Background(), &ListKeysInput{})
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
	out, err := h.Backend.ListKeys(context.Background(), &ListKeysInput{})
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
