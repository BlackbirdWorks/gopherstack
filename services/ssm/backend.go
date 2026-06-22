package ssm

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	documentFormatJSON = "JSON"
	statusActive       = "Active"
)

var (
	ErrParameterNotFound                  = errors.New("ParameterNotFound")
	ErrParameterVersionNotFound           = errors.New("ParameterVersionNotFound")
	ErrParameterAlreadyExists             = errors.New("ParameterAlreadyExists")
	ErrInvalidKeyID                       = errors.New("InvalidKeyId")
	ErrCiphertextTooShort                 = errors.New("ciphertext too short")
	ErrValidationException                = errors.New("ValidationException")
	ErrDocumentAlreadyExists              = errors.New("DocumentAlreadyExists")
	ErrDocumentNotFound                   = errors.New("DocumentNotFound")
	ErrInvalidDocumentVersion             = errors.New("InvalidDocumentVersion")
	ErrCommandNotFound                    = errors.New("CommandNotFound")
	ErrActivationNotFound                 = errors.New("ActivationNotFound")
	ErrAssociationNotFound                = errors.New("AssociationDoesNotExist")
	ErrMaintenanceWindowNotFound          = errors.New("DoesNotExistException")
	ErrMaintenanceWindowExecutionNotFound = errors.New("DoesNotExistException")
	ErrOpsItemNotFound                    = errors.New("OpsItemNotFoundException")
	ErrOpsMetadataNotFound                = errors.New("OpsMetadataNotFoundException")
	ErrPatchBaselineNotFound              = errors.New("DoesNotExistException")
	ErrOpsMetadataAlreadyExists           = errors.New("OpsMetadataAlreadyExistsException")
)

const (
	StringType        = "String"
	StringListType    = "StringList"
	SecureStringType  = "SecureString"
	mockKMSKeyStr     = "gopherstack-mock-kms-key-32byte!"
	maxHistoryResults = 50
	// defaultCommandExpirySecs is the default TTL for SSM commands in seconds (1 hour).
	// AWS SSM commands expire after 1 hour by default.
	defaultCommandExpirySecs = 3600
	// maxHistoryCap is the maximum number of history entries retained per parameter.
	// Older entries beyond this cap are evicted to prevent unbounded growth.
	maxHistoryCap = 100
	// maxDocumentVersionCap is the maximum number of versions retained per document.
	// Matches the AWS-side limit and prevents unbounded growth via repeated UpdateDocument.
	maxDocumentVersionCap = 1000
	// resourceTypeParameter is the SSM resource type for parameters.
	resourceTypeParameter = "Parameter"
)

// validParamNameRegex matches only alphanumeric, ., -, _, and / characters.
var validParamNameRegex = regexp.MustCompile(`^[a-zA-Z0-9._\-/]+$`)

const maxParamNameLength = 2048

// validateParameterName returns a ValidationException error when the name is invalid.
func validateParameterName(name string) error {
	if len(name) > maxParamNameLength {
		return fmt.Errorf(
			"%w: parameter name exceeds maximum length of %d",
			ErrValidationException,
			maxParamNameLength,
		)
	}

	if strings.Contains(name, "//") {
		return fmt.Errorf(
			"%w: parameter name must not contain double slashes",
			ErrValidationException,
		)
	}

	lower := strings.ToLower(strings.TrimPrefix(name, "/"))
	reservedPrefixes := []string{"ssm", "aws", "amazon"}
	for _, prefix := range reservedPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return fmt.Errorf(
				"%w: parameter name must not start with reserved namespace %q",
				ErrValidationException,
				prefix,
			)
		}
	}

	if !validParamNameRegex.MatchString(name) {
		return fmt.Errorf("%w: parameter name contains invalid characters", ErrValidationException)
	}

	return nil
}

// mockGCM is a package-level GCM cipher instance built once from the mock KMS key.
// The AES block and GCM AEAD are stateless after construction, so sharing is safe.
//
//nolint:gochecknoglobals // intentional package-level singleton for GCM pool optimisation
var mockGCM cipher.AEAD

//nolint:gochecknoinits // init is the correct place to initialise the GCM singleton
func init() {
	block, err := aes.NewCipher([]byte(mockKMSKeyStr))
	if err != nil {
		panic("ssm: failed to create AES cipher: " + err.Error())
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic("ssm: failed to create GCM: " + err.Error())
	}

	mockGCM = gcm
}

// encryptValue encrypts a value using AES-256 (mock KMS encryption).
func encryptValue(plaintext string) (string, error) {
	nonce := make([]byte, mockGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := mockGCM.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a value encrypted with encryptValue.
func decryptValue(ciphertext string) (string, error) {
	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	nonceSize := mockGCM.NonceSize()
	if len(ciphertextBytes) < nonceSize {
		return "", ErrCiphertextTooShort
	}

	nonce, ciphertextOnly := ciphertextBytes[:nonceSize], ciphertextBytes[nonceSize:]

	plaintext, err := mockGCM.Open(nil, nonce, ciphertextOnly, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// KMSEncryptor provides symmetric encrypt/decrypt for SecureString parameters.
// Implemented by an adapter wrapping the KMS backend.
type KMSEncryptor interface {
	// EncryptSSM encrypts plaintext using the given KMS key and returns ciphertext bytes.
	EncryptSSM(keyID string, plaintext []byte) ([]byte, error)
	// DecryptSSM decrypts ciphertext and returns plaintext bytes.
	DecryptSSM(ciphertext []byte) ([]byte, error)
}

// InMemoryBackend implements StorageBackend using a concurrency-safe map.
type InMemoryBackend struct {
	kms                      KMSEncryptor
	activations              map[string]map[string]Activation
	maintenanceWindows       map[string]map[string]MaintenanceWindow
	maintenanceWindowTargets map[string]map[string]MaintenanceWindowTarget
	maintenanceWindowTasks   map[string]map[string]MaintenanceWindowTask
	sessions                 map[string]map[string]Session
	patchGroupToBaseline     map[string]map[string]string
	tags                     map[string]map[string]*tags.Tags
	associations             map[string]map[string]Association
	documentVersions         map[string]map[string][]DocumentVersion
	documentPermissions      map[string]map[string][]string
	commands                 map[string]map[string]Command
	commandInvocations       map[string]map[string][]CommandInvocation
	history                  map[string]map[string][]ParameterHistory
	parameters               map[string]map[string]Parameter
	// paramNamesSorted holds per-region parameter names in sorted order for
	// binary-search prefix lookups in GetParametersByPath (O(log n + k) vs O(n)).
	paramNamesSorted           map[string][]string
	documents                  map[string]map[string]Document
	opsItems                   map[string]map[string]OpsItem
	opsItemRelatedItems        map[string]map[string][]OpsItemRelatedItem
	opsMetadata                map[string]map[string]OpsMetadata
	patchBaselines             map[string]map[string]PatchBaseline
	inventory                  map[string]map[string][]InventoryItem  // key: instanceID
	compliance                 map[string]map[string][]ComplianceItem // key: resourceID
	resourceDataSyncs          map[string]map[string]*ResourceDataSync
	parameterLabels            map[string]map[string]map[int64][]string    // paramName → version → labels (0 = latest)
	automationExecutions       map[string]map[string]*AutomationExecution  // executionID → exec
	serviceSettings            map[string]map[string]*ServiceSetting       // settingID → setting
	resourcePolicies           map[string]map[string][]*ResourcePolicy     // resourceARN → policies
	executionPreviews          map[string]map[string]*ExecutionPreview     // previewID → preview
	instancePatchStates        map[string]map[string]*InstancePatchState   // region → instanceID → state
	instancePatches            map[string]map[string][]PatchComplianceData // region → instanceID → patches
	instanceProperties         map[string]map[string]*InstanceProperty     // region → instanceID → properties
	availablePatches           map[string][]Patch                          // region → patches
	mu                         *lockmetrics.RWMutex
	miscResourceTags           map[string]map[string]map[string]string
	resourceIDToOpsMetadataArn map[string]map[string]string
	opsItemEvents              map[string][]OpsItemEventSummary
	commandExpirySecs          float64
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		parameters:                 make(map[string]map[string]Parameter),
		paramNamesSorted:           make(map[string][]string),
		history:                    make(map[string]map[string][]ParameterHistory),
		tags:                       make(map[string]map[string]*tags.Tags),
		documents:                  make(map[string]map[string]Document),
		documentVersions:           make(map[string]map[string][]DocumentVersion),
		documentPermissions:        make(map[string]map[string][]string),
		commands:                   make(map[string]map[string]Command),
		commandInvocations:         make(map[string]map[string][]CommandInvocation),
		activations:                make(map[string]map[string]Activation),
		associations:               make(map[string]map[string]Association),
		maintenanceWindows:         make(map[string]map[string]MaintenanceWindow),
		maintenanceWindowTargets:   make(map[string]map[string]MaintenanceWindowTarget),
		maintenanceWindowTasks:     make(map[string]map[string]MaintenanceWindowTask),
		sessions:                   make(map[string]map[string]Session),
		patchGroupToBaseline:       make(map[string]map[string]string),
		opsItems:                   make(map[string]map[string]OpsItem),
		opsItemRelatedItems:        make(map[string]map[string][]OpsItemRelatedItem),
		opsMetadata:                make(map[string]map[string]OpsMetadata),
		patchBaselines:             make(map[string]map[string]PatchBaseline),
		inventory:                  make(map[string]map[string][]InventoryItem),
		compliance:                 make(map[string]map[string][]ComplianceItem),
		resourceDataSyncs:          make(map[string]map[string]*ResourceDataSync),
		parameterLabels:            make(map[string]map[string]map[int64][]string),
		automationExecutions:       make(map[string]map[string]*AutomationExecution),
		serviceSettings:            make(map[string]map[string]*ServiceSetting),
		resourcePolicies:           make(map[string]map[string][]*ResourcePolicy),
		executionPreviews:          make(map[string]map[string]*ExecutionPreview),
		instancePatchStates:        make(map[string]map[string]*InstancePatchState),
		instancePatches:            make(map[string]map[string][]PatchComplianceData),
		instanceProperties:         make(map[string]map[string]*InstanceProperty),
		availablePatches:           make(map[string][]Patch),
		commandExpirySecs:          defaultCommandExpirySecs,
		mu:                         lockmetrics.New("ssm"),
		resourceIDToOpsMetadataArn: make(map[string]map[string]string),
		miscResourceTags:           make(map[string]map[string]map[string]string),
		opsItemEvents:              make(map[string][]OpsItemEventSummary),
	}

	b.registerDefaultDocuments(defaultRegion)

	return b
}

// WithKMS attaches a KMSEncryptor so SecureString parameters whose KeyID is
// set are encrypted/decrypted using the real KMS backend instead of the
// built-in mock key.
func (b *InMemoryBackend) WithKMS(e KMSEncryptor) *InMemoryBackend {
	b.kms = e

	return b
}

// WithCommandTTL sets the TTL used for the ExpiresAfter field on new commands.
// A zero or negative value falls back to the default (3600 seconds / 1 hour).
func (b *InMemoryBackend) WithCommandTTL(d time.Duration) *InMemoryBackend {
	if d > 0 {
		b.commandExpirySecs = d.Seconds()
	}

	return b
}

func getRegion(ctx context.Context) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok {
		return region
	}

	return defaultRegion
}

func (b *InMemoryBackend) parametersStore(region string) map[string]Parameter {
	return b.parameters[region]
}

func (b *InMemoryBackend) historyStore(region string) map[string][]ParameterHistory {
	return b.history[region]
}

func (b *InMemoryBackend) tagsStore(region string) map[string]*tags.Tags {
	return b.tags[region]
}

func (b *InMemoryBackend) documentsStore(region string) map[string]Document {
	return b.documents[region]
}

func (b *InMemoryBackend) documentVersionsStore(region string) map[string][]DocumentVersion {
	return b.documentVersions[region]
}

func (b *InMemoryBackend) documentPermissionsStore(region string) map[string][]string {
	return b.documentPermissions[region]
}

func (b *InMemoryBackend) commandsStore(region string) map[string]Command {
	return b.commands[region]
}

// expireCommandsLocked removes commands (and their invocations) in the region
// whose ExpiresAfter timestamp has passed. It mirrors the background janitor's
// sweep so commands are pruned on the write path even when the janitor is
// disabled or runs at a long interval. The backend mutex must be held.
func (b *InMemoryBackend) expireCommandsLocked(region string, now float64) {
	commands := b.commands[region]
	if commands == nil {
		return
	}

	for id, cmd := range commands {
		if cmd.ExpiresAfter > 0 && cmd.ExpiresAfter < now {
			delete(commands, id)
			delete(b.commandInvocations[region], id)
		}
	}
}

func (b *InMemoryBackend) commandInvocationsStore(region string) map[string][]CommandInvocation {
	return b.commandInvocations[region]
}

func (b *InMemoryBackend) activationsStore(region string) map[string]Activation {
	return b.activations[region]
}

func (b *InMemoryBackend) associationsStore(region string) map[string]Association {
	return b.associations[region]
}

func (b *InMemoryBackend) maintenanceWindowsStore(region string) map[string]MaintenanceWindow {
	return b.maintenanceWindows[region]
}

func (b *InMemoryBackend) maintenanceWindowTargetsStore(
	region string,
) map[string]MaintenanceWindowTarget {
	return b.maintenanceWindowTargets[region]
}

func (b *InMemoryBackend) maintenanceWindowTasksStore(
	region string,
) map[string]MaintenanceWindowTask {
	return b.maintenanceWindowTasks[region]
}

func (b *InMemoryBackend) sessionsStore(region string) map[string]Session {
	return b.sessions[region]
}

func (b *InMemoryBackend) patchGroupToBaselineStore(region string) map[string]string {
	return b.patchGroupToBaseline[region]
}

func (b *InMemoryBackend) opsItemsStore(region string) map[string]OpsItem {
	return b.opsItems[region]
}

func (b *InMemoryBackend) opsItemRelatedItemsStore(region string) map[string][]OpsItemRelatedItem {
	return b.opsItemRelatedItems[region]
}

func (b *InMemoryBackend) opsMetadataStore(region string) map[string]OpsMetadata {
	return b.opsMetadata[region]
}

func (b *InMemoryBackend) patchBaselinesStore(region string) map[string]PatchBaseline {
	return b.patchBaselines[region]
}

func (b *InMemoryBackend) inventoryStore(region string) map[string][]InventoryItem {
	return b.inventory[region]
}

func (b *InMemoryBackend) complianceStore(region string) map[string][]ComplianceItem {
	return b.compliance[region]
}

func (b *InMemoryBackend) resourceDataSyncsStore(region string) map[string]*ResourceDataSync {
	return b.resourceDataSyncs[region]
}

func (b *InMemoryBackend) parameterLabelsStore(region string) map[string]map[int64][]string {
	return b.parameterLabels[region]
}

func (b *InMemoryBackend) automationExecutionsStore(region string) map[string]*AutomationExecution {
	return b.automationExecutions[region]
}

func (b *InMemoryBackend) serviceSettingsStore(region string) map[string]*ServiceSetting {
	return b.serviceSettings[region]
}

func (b *InMemoryBackend) resourcePoliciesStore(region string) map[string][]*ResourcePolicy {
	return b.resourcePolicies[region]
}

func (b *InMemoryBackend) executionPreviewsStore(region string) map[string]*ExecutionPreview {
	return b.executionPreviews[region]
}

func (b *InMemoryBackend) miscResourceTagsStore(region string) map[string]map[string]string {
	return b.miscResourceTags[region]
}

func (b *InMemoryBackend) resourceIDToOpsMetadataArnStore(region string) map[string]string {
	return b.resourceIDToOpsMetadataArn[region]
}

func (b *InMemoryBackend) opsItemEventsStore(region string) []OpsItemEventSummary {
	return b.opsItemEvents[region]
}

func (b *InMemoryBackend) instancePatchStatesStore(region string) map[string]*InstancePatchState {
	return b.instancePatchStates[region]
}

func (b *InMemoryBackend) instancePatchesStore(region string) map[string][]PatchComplianceData {
	return b.instancePatches[region]
}

func (b *InMemoryBackend) instancePropertiesStore(region string) map[string]*InstanceProperty {
	return b.instanceProperties[region]
}

// encryptSSMValue encrypts a SecureString value using KMS (when keyID is set
// and a KMSEncryptor is wired) or the built-in mock GCM cipher.
// Returns base64-encoded ciphertext for storage.
func (b *InMemoryBackend) encryptSSMValue(keyID, plaintext string) (string, error) {
	if keyID != "" && b.kms != nil {
		ct, err := b.kms.EncryptSSM(keyID, []byte(plaintext))
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidKeyID, err)
		}

		return base64.StdEncoding.EncodeToString(ct), nil
	}

	return encryptValue(plaintext)
}

// decryptSSMValue decrypts a stored SecureString value.  When the value was
// encrypted with KMS (detected by attempting KMS decrypt when a backend is
// available), the KMS path is used; otherwise falls back to the mock cipher.
func (b *InMemoryBackend) decryptSSMValue(keyID, ciphertext string) (string, error) {
	if keyID != "" && b.kms != nil {
		ct, err := base64.StdEncoding.DecodeString(ciphertext)
		if err != nil {
			return "", err
		}
		pt, err := b.kms.DecryptSSM(ct)
		if err != nil {
			return "", err
		}

		return string(pt), nil
	}

	return decryptValue(ciphertext)
}

// PutParameter creates or updates a parameter.
const (
	tierStandard           = "Standard"
	tierAdvanced           = "Advanced"
	tierIntelligentTiering = "Intelligent-Tiering"
	// maxStandardValueBytes is the Standard-tier limit (4 KiB).
	maxStandardValueBytes = 4096
	// maxAdvancedValueBytes is the Advanced-tier limit (8 KiB).
	maxAdvancedValueBytes = 8192
)

// isValidParameterType returns true when t is one of the three supported SSM
// parameter types. Real AWS rejects missing or unrecognised types with
// ValidationException.
func isValidParameterType(t string) bool {
	switch t {
	case StringType, StringListType, SecureStringType:
		return true
	}

	return false
}

// isValidDataType returns true when dt is a supported SSM DataType value.
func isValidDataType(dt string) bool {
	switch dt {
	case "text", "aws:ec2:image", "aws:ssm:integration-default-configuration-directory":
		return true
	}

	return false
}

// validateAllowedPattern compiles the pattern and checks the value against it.
func validateAllowedPattern(pattern, value string) error {
	if pattern == "" {
		return nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("%w: invalid AllowedPattern: %w", ErrValidationException, err)
	}

	if !re.MatchString(value) {
		return fmt.Errorf(
			"%w: parameter value does not match AllowedPattern %q",
			ErrValidationException, pattern,
		)
	}

	return nil
}

// resolveTier canonicalises the tier string and enforces per-tier value size limits.
// Returns the resolved tier name or an error.
func resolveTier(tier, value string) (string, error) {
	if tier == "" {
		tier = tierStandard
	}

	switch tier {
	case tierStandard, tierIntelligentTiering:
		if len(value) > maxStandardValueBytes {
			return "", fmt.Errorf(
				"%w: parameter value exceeds %d bytes for %s tier",
				ErrValidationException, maxStandardValueBytes, tier,
			)
		}
	case tierAdvanced:
		if len(value) > maxAdvancedValueBytes {
			return "", fmt.Errorf(
				"%w: parameter value exceeds %d bytes for Advanced tier",
				ErrValidationException, maxAdvancedValueBytes,
			)
		}
	default:
		return "", fmt.Errorf("%w: invalid Tier %q", ErrValidationException, tier)
	}

	return tier, nil
}

// parameterARN builds the ARN for a parameter. AWS omits the leading slash
// between "parameter" and the name (so /a/b → parameter/a/b, and a relative
// name "foo" → parameter/foo).
func parameterARN(region, account, name string) string {
	trimmed := strings.TrimPrefix(name, "/")

	return arn.Build("ssm", region, account, fmt.Sprintf("parameter/%s", trimmed))
}

// splitParameterSelector splits a parameter name into its base name and the
// selector suffix (version or label). A selector is introduced by the last ":"
// in the name. AWS parameter names may legitimately contain "/" but never ":",
// so any ":" delimits a selector. Returns (baseName, selector) where selector
// is the part after the colon ("" when no selector is present).
func splitParameterSelector(name string) (string, string) {
	idx := strings.LastIndex(name, ":")
	if idx < 0 {
		return name, ""
	}

	return name[:idx], name[idx+1:]
}

// resolveParameterSelector returns the Parameter for the given base name and
// selector. The selector may be empty (latest version), a numeric version, or a
// label. It mirrors AWS error semantics:
//   - unknown parameter             → ParameterNotFound
//   - numeric selector, no version  → ParameterVersionNotFound
//   - label selector, no match      → ParameterNotFound
//
// Caller must hold at least the read lock.
func (b *InMemoryBackend) resolveParameterSelector(
	region, baseName, selector string,
) (Parameter, error) {
	current, exists := b.parametersStore(region)[baseName]
	if !exists {
		return Parameter{}, ErrParameterNotFound
	}

	if selector == "" {
		return current, nil
	}

	history := b.historyStore(region)[baseName]

	// Numeric selector → specific version.
	if version, err := strconv.ParseInt(selector, 10, 64); err == nil {
		return b.parameterAtVersion(current, history, version)
	}

	// Label selector → resolve label to a version via the labels store.
	version, ok := b.versionForLabel(region, baseName, selector)
	if !ok {
		return Parameter{}, ErrParameterNotFound
	}

	param, err := b.parameterAtVersion(current, history, version)
	if err != nil {
		// A label pointing at a missing version behaves like a missing parameter.
		return Parameter{}, ErrParameterNotFound
	}

	return param, nil
}

// parameterAtVersion materializes a Parameter for a specific version from the
// history list, falling back to the current record when the requested version
// is the current one. Returns ParameterVersionNotFound when no such version
// exists.
func (b *InMemoryBackend) parameterAtVersion(
	current Parameter, history []ParameterHistory, version int64,
) (Parameter, error) {
	if version == current.Version {
		return current, nil
	}

	for _, h := range history {
		if h.Version != version {
			continue
		}

		return Parameter{
			Name:             h.Name,
			Type:             h.Type,
			Value:            h.Value,
			Description:      h.Description,
			KeyID:            h.KeyID,
			Tier:             h.Tier,
			AllowedPattern:   h.AllowedPattern,
			DataType:         h.DataType,
			Version:          h.Version,
			LastModifiedDate: h.LastModifiedDate,
		}, nil
	}

	return Parameter{}, ErrParameterVersionNotFound
}

// versionForLabel returns the version a label currently points at. Caller must
// hold at least the read lock.
func (b *InMemoryBackend) versionForLabel(region, name, label string) (int64, bool) {
	versionLabels, ok := b.parameterLabelsStore(region)[name]
	if !ok {
		return 0, false
	}

	for version, labels := range versionLabels {
		if slices.Contains(labels, label) {
			return version, true
		}
	}

	return 0, false
}

type putParameterValidated struct {
	dataType string
	tier     string
}

// validatePutParameterInput validates the pre-lock fields of a PutParameter
// request and returns the resolved dataType and tier.
func validatePutParameterInput(input *PutParameterInput) (putParameterValidated, error) {
	if err := validateParameterName(input.Name); err != nil {
		return putParameterValidated{}, err
	}

	if !isValidParameterType(input.Type) {
		return putParameterValidated{}, fmt.Errorf(
			"%w: invalid Type %q, must be String, StringList, or SecureString",
			ErrValidationException, input.Type,
		)
	}

	dataType := input.DataType
	if dataType == "" {
		dataType = "text"
	}

	if !isValidDataType(dataType) {
		return putParameterValidated{}, fmt.Errorf(
			"%w: invalid DataType %q", ErrValidationException, dataType,
		)
	}

	if err := validateAllowedPattern(input.AllowedPattern, input.Value); err != nil {
		return putParameterValidated{}, err
	}

	tier, err := resolveTier(input.Tier, input.Value)
	if err != nil {
		return putParameterValidated{}, err
	}

	return putParameterValidated{dataType: dataType, tier: tier}, nil
}

func (b *InMemoryBackend) PutParameter(
	ctx context.Context,
	input *PutParameterInput,
) (*PutParameterOutput, error) {
	validated, err := validatePutParameterInput(input)
	if err != nil {
		return nil, err
	}

	dataType := validated.dataType
	tier := validated.tier
	region := getRegion(ctx)

	b.mu.Lock("PutParameter")
	defer b.mu.Unlock()

	if b.parameters[region] == nil {
		b.parameters[region] = make(map[string]Parameter)
	}
	params := b.parametersStore(region)
	existing, exists := params[input.Name]
	if exists && !input.Overwrite {
		return nil, ErrParameterAlreadyExists
	}

	version := int64(1)
	if exists {
		version = existing.Version + 1
	}

	// Encrypt if SecureString type; use KMS when a KeyID is specified.
	value := input.Value
	if input.Type == SecureStringType {
		var encErr error
		value, encErr = b.encryptSSMValue(input.KeyID, input.Value)
		if encErr != nil {
			return nil, encErr
		}
	}

	param := Parameter{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Description:      input.Description,
		Version:          version,
		LastModifiedDate: UnixTimeFloat(time.Now()),
		KeyID:            input.KeyID,
		Tier:             tier,
		AllowedPattern:   input.AllowedPattern,
		DataType:         dataType,
		Policies:         input.Policies,
	}

	params[input.Name] = param
	if !exists {
		b.insertSortedParamName(region, input.Name)
	}

	// Store in history (store encrypted value for SecureString)
	paramHistory := ParameterHistory{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Version:          version,
		LastModifiedDate: param.LastModifiedDate,
		Labels:           []string{},
		KeyID:            input.KeyID,
		Tier:             tier,
		AllowedPattern:   input.AllowedPattern,
		DataType:         dataType,
		Description:      input.Description,
	}
	if b.history[region] == nil {
		b.history[region] = make(map[string][]ParameterHistory)
	}
	history := b.historyStore(region)
	history[input.Name] = append(history[input.Name], paramHistory)

	// Cap history to the most recent maxHistoryCap entries to prevent unbounded growth.
	if len(history[input.Name]) > maxHistoryCap {
		history[input.Name] = history[input.Name][len(history[input.Name])-maxHistoryCap:]
	}

	return &PutParameterOutput{Version: version, Tier: tier}, nil
}

// GetParameter retrieves a single parameter. The name may carry a version or
// label selector suffix (e.g. "/a/b:3" or "/a/b:prod"), in which case the
// matching version is returned and echoed back via Parameter.Selector. The
// response always includes the parameter ARN.
func (b *InMemoryBackend) GetParameter(
	ctx context.Context,
	input *GetParameterInput,
) (*GetParameterOutput, error) {
	region := getRegion(ctx)
	account := awsmeta.Account(ctx)

	baseName, selector := splitParameterSelector(input.Name)

	b.mu.RLock("GetParameter")
	defer b.mu.RUnlock()

	param, err := b.resolveParameterSelector(region, baseName, selector)
	if err != nil {
		return nil, err
	}

	// Decrypt SecureString if WithDecryption is true; propagate errors.
	if input.WithDecryption && param.Type == SecureStringType {
		decrypted, derr := b.decryptSSMValue(param.KeyID, param.Value)
		if derr != nil {
			return nil, fmt.Errorf("%w: %w", ErrValidationException, derr)
		}

		param.Value = decrypted
	}

	param.ARN = parameterARN(region, account, baseName)
	if selector != "" {
		param.Selector = ":" + selector
	}

	return &GetParameterOutput{Parameter: param}, nil
}

// GetParameters retrieves multiple parameters. Missing names are returned as InvalidParameters.
func (b *InMemoryBackend) GetParameters(
	ctx context.Context,
	input *GetParametersInput,
) (*GetParametersOutput, error) {
	region := getRegion(ctx)
	account := awsmeta.Account(ctx)

	b.mu.RLock("GetParameters")
	defer b.mu.RUnlock()

	output := &GetParametersOutput{
		Parameters:        make([]Parameter, 0, len(input.Names)),
		InvalidParameters: make([]string, 0, len(input.Names)),
	}

	for _, name := range input.Names {
		baseName, selector := splitParameterSelector(name)

		param, err := b.resolveParameterSelector(region, baseName, selector)
		if err != nil {
			// Unknown name, missing version, or unresolvable label all become
			// invalid parameters in GetParameters (AWS does not fail the call).
			output.InvalidParameters = append(output.InvalidParameters, name)

			continue
		}

		// Decrypt SecureString if WithDecryption is true
		if input.WithDecryption && param.Type == SecureStringType {
			decrypted, derr := b.decryptSSMValue(param.KeyID, param.Value)
			if derr != nil {
				// If decryption fails, add to invalid parameters
				output.InvalidParameters = append(output.InvalidParameters, name)

				continue
			}
			param.Value = decrypted
		}

		param.ARN = parameterARN(region, account, baseName)
		if selector != "" {
			param.Selector = ":" + selector
		}
		output.Parameters = append(output.Parameters, param)
	}

	return output, nil
}

// DeleteParameter deletes a single parameter.
func (b *InMemoryBackend) DeleteParameter(
	ctx context.Context,
	input *DeleteParameterInput,
) (*DeleteParameterOutput, error) {
	region := getRegion(ctx)

	b.mu.Lock("DeleteParameter")
	defer b.mu.Unlock()

	params := b.parametersStore(region)
	if _, exists := params[input.Name]; !exists {
		return nil, ErrParameterNotFound
	}

	delete(params, input.Name)
	delete(b.historyStore(region), input.Name)
	b.removeSortedParamName(region, input.Name)

	tags := b.tagsStore(region)
	if t, ok := tags[input.Name]; ok {
		t.Close()
		delete(tags, input.Name)
	}

	return &DeleteParameterOutput{}, nil
}

// DeleteParameters deletes multiple parameters.
func (b *InMemoryBackend) DeleteParameters(
	ctx context.Context,
	input *DeleteParametersInput,
) (*DeleteParametersOutput, error) {
	region := getRegion(ctx)

	b.mu.Lock("DeleteParameters")
	defer b.mu.Unlock()

	params := b.parametersStore(region)
	history := b.historyStore(region)
	tags := b.tagsStore(region)

	output := &DeleteParametersOutput{
		DeletedParameters: make([]string, 0, len(input.Names)),
		InvalidParameters: make([]string, 0, len(input.Names)),
	}

	for _, name := range input.Names {
		if _, exists := params[name]; exists {
			delete(params, name)
			delete(history, name)
			b.removeSortedParamName(region, name)
			if t, ok := tags[name]; ok {
				t.Close()
				delete(tags, name)
			}
			output.DeletedParameters = append(output.DeletedParameters, name)
		} else {
			output.InvalidParameters = append(output.InvalidParameters, name)
		}
	}

	return output, nil
}

// GetParameterHistory retrieves all versions of a parameter.
// buildReversedHistory returns the history list in reverse order (newest first),
// populating labels from parameterLabels and optionally decrypting SecureString values.
func (b *InMemoryBackend) buildReversedHistory(
	historyList []ParameterHistory,
	parameterLabels map[string]map[int64][]string,
	name string,
	withDecryption bool,
) []ParameterHistory {
	n := len(historyList)
	reversed := make([]ParameterHistory, n)
	for i, h := range historyList {
		entry := h
		if versionLabels, ok := parameterLabels[name]; ok {
			if labels, ok2 := versionLabels[entry.Version]; ok2 && len(labels) > 0 {
				entry.Labels = labels
			}
		}
		if withDecryption && entry.Type == SecureStringType {
			if decrypted, err := b.decryptSSMValue(entry.KeyID, entry.Value); err == nil {
				entry.Value = decrypted
			}
		}
		reversed[n-1-i] = entry
	}

	return reversed
}

func (b *InMemoryBackend) GetParameterHistory(
	ctx context.Context,
	input *GetParameterHistoryInput,
) (*GetParameterHistoryOutput, error) {
	region := getRegion(ctx)

	b.mu.RLock("GetParameterHistory")
	defer b.mu.RUnlock()

	historyList, exists := b.historyStore(region)[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	maxResults := int64(maxHistoryResults)
	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > maxHistoryResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				maxHistoryResults,
			)
		}

		maxResults = *input.MaxResults
	}

	reversed := b.buildReversedHistory(
		historyList,
		b.parameterLabelsStore(region),
		input.Name,
		input.WithDecryption,
	)
	n := len(reversed)

	startIdx := parseNextToken(input.NextToken)

	if startIdx >= n {
		return &GetParameterHistoryOutput{Parameters: []ParameterHistory{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < n {
		nextToken = strconv.Itoa(end)
	} else {
		end = n
	}

	return &GetParameterHistoryOutput{
		Parameters: reversed[startIdx:end],
		NextToken:  nextToken,
	}, nil
}

// ListAll returns all parameters sorted by name (useful for Dashboard UI).
func (b *InMemoryBackend) ListAll(ctx context.Context) []Parameter {
	region := getRegion(ctx)

	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	paramsMap := b.parametersStore(region)
	params := make([]Parameter, 0, len(paramsMap))
	for _, p := range paramsMap {
		params = append(params, p)
	}

	sort.Slice(params, func(i, j int) bool {
		return strings.Compare(params[i].Name, params[j].Name) < 0
	})

	return params
}

const (
	defaultPathMaxResults     = 10
	defaultDescribeMaxResults = 50
)

// paramByPathMatchesFilters converts a Parameter to ParameterMetadata and
// delegates to paramMatchesFilters, keeping GetParametersByPath's complexity low.
func paramByPathMatchesFilters(param Parameter, filters []ParameterFilter) bool {
	meta := ParameterMetadata{
		Name:     param.Name,
		Type:     param.Type,
		KeyID:    param.KeyID,
		Tier:     param.Tier,
		DataType: param.DataType,
	}

	return paramMatchesFilters(meta, filters)
}

// insertSortedParamName inserts name into the sorted paramNamesSorted[region] slice.
// Caller must hold the write lock.
func (b *InMemoryBackend) insertSortedParamName(region, name string) {
	names := b.paramNamesSorted[region]
	i := sort.SearchStrings(names, name)
	b.paramNamesSorted[region] = slices.Insert(names, i, name)
}

// removeSortedParamName removes name from the sorted paramNamesSorted[region] slice.
// Caller must hold the write lock.
func (b *InMemoryBackend) removeSortedParamName(region, name string) {
	names := b.paramNamesSorted[region]
	i := sort.SearchStrings(names, name)
	if i < len(names) && names[i] == name {
		b.paramNamesSorted[region] = slices.Delete(names, i, i+1)
	}
}

// collectPathParamsSorted uses binary search on the sorted name index to find
// parameters matching path in O(log n + k) instead of O(n).
func (b *InMemoryBackend) collectPathParamsSorted(
	store map[string]Parameter,
	sortedNames []string,
	path string,
	recursive bool,
	filters []ParameterFilter,
) []Parameter {
	// Find first name >= path via binary search, then scan while HasPrefix.
	start := sort.SearchStrings(sortedNames, path)
	var matched []Parameter
	for i := start; i < len(sortedNames); i++ {
		name := sortedNames[i]
		if !strings.HasPrefix(name, path) {
			break
		}
		if !recursive {
			suffix := name[len(path):]
			if strings.Contains(suffix, "/") {
				continue
			}
		}
		param, ok := store[name]
		if !ok {
			continue
		}
		if len(filters) > 0 && !paramByPathMatchesFilters(param, filters) {
			continue
		}
		matched = append(matched, param)
	}
	// Results are already sorted since sortedNames is sorted.
	return matched
}

// decryptParamsSlice returns a copy of params with SecureString values decrypted
// when requested, and the ARN populated on each parameter.
func (b *InMemoryBackend) decryptParamsSlice(
	params []Parameter, withDecryption bool, region, account string,
) []Parameter {
	// No capacity hint — user-derived values in the capacity slot trigger CodeQL.
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	result := make([]Parameter, 0)
	for _, p := range params {
		if withDecryption && p.Type == SecureStringType {
			if decrypted, err := b.decryptSSMValue(p.KeyID, p.Value); err == nil {
				p.Value = decrypted
			}
		}
		p.ARN = parameterARN(region, account, p.Name)
		result = append(result, p)
	}

	return result
}

// GetParametersByPath returns parameters whose names begin with the given path.
func (b *InMemoryBackend) GetParametersByPath(
	ctx context.Context,
	input *GetParametersByPathInput,
) (*GetParametersByPathOutput, error) {
	region := getRegion(ctx)
	account := awsmeta.Account(ctx)

	b.mu.RLock("GetParametersByPath")
	defer b.mu.RUnlock()

	path := input.Path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	matched := b.collectPathParamsSorted(
		b.parametersStore(region),
		b.paramNamesSorted[region],
		path,
		input.Recursive,
		input.ParameterFilters,
	)

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultPathMaxResults)
	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > defaultPathMaxResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				defaultPathMaxResults,
			)
		}

		maxResults = *input.MaxResults
	}

	if startIdx >= len(matched) {
		return &GetParametersByPathOutput{Parameters: []Parameter{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(matched) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(matched)
	}

	return &GetParametersByPathOutput{
		Parameters: b.decryptParamsSlice(matched[startIdx:end], input.WithDecryption, region, account),
		NextToken:  nextToken,
	}, nil
}

// DescribeParameters returns metadata for all parameters (no values).
func (b *InMemoryBackend) DescribeParameters(
	ctx context.Context,
	input *DescribeParametersInput,
) (*DescribeParametersOutput, error) {
	region := getRegion(ctx)

	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	paramsMap := b.parametersStore(region)
	all := make([]ParameterMetadata, 0, len(paramsMap))

	for _, p := range paramsMap {
		all = append(all, ParameterMetadata{
			Name:             p.Name,
			Type:             p.Type,
			Version:          p.Version,
			LastModifiedDate: p.LastModifiedDate,
			Description:      p.Description,
			KeyID:            p.KeyID,
			Tier:             p.Tier,
			AllowedPattern:   p.AllowedPattern,
			DataType:         p.DataType,
			Policies:         p.Policies,
		})
	}

	// Apply filters
	if len(input.ParameterFilters) > 0 {
		var filtered []ParameterMetadata

		for _, meta := range all {
			if paramMatchesFilters(meta, input.ParameterFilters) {
				filtered = append(filtered, meta)
			}
		}

		all = filtered
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name < all[j].Name
	})

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultDescribeMaxResults)
	if input.MaxResults != nil {
		if *input.MaxResults < 1 || *input.MaxResults > defaultDescribeMaxResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between 1 and %d",
				ErrValidationException,
				defaultDescribeMaxResults,
			)
		}

		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &DescribeParametersOutput{Parameters: []ParameterMetadata{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &DescribeParametersOutput{
		Parameters: all[startIdx:end],
		NextToken:  nextToken,
	}, nil
}

// parseNextToken converts a NextToken string to an integer start index.
func parseNextToken(token string) int {
	if token == "" {
		return 0
	}

	idx, err := strconv.Atoi(token)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// paramMatchesFilters returns true when the metadata satisfies ALL filters.
func paramMatchesFilters(meta ParameterMetadata, filters []ParameterFilter) bool {
	for _, f := range filters {
		if !paramMatchesFilter(meta, f) {
			return false
		}
	}

	return true
}

// paramMatchesFilter returns true when the metadata satisfies a single filter.
// Within one filter, multiple Values are OR-combined.
// Returns an error for unrecognised filter keys (AWS behavior).
func paramMatchesFilter(meta ParameterMetadata, f ParameterFilter) bool {
	var fieldValue string

	switch f.Key {
	case "Name":
		fieldValue = meta.Name
	case "Type":
		fieldValue = meta.Type
	case "KeyId":
		fieldValue = meta.KeyID
	case "Tier":
		fieldValue = meta.Tier
	case "DataType":
		fieldValue = meta.DataType
	default:
		return true // unknown keys are silently ignored (backwards compat)
	}

	option := f.Option
	if option == "" {
		option = "Equals"
	}

	for _, v := range f.Values {
		switch option {
		case "Equals":
			if fieldValue == v {
				return true
			}
		case "BeginsWith":
			if strings.HasPrefix(fieldValue, v) {
				return true
			}
		case "Contains":
			if strings.Contains(fieldValue, v) {
				return true
			}
		}
	}

	return false
}

// AddTagsToResource adds or updates tags for a resource.
func (b *InMemoryBackend) AddTagsToResource(
	ctx context.Context,
	input *AddTagsToResourceInput,
) error {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("AddTagsToResource")
		defer b.mu.Unlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if _, ok := params[name]; !ok {
			return ErrParameterNotFound
		}
		if b.tags[region] == nil {
			b.tags[region] = make(map[string]*tags.Tags)
		}
		tagsStore := b.tagsStore(region)
		if tagsStore[name] == nil {
			tagsStore[name] = tags.New("ssm." + name + ".tags")
		}
		for _, t := range input.Tags {
			tagsStore[name].Set(t.Key, t.Value)
		}

		return nil
	}

	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	if b.miscResourceTags[region] == nil {
		b.miscResourceTags[region] = make(map[string]map[string]string)
	}
	miscTags := b.miscResourceTagsStore(region)
	if miscTags[input.ResourceID] == nil {
		miscTags[input.ResourceID] = make(map[string]string)
	}
	for _, t := range input.Tags {
		miscTags[input.ResourceID][t.Key] = t.Value
	}

	return nil
}

// RemoveTagsFromResource removes tags from a resource.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	input *RemoveTagsFromResourceInput,
) error {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("RemoveTagsFromResource")
		defer b.mu.Unlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if _, ok := params[name]; !ok {
			return ErrParameterNotFound
		}
		tagsStore := b.tagsStore(region)
		if tagsStore[name] != nil {
			tagsStore[name].DeleteKeys(input.TagKeys)
		}

		return nil
	}

	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	miscTags := b.miscResourceTagsStore(region)
	if rt := miscTags[input.ResourceID]; rt != nil {
		for _, k := range input.TagKeys {
			delete(rt, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	input *ListTagsForResourceInput,
) (*ListTagsForResourceOutput, error) {
	region := getRegion(ctx)

	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.RLock("ListTagsForResource")
		defer b.mu.RUnlock()

		params := b.parametersStore(region)
		name := input.ResourceID
		if _, ok := params[name]; !ok {
			return nil, ErrParameterNotFound
		}
		var tagList []Tag
		tagsStore := b.tagsStore(region)
		if tagsStore[name] != nil {
			for k, v := range tagsStore[name].Clone() {
				tagList = append(tagList, Tag{Key: k, Value: v})
			}
		}
		sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

		return &ListTagsForResourceOutput{TagList: tagList}, nil
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	miscTags := b.miscResourceTagsStore(region)
	var tagList []Tag
	for k, v := range miscTags[input.ResourceID] {
		tagList = append(tagList, Tag{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return &ListTagsForResourceOutput{TagList: tagList}, nil
}

// registerDefaultDocuments pre-registers the built-in AWS documents.
func (b *InMemoryBackend) registerDefaultDocuments(region string) {
	now := UnixTimeFloat(time.Now())
	defaults := []struct {
		name     string
		docType  string
		content  string
		platform []string
	}{
		{
			name:    "AWS-RunShellScript",
			docType: DocumentTypeCommand,
			content: `{"schemaVersion":"2.2","description":"Run shell script",` +
				`"parameters":{"commands":{"type":"StringList"}},` +
				`"mainSteps":[{"action":"aws:runShellScript","name":"runShellScript",` +
				`"inputs":{"commands":["{{ commands }}"]}}]}`,
			platform: []string{"Linux"},
		},
		{
			name:    "AWS-RunPowerShellScript",
			docType: DocumentTypeCommand,
			content: `{"schemaVersion":"2.2","description":"Run PowerShell script",` +
				`"parameters":{"commands":{"type":"StringList"}},` +
				`"mainSteps":[{"action":"aws:runPowerShellScript","name":"runPowerShellScript",` +
				`"inputs":{"commands":["{{ commands }}"]}}]}`,
			platform: []string{"Windows"},
		},
	}

	if b.documents[region] == nil {
		b.documents[region] = make(map[string]Document)
	}
	if b.documentVersions[region] == nil {
		b.documentVersions[region] = make(map[string][]DocumentVersion)
	}
	documents := b.documentsStore(region)
	documentVersions := b.documentVersionsStore(region)

	for _, d := range defaults {
		doc := Document{
			Name:            d.name,
			Content:         d.content,
			DocumentType:    d.docType,
			DocumentFormat:  documentFormatJSON,
			Status:          statusActive,
			SchemaVersion:   "2.2",
			PlatformTypes:   d.platform,
			CreatedDate:     now,
			DocumentVersion: "1",
			LatestVersion:   "1",
			DefaultVersion:  "1",
		}
		documents[d.name] = doc
		documentVersions[d.name] = []DocumentVersion{
			{
				Name:             d.name,
				DocumentVersion:  "1",
				CreatedDate:      now,
				IsDefaultVersion: true,
				DocumentFormat:   documentFormatJSON,
				Status:           statusActive,
				Content:          d.content,
			},
		}
	}
}

const defaultListDocMaxResults = 50

// CreateDocument stores a new SSM document.
func (b *InMemoryBackend) CreateDocument(
	ctx context.Context,
	input *CreateDocumentInput,
) (*CreateDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CreateDocument")
	defer b.mu.Unlock()

	if b.documents[region] == nil {
		b.documents[region] = make(map[string]Document)
	}
	store := b.documentsStore(region)
	if _, exists := store[input.Name]; exists {
		return nil, ErrDocumentAlreadyExists
	}

	format := input.DocumentFormat
	if format == "" {
		format = documentFormatJSON
	}

	docType := input.DocumentType
	if docType == "" {
		docType = DocumentTypeCommand
	}

	now := UnixTimeFloat(time.Now())
	doc := Document{
		Name:            input.Name,
		Content:         input.Content,
		DocumentType:    docType,
		DocumentFormat:  format,
		Status:          statusActive,
		TargetType:      input.TargetType,
		Description:     input.Description,
		PlatformTypes:   input.PlatformTypes,
		SchemaVersion:   "2.2",
		CreatedDate:     now,
		DocumentVersion: "1",
		LatestVersion:   "1",
		DefaultVersion:  "1",
		Requires:        input.Requires,
	}

	store[input.Name] = doc
	if b.documentVersions[region] == nil {
		b.documentVersions[region] = make(map[string][]DocumentVersion)
	}
	versionStore := b.documentVersionsStore(region)
	versionStore[input.Name] = []DocumentVersion{
		{
			Name:             input.Name,
			DocumentVersion:  "1",
			CreatedDate:      now,
			IsDefaultVersion: true,
			DocumentFormat:   format,
			Status:           statusActive,
			Content:          input.Content,
		},
	}

	return &CreateDocumentOutput{DocumentDescription: doc}, nil
}

// GetDocument retrieves a document's content.
func (b *InMemoryBackend) GetDocument(
	ctx context.Context,
	input *GetDocumentInput,
) (*GetDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetDocument")
	defer b.mu.RUnlock()

	doc, exists := b.documentsStore(region)[input.Name]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	content := doc.Content
	version := doc.DocumentVersion

	if input.DocumentVersion != "" && input.DocumentVersion != "$LATEST" &&
		input.DocumentVersion != "$DEFAULT" {
		versions := b.documentVersionsStore(region)[input.Name]
		found := false
		for _, v := range versions {
			if v.DocumentVersion == input.DocumentVersion {
				found = true
				version = v.DocumentVersion
				content = v.Content

				break
			}
		}
		if !found {
			return nil, ErrInvalidDocumentVersion
		}
	}

	return &GetDocumentOutput{
		Name:            doc.Name,
		Content:         content,
		DocumentType:    doc.DocumentType,
		DocumentFormat:  doc.DocumentFormat,
		DocumentVersion: version,
		Status:          doc.Status,
	}, nil
}

// documentMatchesFilters returns true when doc satisfies all provided DocumentFilters.
// Supported filter keys: DocumentType, Name.
func documentMatchesFilters(doc Document, filters []DocumentFilter) bool {
	for _, f := range filters {
		var fieldValue string

		switch f.Key {
		case "DocumentType":
			fieldValue = doc.DocumentType
		case "Name":
			fieldValue = doc.Name
		default:
			continue
		}

		if !slices.Contains(f.Values, fieldValue) {
			return false
		}
	}

	return true
}

// DescribeDocument returns document metadata.
func (b *InMemoryBackend) DescribeDocument(
	ctx context.Context,
	input *DescribeDocumentInput,
) (*DescribeDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeDocument")
	defer b.mu.RUnlock()

	doc, exists := b.documentsStore(region)[input.Name]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	return &DescribeDocumentOutput{Document: doc}, nil
}

// ListDocuments returns a list of document identifiers filtered by key-value criteria.
func (b *InMemoryBackend) ListDocuments(
	ctx context.Context,
	input *ListDocumentsInput,
) (*ListDocumentsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListDocuments")
	defer b.mu.RUnlock()

	// Merge Filters and DocumentFilters (both carry the same shape).
	allFilters := make([]DocumentFilter, 0, len(input.Filters)+len(input.DocumentFilters))
	allFilters = append(allFilters, input.Filters...)
	allFilters = append(allFilters, input.DocumentFilters...)

	store := b.documentsStore(region)
	all := make([]DocumentIdentifier, 0, len(store))
	for _, doc := range store {
		if !documentMatchesFilters(doc, allFilters) {
			continue
		}

		all = append(all, DocumentIdentifier{
			Name:            doc.Name,
			DocumentType:    doc.DocumentType,
			DocumentFormat:  doc.DocumentFormat,
			DocumentVersion: doc.DocumentVersion,
			SchemaVersion:   doc.SchemaVersion,
			PlatformTypes:   doc.PlatformTypes,
		})
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListDocumentsOutput{DocumentIdentifiers: []DocumentIdentifier{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListDocumentsOutput{
		DocumentIdentifiers: all[startIdx:end],
		NextToken:           nextToken,
	}, nil
}

// UpdateDocument increments the document version and updates content.
func (b *InMemoryBackend) UpdateDocument(
	ctx context.Context,
	input *UpdateDocumentInput,
) (*UpdateDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("UpdateDocument")
	defer b.mu.Unlock()

	store := b.documentsStore(region)
	doc, exists := store[input.Name]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	// Validate DocumentVersion if provided.
	if input.DocumentVersion != "" {
		switch input.DocumentVersion {
		case "$LATEST", "$DEFAULT", doc.LatestVersion:
			// accepted versions
		default:
			return nil, ErrInvalidDocumentVersion
		}
	}

	latestVer, _ := strconv.Atoi(doc.LatestVersion)
	newVer := strconv.Itoa(latestVer + 1)

	format := input.DocumentFormat
	if format == "" {
		format = doc.DocumentFormat
	}

	now := UnixTimeFloat(time.Now())
	doc.Content = input.Content
	doc.DocumentVersion = newVer
	doc.LatestVersion = newVer
	doc.DocumentFormat = format
	store[input.Name] = doc

	versionStore := b.documentVersionsStore(region)
	versionStore[input.Name] = append(versionStore[input.Name], DocumentVersion{
		Name:             input.Name,
		DocumentVersion:  newVer,
		CreatedDate:      now,
		IsDefaultVersion: false,
		DocumentFormat:   format,
		Status:           statusActive,
		Content:          input.Content,
	})

	if len(versionStore[input.Name]) > maxDocumentVersionCap {
		vers := versionStore[input.Name]
		versionStore[input.Name] = vers[len(vers)-maxDocumentVersionCap:]
	}

	return &UpdateDocumentOutput{DocumentDescription: doc}, nil
}

// DeleteDocument removes a document and all its versions and permissions.
func (b *InMemoryBackend) DeleteDocument(
	ctx context.Context,
	input *DeleteDocumentInput,
) (*DeleteDocumentOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("DeleteDocument")
	defer b.mu.Unlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	delete(b.documentsStore(region), input.Name)
	delete(b.documentVersionsStore(region), input.Name)
	delete(b.documentPermissionsStore(region), input.Name)

	return &DeleteDocumentOutput{}, nil
}

// DescribeDocumentPermission returns the sharing permissions for a document.
func (b *InMemoryBackend) DescribeDocumentPermission(
	ctx context.Context,
	input *DescribeDocumentPermissionInput,
) (*DescribeDocumentPermissionOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("DescribeDocumentPermission")
	defer b.mu.RUnlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	accountIDs := b.documentPermissionsStore(region)[input.Name]
	if accountIDs == nil {
		accountIDs = []string{}
	}

	return &DescribeDocumentPermissionOutput{
		AccountIDs:             accountIDs,
		AccountSharingInfoList: []any{},
	}, nil
}

// ModifyDocumentPermission updates the sharing permissions for a document.
func (b *InMemoryBackend) ModifyDocumentPermission(
	ctx context.Context,
	input *ModifyDocumentPermissionInput,
) (*ModifyDocumentPermissionOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("ModifyDocumentPermission")
	defer b.mu.Unlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	if b.documentPermissions[region] == nil {
		b.documentPermissions[region] = make(map[string][]string)
	}
	store := b.documentPermissionsStore(region)
	current := store[input.Name]

	for _, id := range input.AccountIDsToAdd {
		if !slices.Contains(current, id) {
			current = append(current, id)
		}
	}

	for _, id := range input.AccountIDsToRemove {
		current = slices.DeleteFunc(current, func(v string) bool { return v == id })
	}

	store[input.Name] = current

	return &ModifyDocumentPermissionOutput{}, nil
}

// ListDocumentVersions returns all versions of a document.
func (b *InMemoryBackend) ListDocumentVersions(
	ctx context.Context,
	input *ListDocumentVersionsInput,
) (*ListDocumentVersionsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListDocumentVersions")
	defer b.mu.RUnlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	versions := b.documentVersionsStore(region)[input.Name]

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(versions) {
		return &ListDocumentVersionsOutput{DocumentVersions: []DocumentVersion{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(versions) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(versions)
	}

	return &ListDocumentVersionsOutput{
		DocumentVersions: versions[startIdx:end],
		NextToken:        nextToken,
	}, nil
}

// SendCommand records a command stub and returns a generated command ID.
func (b *InMemoryBackend) SendCommand(
	ctx context.Context,
	input *SendCommandInput,
) (*SendCommandOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("SendCommand")
	defer b.mu.Unlock()

	if _, exists := b.documentsStore(region)[input.DocumentName]; !exists {
		return nil, ErrDocumentNotFound
	}

	now := UnixTimeFloat(time.Now())
	cmdID := uuid.NewString()

	// Prune any commands that have aged out so the commands/invocations maps do
	// not grow unbounded between (or without) janitor runs.
	b.expireCommandsLocked(region, now)

	timeoutSecs := input.TimeoutSeconds
	if timeoutSecs == 0 {
		timeoutSecs = 3600
	}

	cmd := Command{
		CommandID:          cmdID,
		DocumentName:       input.DocumentName,
		Parameters:         input.Parameters,
		Status:             commandStatusSuccess,
		StatusDetails:      commandStatusSuccess,
		RequestedDateTime:  now,
		ExpiresAfter:       now + b.commandExpirySecs,
		InstanceIDs:        input.InstanceIDs,
		Targets:            input.Targets,
		Comment:            input.Comment,
		TimeoutSeconds:     timeoutSecs,
		OutputS3BucketName: input.OutputS3BucketName,
		OutputS3KeyPrefix:  input.OutputS3KeyPrefix,
		OutputS3Region:     input.OutputS3Region,
	}

	if b.commands[region] == nil {
		b.commands[region] = make(map[string]Command)
	}
	b.commandsStore(region)[cmdID] = cmd

	invocations := make([]CommandInvocation, 0, len(input.InstanceIDs))
	for _, instanceID := range input.InstanceIDs {
		inv := CommandInvocation{
			CommandID:         cmdID,
			InstanceID:        instanceID,
			DocumentName:      input.DocumentName,
			Status:            commandStatusSuccess,
			StatusDetails:     commandStatusSuccess,
			RequestedDateTime: now,
			Comment:           input.Comment,
		}
		invocations = append(invocations, inv)
	}
	if b.commandInvocations[region] == nil {
		b.commandInvocations[region] = make(map[string][]CommandInvocation)
	}
	b.commandInvocationsStore(region)[cmdID] = invocations

	return &SendCommandOutput{Command: cmd}, nil
}

// ListCommands returns recorded commands.
func (b *InMemoryBackend) ListCommands(
	ctx context.Context,
	input *ListCommandsInput,
) (*ListCommandsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListCommands")
	defer b.mu.RUnlock()

	store := b.commandsStore(region)
	all := make([]Command, 0, len(store))
	for _, cmd := range store {
		if input.CommandID != "" && cmd.CommandID != input.CommandID {
			continue
		}
		all = append(all, cmd)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CommandID < all[j].CommandID })

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListCommandsOutput{Commands: []Command{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListCommandsOutput{
		Commands:  all[startIdx:end],
		NextToken: nextToken,
	}, nil
}

// GetCommandInvocation returns the stored invocation for the given command and instance.
func (b *InMemoryBackend) GetCommandInvocation(
	ctx context.Context,
	input *GetCommandInvocationInput,
) (*GetCommandInvocationOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("GetCommandInvocation")
	defer b.mu.RUnlock()

	if _, exists := b.commandsStore(region)[input.CommandID]; !exists {
		return nil, ErrCommandNotFound
	}

	for _, inv := range b.commandInvocationsStore(region)[input.CommandID] {
		if inv.InstanceID == input.InstanceID {
			return &GetCommandInvocationOutput{
				CommandID:             input.CommandID,
				InstanceID:            input.InstanceID,
				DocumentName:          inv.DocumentName,
				Status:                inv.Status,
				StatusDetails:         inv.StatusDetails,
				StandardOutputContent: inv.StandardOutputContent,
				StandardErrorContent:  inv.StandardErrorContent,
				StandardOutputURL:     inv.StandardOutputURL,
				StandardErrorURL:      inv.StandardErrorURL,
				Comment:               inv.Comment,
			}, nil
		}
	}

	return nil, ErrCommandNotFound
}

// ListCommandInvocations returns invocations for a given command.
func (b *InMemoryBackend) ListCommandInvocations(
	ctx context.Context,
	input *ListCommandInvocationsInput,
) (*ListCommandInvocationsOutput, error) {
	region := getRegion(ctx)
	b.mu.RLock("ListCommandInvocations")
	defer b.mu.RUnlock()

	all := make([]CommandInvocation, 0, len(b.commandInvocationsStore(region)))
	for cmdID, invs := range b.commandInvocationsStore(region) {
		if input.CommandID != "" && cmdID != input.CommandID {
			continue
		}
		for _, inv := range invs {
			if input.InstanceID != "" && inv.InstanceID != input.InstanceID {
				continue
			}
			all = append(all, inv)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CommandID != all[j].CommandID {
			return all[i].CommandID < all[j].CommandID
		}

		return all[i].InstanceID < all[j].InstanceID
	})

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultListDocMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
		maxResults = *input.MaxResults
	}

	if startIdx >= len(all) {
		return &ListCommandInvocationsOutput{CommandInvocations: []CommandInvocation{}}, nil
	}

	end := startIdx + int(maxResults)

	var nextToken string

	if end < len(all) {
		nextToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return &ListCommandInvocationsOutput{
		CommandInvocations: all[startIdx:end],
		NextToken:          nextToken,
	}, nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, regionTags := range b.tags {
		for _, t := range regionTags {
			t.Close()
		}
	}

	b.parameters = make(map[string]map[string]Parameter)
	b.paramNamesSorted = make(map[string][]string)
	b.history = make(map[string]map[string][]ParameterHistory)
	b.tags = make(map[string]map[string]*tags.Tags)
	b.documents = make(map[string]map[string]Document)
	b.documentVersions = make(map[string]map[string][]DocumentVersion)
	b.documentPermissions = make(map[string]map[string][]string)
	b.commands = make(map[string]map[string]Command)
	b.commandInvocations = make(map[string]map[string][]CommandInvocation)
	b.activations = make(map[string]map[string]Activation)
	b.associations = make(map[string]map[string]Association)
	b.maintenanceWindows = make(map[string]map[string]MaintenanceWindow)
	b.maintenanceWindowTargets = make(map[string]map[string]MaintenanceWindowTarget)
	b.maintenanceWindowTasks = make(map[string]map[string]MaintenanceWindowTask)
	b.sessions = make(map[string]map[string]Session)
	b.patchGroupToBaseline = make(map[string]map[string]string)
	b.opsItems = make(map[string]map[string]OpsItem)
	b.opsItemRelatedItems = make(map[string]map[string][]OpsItemRelatedItem)
	b.opsMetadata = make(map[string]map[string]OpsMetadata)
	b.patchBaselines = make(map[string]map[string]PatchBaseline)
	b.resourceIDToOpsMetadataArn = make(map[string]map[string]string)
	b.miscResourceTags = make(map[string]map[string]map[string]string)
	b.resourceDataSyncs = make(map[string]map[string]*ResourceDataSync)
	b.parameterLabels = make(map[string]map[string]map[int64][]string)
	b.automationExecutions = make(map[string]map[string]*AutomationExecution)
	b.serviceSettings = make(map[string]map[string]*ServiceSetting)
	b.resourcePolicies = make(map[string]map[string][]*ResourcePolicy)
	b.executionPreviews = make(map[string]map[string]*ExecutionPreview)
	b.inventory = make(map[string]map[string][]InventoryItem)
	b.compliance = make(map[string]map[string][]ComplianceItem)
	b.opsItemEvents = nil
	b.registerDefaultDocuments(defaultRegion)
}

const (
	activationCodeChars        = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	activationCodeLen          = 20
	windowIDPrefix             = "mw-"
	windowTargetIDPrefix       = "mwt-"
	windowTaskIDPrefix         = "mwtask-"
	sessionIDPrefix            = "session-"
	sessionStatusConnected     = "Connected"
	sessionStatusTerminated    = "Terminated"
	activationIDPrefix         = "act-"
	baselineIDPrefix           = "pb-"
	opsItemIDPrefix            = "oi-"
	opsMetadataArnTpl          = "arn:aws:ssm:%s:%s:opsmetadata/%s"
	defaultAccountID           = "123456789012"
	defaultRegion              = "us-east-1"
	defaultOpsItemStatus       = "Open"
	defaultActivationExpiryHrs = 24
)

const (
	commandStatusPending    = "Pending"
	commandStatusInProgress = "InProgress"
	commandStatusSuccess    = "Success"
	commandStatusFailed     = "Failed"
	commandStatusCancelled  = "Cancelled"
	commandStatusTimedOut   = "TimedOut"
	assocStatusSuccess      = "Success"
	faultClient             = "Client"
	opsItemStatusOpen       = "Open"
)

func generateCode(n int) string {
	const (
		byteRange = 256
		bufMult   = 2
		maxChar   = byte(byteRange - (byteRange % len(activationCodeChars)))
	)
	result := make([]byte, 0, n)
	buf := make([]byte, n*bufMult)
	for len(result) < n {
		_, _ = rand.Read(buf)
		for _, b := range buf {
			if len(result) == n {
				break
			}
			if b < maxChar {
				result = append(result, activationCodeChars[int(b)%len(activationCodeChars)])
			}
		}
	}

	return string(result)
}

// CancelCommand cancels a running command (sets status to Cancelled).
func (b *InMemoryBackend) CancelCommand(
	ctx context.Context,
	input *CancelCommandInput,
) (*CancelCommandOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CancelCommand")
	defer b.mu.Unlock()

	store := b.commandsStore(region)
	cmd, exists := store[input.CommandID]
	if !exists {
		return nil, ErrCommandNotFound
	}

	cmd.Status = commandStatusCancelled
	store[input.CommandID] = cmd

	invStore := b.commandInvocationsStore(region)
	invs := invStore[input.CommandID]
	for i := range invs {
		invs[i].Status = commandStatusCancelled
		invs[i].StatusDetails = commandStatusCancelled
	}
	invStore[input.CommandID] = invs

	return &CancelCommandOutput{}, nil
}

// CancelMaintenanceWindowExecution cancels a maintenance window execution.
func (b *InMemoryBackend) CancelMaintenanceWindowExecution(
	_ context.Context,
	input *CancelMaintenanceWindowExecutionInput,
) (*CancelMaintenanceWindowExecutionOutput, error) {
	return &CancelMaintenanceWindowExecutionOutput{
		WindowExecutionID: input.WindowExecutionID,
	}, nil
}

// CreateActivation creates a new activation for managed instances.
func (b *InMemoryBackend) CreateActivation(
	ctx context.Context,
	input *CreateActivationInput,
) (*CreateActivationOutput, error) {
	if input.IamRole == "" {
		return nil, fmt.Errorf("%w: IamRole is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateActivation")
	defer b.mu.Unlock()

	activationID := activationIDPrefix + uuid.NewString()
	code := generateCode(activationCodeLen)

	limit := input.RegistrationLimit
	if limit <= 0 {
		limit = 1
	}

	now := UnixTimeFloat(time.Now())
	expiry := input.ExpirationDate
	if expiry == 0 {
		expiry = UnixTimeFloat(time.Now().Add(defaultActivationExpiryHrs * time.Hour))
	}

	act := Activation{
		ActivationID:        activationID,
		ActivationCode:      code,
		Description:         input.Description,
		DefaultInstanceName: input.DefaultInstanceName,
		IamRole:             input.IamRole,
		RegistrationLimit:   limit,
		RegistrationsCount:  0,
		ExpirationDate:      expiry,
		Expired:             false,
		CreatedDate:         now,
	}

	if b.activations[region] == nil {
		b.activations[region] = make(map[string]Activation)
	}
	b.activationsStore(region)[activationID] = act

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[activationID] == nil {
			miscTags[activationID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[activationID][t.Key] = t.Value
		}
	}

	return &CreateActivationOutput{
		ActivationCode: code,
		ActivationID:   activationID,
	}, nil
}

// copyAssocParameters deep copies a parameters map for associations.
func copyAssocParameters(src map[string][]string) map[string][]string {
	if src == nil {
		return nil
	}
	dst := make(map[string][]string, len(src))
	for k, v := range src {
		dst[k] = append([]string(nil), v...)
	}

	return dst
}

// copyAssocTargets deep copies a targets slice for associations.
func copyAssocTargets(src []AssociationTarget) []AssociationTarget {
	if src == nil {
		return nil
	}
	dst := make([]AssociationTarget, len(src))
	for i, t := range src {
		dst[i] = AssociationTarget{
			Key:    t.Key,
			Values: append([]string(nil), t.Values...),
		}
	}

	return dst
}

// CreateAssociation creates a new association between a document and targets.
func (b *InMemoryBackend) CreateAssociation(
	ctx context.Context,
	input *CreateAssociationInput,
) (*CreateAssociationOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateAssociation")
	defer b.mu.Unlock()

	if _, exists := b.documentsStore(region)[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	assocID := uuid.NewString()
	now := UnixTimeFloat(time.Now())

	assoc := Association{
		AssociationID:             assocID,
		AssociationName:           input.AssociationName,
		DocumentVersion:           input.DocumentVersion,
		InstanceID:                input.InstanceID,
		Name:                      input.Name,
		Parameters:                copyAssocParameters(input.Parameters),
		Targets:                   copyAssocTargets(input.Targets),
		Overview:                  &AssociationOverview{Status: assocStatusSuccess},
		LastUpdateAssociationDate: now,
	}

	if b.associations[region] == nil {
		b.associations[region] = make(map[string]Association)
	}
	b.associationsStore(region)[assocID] = assoc

	return &CreateAssociationOutput{AssociationDescription: assoc}, nil
}

// CreateAssociationBatch creates multiple associations in a batch.
func (b *InMemoryBackend) CreateAssociationBatch(
	ctx context.Context,
	input *CreateAssociationBatchInput,
) (*CreateAssociationBatchOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("CreateAssociationBatch")
	defer b.mu.Unlock()

	output := &CreateAssociationBatchOutput{
		Successful: make([]Association, 0, len(input.Entries)),
		Failed:     make([]FailedCreateAssociation, 0, len(input.Entries)),
	}

	now := UnixTimeFloat(time.Now())
	docs := b.documentsStore(region)
	if b.associations[region] == nil {
		b.associations[region] = make(map[string]Association)
	}
	assocs := b.associationsStore(region)

	for _, entry := range input.Entries {
		if _, exists := docs[entry.Name]; !exists {
			output.Failed = append(output.Failed, FailedCreateAssociation{
				Entry:   entry,
				Message: ErrDocumentNotFound.Error(),
				Fault:   faultClient,
			})

			continue
		}

		assocID := uuid.NewString()
		assoc := Association{
			AssociationID:             assocID,
			AssociationName:           entry.AssociationName,
			DocumentVersion:           entry.DocumentVersion,
			InstanceID:                entry.InstanceID,
			Name:                      entry.Name,
			Parameters:                copyAssocParameters(entry.Parameters),
			Targets:                   copyAssocTargets(entry.Targets),
			Overview:                  &AssociationOverview{Status: assocStatusSuccess},
			LastUpdateAssociationDate: now,
		}

		assocs[assocID] = assoc
		output.Successful = append(output.Successful, assoc)
	}

	return output, nil
}

// CreateMaintenanceWindow creates a new maintenance window.
func (b *InMemoryBackend) CreateMaintenanceWindow(
	ctx context.Context,
	input *CreateMaintenanceWindowInput,
) (*CreateMaintenanceWindowOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	const (
		minWindowDuration = int32(1)
		maxWindowDuration = int32(24)
	)
	if input.Duration < minWindowDuration || input.Duration > maxWindowDuration {
		return nil, fmt.Errorf(
			"%w: Duration must be between 1 and 24 hours",
			ErrValidationException,
		)
	}
	if input.Cutoff >= input.Duration {
		return nil, fmt.Errorf("%w: Cutoff must be less than Duration", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateMaintenanceWindow")
	defer b.mu.Unlock()

	windowID := windowIDPrefix + uuid.NewString()
	now := UnixTimeFloat(time.Now())

	mw := MaintenanceWindow{
		WindowID:                 windowID,
		Name:                     input.Name,
		Description:              input.Description,
		Schedule:                 input.Schedule,
		Duration:                 input.Duration,
		Cutoff:                   input.Cutoff,
		AllowUnassociatedTargets: input.AllowUnassociatedTargets,
		Enabled:                  true,
		CreatedDate:              now,
		ModifiedDate:             now,
	}

	if b.maintenanceWindows[region] == nil {
		b.maintenanceWindows[region] = make(map[string]MaintenanceWindow)
	}
	b.maintenanceWindowsStore(region)[windowID] = mw

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[windowID] == nil {
			miscTags[windowID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[windowID][t.Key] = t.Value
		}
	}

	return &CreateMaintenanceWindowOutput{WindowID: windowID}, nil
}

// CreateOpsItem creates a new OpsItem.
func (b *InMemoryBackend) CreateOpsItem(
	ctx context.Context,
	input *CreateOpsItemInput,
) (*CreateOpsItemOutput, error) {
	if input.Title == "" {
		return nil, fmt.Errorf("%w: Title is required", ErrValidationException)
	}

	if input.Source == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateOpsItem")
	defer b.mu.Unlock()

	opsItemID := opsItemIDPrefix + uuid.NewString()
	opsItemArn := arn.Build("ssm", region, defaultAccountID, fmt.Sprintf("opsitem/%s", opsItemID))
	now := UnixTimeFloat(time.Now())

	item := OpsItem{
		OpsItemID:        opsItemID,
		OpsItemArn:       opsItemArn,
		OpsItemType:      input.OpsItemType,
		Title:            input.Title,
		Source:           input.Source,
		Description:      input.Description,
		Status:           opsItemStatusOpen,
		Severity:         input.Severity,
		Category:         input.Category,
		OperationalData:  input.OperationalData,
		CreatedTime:      now,
		LastModifiedTime: now,
	}

	if b.opsItems[region] == nil {
		b.opsItems[region] = make(map[string]OpsItem)
	}
	b.opsItemsStore(region)[opsItemID] = item

	b.opsItemEvents[region] = append(b.opsItemEvents[region], OpsItemEventSummary{
		OpsItemID: opsItemID,
		EventID:   "event-create-" + opsItemID,
	})

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[opsItemID] == nil {
			miscTags[opsItemID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[opsItemID][t.Key] = t.Value
		}
	}

	return &CreateOpsItemOutput{
		OpsItemID:  opsItemID,
		OpsItemArn: opsItemArn,
	}, nil
}

// AssociateOpsItemRelatedItem associates a related item to an OpsItem.
func (b *InMemoryBackend) AssociateOpsItemRelatedItem(
	ctx context.Context,
	input *AssociateOpsItemRelatedItemInput,
) (*AssociateOpsItemRelatedItemOutput, error) {
	region := getRegion(ctx)
	b.mu.Lock("AssociateOpsItemRelatedItem")
	defer b.mu.Unlock()

	if _, exists := b.opsItemsStore(region)[input.OpsItemID]; !exists {
		return nil, ErrOpsItemNotFound
	}

	assocID := uuid.NewString()
	related := OpsItemRelatedItem{
		AssociationID:   assocID,
		AssociationType: input.AssociationType,
		ResourceType:    input.ResourceType,
		ResourceURI:     input.ResourceURI,
	}

	if b.opsItemRelatedItems[region] == nil {
		b.opsItemRelatedItems[region] = make(map[string][]OpsItemRelatedItem)
	}
	store := b.opsItemRelatedItemsStore(region)
	store[input.OpsItemID] = append(store[input.OpsItemID], related)

	return &AssociateOpsItemRelatedItemOutput{AssociationID: assocID}, nil
}

// CreateOpsMetadata creates OpsMetadata for a resource.
func (b *InMemoryBackend) CreateOpsMetadata(
	ctx context.Context,
	input *CreateOpsMetadataInput,
) (*CreateOpsMetadataOutput, error) {
	if input.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidationException)
	}

	region := getRegion(ctx)
	b.mu.Lock("CreateOpsMetadata")
	defer b.mu.Unlock()

	if b.resourceIDToOpsMetadataArn[region] == nil {
		b.resourceIDToOpsMetadataArn[region] = make(map[string]string)
	}
	resToArn := b.resourceIDToOpsMetadataArnStore(region)
	if _, exists := resToArn[input.ResourceID]; exists {
		return nil, fmt.Errorf(
			"%w: OpsMetadata already exists for resource %s",
			ErrOpsMetadataAlreadyExists,
			input.ResourceID,
		)
	}

	metaID := uuid.NewString()
	arn := fmt.Sprintf(opsMetadataArnTpl, region, defaultAccountID, metaID)
	now := UnixTimeFloat(time.Now())

	meta := OpsMetadata{
		OpsMetadataArn:   arn,
		ResourceID:       input.ResourceID,
		Metadata:         input.Metadata,
		CreationDate:     now,
		LastModifiedDate: now,
	}

	if b.opsMetadata[region] == nil {
		b.opsMetadata[region] = make(map[string]OpsMetadata)
	}
	b.opsMetadataStore(region)[arn] = meta
	resToArn[input.ResourceID] = arn

	return &CreateOpsMetadataOutput{OpsMetadataArn: arn}, nil
}

// CreatePatchBaseline creates a new patch baseline.
func (b *InMemoryBackend) CreatePatchBaseline(
	ctx context.Context,
	input *CreatePatchBaselineInput,
) (*CreatePatchBaselineOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	const defaultPatchOS = "WINDOWS"
	os := input.OperatingSystem
	if os == "" {
		os = defaultPatchOS
	}

	region := getRegion(ctx)
	b.mu.Lock("CreatePatchBaseline")
	defer b.mu.Unlock()

	baselineID := baselineIDPrefix + uuid.NewString()
	now := UnixTimeFloat(time.Now())

	bl := PatchBaseline{
		BaselineID:                     baselineID,
		Name:                           input.Name,
		Description:                    input.Description,
		OperatingSystem:                os,
		ApprovedPatches:                input.ApprovedPatches,
		RejectedPatches:                input.RejectedPatches,
		ApprovedPatchesComplianceLevel: input.ApprovedPatchesComplianceLevel,
		CreatedDate:                    now,
		ModifiedDate:                   now,
	}

	if b.patchBaselines[region] == nil {
		b.patchBaselines[region] = make(map[string]PatchBaseline)
	}
	b.patchBaselinesStore(region)[baselineID] = bl

	if len(input.Tags) > 0 {
		if b.miscResourceTags[region] == nil {
			b.miscResourceTags[region] = make(map[string]map[string]string)
		}
		miscTags := b.miscResourceTagsStore(region)
		if miscTags[baselineID] == nil {
			miscTags[baselineID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			miscTags[baselineID][t.Key] = t.Value
		}
	}

	return &CreatePatchBaselineOutput{BaselineID: baselineID}, nil
}

// AccountID returns the mocked AWS account ID used by this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// Region returns the mocked AWS region used by this backend.
func (b *InMemoryBackend) Region() string { return defaultRegion }
