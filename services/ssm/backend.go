package ssm

import (
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

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	documentFormatJSON = "JSON"
	statusActive       = "Active"
)

var (
	ErrParameterNotFound                  = errors.New("ParameterNotFound")
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
		return fmt.Errorf("%w: parameter name exceeds maximum length of %d", ErrValidationException, maxParamNameLength)
	}

	if strings.Contains(name, "//") {
		return fmt.Errorf("%w: parameter name must not contain double slashes", ErrValidationException)
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

// InMemoryBackend implements StorageBackend using a concurrency-safe map.
type InMemoryBackend struct {
	activations                map[string]Activation
	maintenanceWindows         map[string]MaintenanceWindow
	maintenanceWindowTargets   map[string]MaintenanceWindowTarget
	maintenanceWindowTasks     map[string]MaintenanceWindowTask
	sessions                   map[string]Session
	patchGroupToBaseline       map[string]string
	tags                       map[string]*tags.Tags
	associations               map[string]Association
	documentVersions           map[string][]DocumentVersion
	documentPermissions        map[string][]string
	commands                   map[string]Command
	commandInvocations         map[string][]CommandInvocation
	history                    map[string][]ParameterHistory
	parameters                 map[string]Parameter
	documents                  map[string]Document
	opsItems                   map[string]OpsItem
	opsItemRelatedItems        map[string][]OpsItemRelatedItem
	opsMetadata                map[string]OpsMetadata
	patchBaselines             map[string]PatchBaseline
	inventory                  map[string][]InventoryItem  // key: instanceID
	compliance                 map[string][]ComplianceItem // key: resourceID
	resourceDataSyncs          map[string]*ResourceDataSync
	automationExecutions       map[string]*AutomationExecution // executionID → exec
	serviceSettings            map[string]*ServiceSetting      // settingID → setting
	resourcePolicies           map[string][]*ResourcePolicy    // resourceARN → policies
	executionPreviews          map[string]*ExecutionPreview    // previewID → preview
	mu                         *lockmetrics.RWMutex
	miscResourceTags           map[string]map[string]string
	resourceIDToOpsMetadataArn map[string]string
	commandExpirySecs          float64
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		parameters:                 make(map[string]Parameter),
		history:                    make(map[string][]ParameterHistory),
		tags:                       make(map[string]*tags.Tags),
		documents:                  make(map[string]Document),
		documentVersions:           make(map[string][]DocumentVersion),
		documentPermissions:        make(map[string][]string),
		commands:                   make(map[string]Command),
		commandInvocations:         make(map[string][]CommandInvocation),
		activations:                make(map[string]Activation),
		associations:               make(map[string]Association),
		maintenanceWindows:         make(map[string]MaintenanceWindow),
		maintenanceWindowTargets:   make(map[string]MaintenanceWindowTarget),
		maintenanceWindowTasks:     make(map[string]MaintenanceWindowTask),
		sessions:                   make(map[string]Session),
		patchGroupToBaseline:       make(map[string]string),
		opsItems:                   make(map[string]OpsItem),
		opsItemRelatedItems:        make(map[string][]OpsItemRelatedItem),
		opsMetadata:                make(map[string]OpsMetadata),
		patchBaselines:             make(map[string]PatchBaseline),
		inventory:                  make(map[string][]InventoryItem),
		compliance:                 make(map[string][]ComplianceItem),
		resourceDataSyncs:          make(map[string]*ResourceDataSync),
		automationExecutions:       make(map[string]*AutomationExecution),
		serviceSettings:            make(map[string]*ServiceSetting),
		resourcePolicies:           make(map[string][]*ResourcePolicy),
		executionPreviews:          make(map[string]*ExecutionPreview),
		commandExpirySecs:          defaultCommandExpirySecs,
		mu:                         lockmetrics.New("ssm"),
		resourceIDToOpsMetadataArn: make(map[string]string),
		miscResourceTags:           make(map[string]map[string]string),
	}

	b.registerDefaultDocuments()

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

// PutParameter creates or updates a parameter.
func (b *InMemoryBackend) PutParameter(input *PutParameterInput) (*PutParameterOutput, error) {
	if err := validateParameterName(input.Name); err != nil {
		return nil, err
	}

	// AWS SSM rejects parameter values larger than 4 KiB on the Standard tier
	// (8 KiB on Advanced). gopherstack does not yet model parameter tiers, so
	// enforce the more restrictive Standard limit which is the AWS default.
	const maxParameterValueBytes = 4096
	if len(input.Value) > maxParameterValueBytes {
		return nil, fmt.Errorf("%w: parameter value exceeds %d bytes",
			ErrValidationException, maxParameterValueBytes)
	}

	b.mu.Lock("PutParameter")
	defer b.mu.Unlock()

	existing, exists := b.parameters[input.Name]
	if exists && !input.Overwrite {
		return nil, ErrParameterAlreadyExists
	}

	version := int64(1)
	if exists {
		version = existing.Version + 1
	}

	// Encrypt if SecureString type
	value := input.Value
	if input.Type == SecureStringType {
		encrypted, err := encryptValue(input.Value)
		if err != nil {
			return nil, err
		}
		value = encrypted
	}

	param := Parameter{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Description:      input.Description,
		Version:          version,
		LastModifiedDate: UnixTimeFloat(time.Now()),
	}

	b.parameters[input.Name] = param

	// Store in history (store encrypted value for SecureString)
	paramHistory := ParameterHistory{
		Name:             input.Name,
		Type:             input.Type,
		Value:            value,
		Version:          version,
		LastModifiedDate: param.LastModifiedDate,
		Labels:           []string{}, // Placeholder for labels support in future
	}
	b.history[input.Name] = append(b.history[input.Name], paramHistory)

	// Cap history to the most recent maxHistoryCap entries to prevent unbounded growth.
	if len(b.history[input.Name]) > maxHistoryCap {
		b.history[input.Name] = b.history[input.Name][len(b.history[input.Name])-maxHistoryCap:]
	}

	return &PutParameterOutput{Version: version}, nil
}

// GetParameter retrieves a single parameter.
func (b *InMemoryBackend) GetParameter(input *GetParameterInput) (*GetParameterOutput, error) {
	b.mu.RLock("GetParameter")
	defer b.mu.RUnlock()

	param, exists := b.parameters[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	// Decrypt SecureString if WithDecryption is true
	if input.WithDecryption && param.Type == SecureStringType {
		decrypted, err := decryptValue(param.Value)
		if err != nil {
			// If decryption fails, return the parameter with encrypted value
			return &GetParameterOutput{Parameter: param}, nil
		}
		param.Value = decrypted
	}

	return &GetParameterOutput{Parameter: param}, nil
}

// GetParameters retrieves multiple parameters. Missing names are returned as InvalidParameters.
func (b *InMemoryBackend) GetParameters(input *GetParametersInput) (*GetParametersOutput, error) {
	b.mu.RLock("GetParameters")
	defer b.mu.RUnlock()

	output := &GetParametersOutput{
		Parameters:        make([]Parameter, 0, len(input.Names)),
		InvalidParameters: make([]string, 0, len(input.Names)),
	}

	for _, name := range input.Names {
		if param, exists := b.parameters[name]; exists {
			// Decrypt SecureString if WithDecryption is true
			if input.WithDecryption && param.Type == SecureStringType {
				decrypted, err := decryptValue(param.Value)
				if err != nil {
					// If decryption fails, add to invalid parameters
					output.InvalidParameters = append(output.InvalidParameters, name)

					continue
				}
				param.Value = decrypted
			}
			output.Parameters = append(output.Parameters, param)
		} else {
			output.InvalidParameters = append(output.InvalidParameters, name)
		}
	}

	return output, nil
}

// DeleteParameter deletes a single parameter.
func (b *InMemoryBackend) DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error) {
	b.mu.Lock("DeleteParameter")
	defer b.mu.Unlock()

	if _, exists := b.parameters[input.Name]; !exists {
		return nil, ErrParameterNotFound
	}

	delete(b.parameters, input.Name)
	delete(b.history, input.Name)
	b.tags[input.Name].Close()
	delete(b.tags, input.Name)

	return &DeleteParameterOutput{}, nil
}

// DeleteParameters deletes multiple parameters.
func (b *InMemoryBackend) DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error) {
	b.mu.Lock("DeleteParameters")
	defer b.mu.Unlock()

	output := &DeleteParametersOutput{
		DeletedParameters: make([]string, 0, len(input.Names)),
		InvalidParameters: make([]string, 0, len(input.Names)),
	}

	for _, name := range input.Names {
		if _, exists := b.parameters[name]; exists {
			delete(b.parameters, name)
			delete(b.history, name)
			b.tags[name].Close()
			delete(b.tags, name)
			output.DeletedParameters = append(output.DeletedParameters, name)
		} else {
			output.InvalidParameters = append(output.InvalidParameters, name)
		}
	}

	return output, nil
}

// GetParameterHistory retrieves all versions of a parameter.
func (b *InMemoryBackend) GetParameterHistory(input *GetParameterHistoryInput) (*GetParameterHistoryOutput, error) {
	b.mu.RLock("GetParameterHistory")
	defer b.mu.RUnlock()

	historyList, exists := b.history[input.Name]
	if !exists {
		return nil, ErrParameterNotFound
	}

	// Default max results to 50
	maxResults := int64(maxHistoryResults)
	if input.MaxResults != nil && *input.MaxResults > 0 && *input.MaxResults < 50 {
		maxResults = *input.MaxResults
	}

	// For simplicity, we'll return results in reverse order (latest first)
	// In a real implementation, NextToken would handle pagination properly
	output := &GetParameterHistoryOutput{
		Parameters: make([]ParameterHistory, 0),
	}

	// Return in reverse order (newest first)
	for i := len(historyList) - 1; i >= 0 && int64(len(output.Parameters)) < maxResults; i-- {
		output.Parameters = append(output.Parameters, historyList[i])
	}

	return output, nil
}

// ListAll returns all parameters sorted by name (useful for Dashboard UI).
func (b *InMemoryBackend) ListAll() []Parameter {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()

	params := make([]Parameter, 0, len(b.parameters))
	for _, p := range b.parameters {
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

// paramMatchesPath checks if a parameter name matches the given path prefix.
// If recursive is false, only direct children are matched (no nested paths).
func paramMatchesPath(name, path string, recursive bool) bool {
	if !strings.HasPrefix(name, path) {
		return false
	}
	if recursive {
		return true
	}
	suffix := name[len(path):]

	return !strings.Contains(suffix, "/")
}

// GetParametersByPath returns parameters whose names begin with the given path.
func (b *InMemoryBackend) GetParametersByPath(input *GetParametersByPathInput) (*GetParametersByPathOutput, error) {
	b.mu.RLock("GetParametersByPath")
	defer b.mu.RUnlock()

	// Normalize path to end with /
	path := input.Path
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	// Collect matching parameters
	var matched []Parameter

	for name, param := range b.parameters {
		if paramMatchesPath(name, path, input.Recursive) {
			matched = append(matched, param)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].Name < matched[j].Name
	})

	startIdx := parseNextToken(input.NextToken)

	maxResults := int64(defaultPathMaxResults)
	if input.MaxResults != nil && *input.MaxResults > 0 {
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

	// No capacity hint — user-derived values like (end-startIdx) in the
	// capacity slot trigger CodeQL.
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	result := make([]Parameter, 0)

	for _, p := range matched[startIdx:end] {
		if input.WithDecryption && p.Type == SecureStringType {
			if decrypted, err := decryptValue(p.Value); err == nil {
				p.Value = decrypted
			}
		}

		result = append(result, p)
	}

	return &GetParametersByPathOutput{
		Parameters: result,
		NextToken:  nextToken,
	}, nil
}

// DescribeParameters returns metadata for all parameters (no values).
func (b *InMemoryBackend) DescribeParameters(input *DescribeParametersInput) (*DescribeParametersOutput, error) {
	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	all := make([]ParameterMetadata, 0, len(b.parameters))

	for _, p := range b.parameters {
		all = append(all, ParameterMetadata{
			Name:             p.Name,
			Type:             p.Type,
			Version:          p.Version,
			LastModifiedDate: p.LastModifiedDate,
			Description:      p.Description,
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
	if input.MaxResults != nil && *input.MaxResults > 0 {
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
func paramMatchesFilter(meta ParameterMetadata, f ParameterFilter) bool {
	var fieldValue string

	switch f.Key {
	case "Name":
		fieldValue = meta.Name
	case "Type":
		fieldValue = meta.Type
	default:
		return true // unknown keys are ignored
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
func (b *InMemoryBackend) AddTagsToResource(input *AddTagsToResourceInput) error {
	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("AddTagsToResource")
		defer b.mu.Unlock()

		name := input.ResourceID
		if _, ok := b.parameters[name]; !ok {
			return ErrParameterNotFound
		}
		if b.tags[name] == nil {
			b.tags[name] = tags.New("ssm." + name + ".tags")
		}
		for _, t := range input.Tags {
			b.tags[name].Set(t.Key, t.Value)
		}

		return nil
	}

	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	if b.miscResourceTags[input.ResourceID] == nil {
		b.miscResourceTags[input.ResourceID] = make(map[string]string)
	}
	for _, t := range input.Tags {
		b.miscResourceTags[input.ResourceID][t.Key] = t.Value
	}

	return nil
}

// RemoveTagsFromResource removes tags from a resource.
func (b *InMemoryBackend) RemoveTagsFromResource(input *RemoveTagsFromResourceInput) error {
	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.Lock("RemoveTagsFromResource")
		defer b.mu.Unlock()

		name := input.ResourceID
		if _, ok := b.parameters[name]; !ok {
			return ErrParameterNotFound
		}
		if b.tags[name] != nil {
			b.tags[name].DeleteKeys(input.TagKeys)
		}

		return nil
	}

	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	if rt := b.miscResourceTags[input.ResourceID]; rt != nil {
		for _, k := range input.TagKeys {
			delete(rt, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error) {
	if input.ResourceType == resourceTypeParameter || input.ResourceType == "" {
		b.mu.RLock("ListTagsForResource")
		defer b.mu.RUnlock()

		name := input.ResourceID
		if _, ok := b.parameters[name]; !ok {
			return nil, ErrParameterNotFound
		}
		var tagList []Tag
		if b.tags[name] != nil {
			for k, v := range b.tags[name].Clone() {
				tagList = append(tagList, Tag{Key: k, Value: v})
			}
		}
		sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

		return &ListTagsForResourceOutput{TagList: tagList}, nil
	}

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	var tagList []Tag
	for k, v := range b.miscResourceTags[input.ResourceID] {
		tagList = append(tagList, Tag{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return &ListTagsForResourceOutput{TagList: tagList}, nil
}

// registerDefaultDocuments pre-registers the built-in AWS documents.
func (b *InMemoryBackend) registerDefaultDocuments() {
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
		b.documents[d.name] = doc
		b.documentVersions[d.name] = []DocumentVersion{
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
func (b *InMemoryBackend) CreateDocument(input *CreateDocumentInput) (*CreateDocumentOutput, error) {
	b.mu.Lock("CreateDocument")
	defer b.mu.Unlock()

	if _, exists := b.documents[input.Name]; exists {
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
	}

	b.documents[input.Name] = doc
	b.documentVersions[input.Name] = []DocumentVersion{
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
func (b *InMemoryBackend) GetDocument(input *GetDocumentInput) (*GetDocumentOutput, error) {
	b.mu.RLock("GetDocument")
	defer b.mu.RUnlock()

	doc, exists := b.documents[input.Name]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	content := doc.Content
	version := doc.DocumentVersion

	if input.DocumentVersion != "" && input.DocumentVersion != "$LATEST" && input.DocumentVersion != "$DEFAULT" {
		versions := b.documentVersions[input.Name]
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

// DescribeDocument returns document metadata.
func (b *InMemoryBackend) DescribeDocument(input *DescribeDocumentInput) (*DescribeDocumentOutput, error) {
	b.mu.RLock("DescribeDocument")
	defer b.mu.RUnlock()

	doc, exists := b.documents[input.Name]
	if !exists {
		return nil, ErrDocumentNotFound
	}

	return &DescribeDocumentOutput{Document: doc}, nil
}

// ListDocuments returns a list of document identifiers.
func (b *InMemoryBackend) ListDocuments(input *ListDocumentsInput) (*ListDocumentsOutput, error) {
	b.mu.RLock("ListDocuments")
	defer b.mu.RUnlock()

	all := make([]DocumentIdentifier, 0, len(b.documents))
	for _, doc := range b.documents {
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
func (b *InMemoryBackend) UpdateDocument(input *UpdateDocumentInput) (*UpdateDocumentOutput, error) {
	b.mu.Lock("UpdateDocument")
	defer b.mu.Unlock()

	doc, exists := b.documents[input.Name]
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
	b.documents[input.Name] = doc

	b.documentVersions[input.Name] = append(b.documentVersions[input.Name], DocumentVersion{
		Name:             input.Name,
		DocumentVersion:  newVer,
		CreatedDate:      now,
		IsDefaultVersion: false,
		DocumentFormat:   format,
		Status:           statusActive,
		Content:          input.Content,
	})

	if len(b.documentVersions[input.Name]) > maxDocumentVersionCap {
		vers := b.documentVersions[input.Name]
		b.documentVersions[input.Name] = vers[len(vers)-maxDocumentVersionCap:]
	}

	return &UpdateDocumentOutput{DocumentDescription: doc}, nil
}

// DeleteDocument removes a document and all its versions and permissions.
func (b *InMemoryBackend) DeleteDocument(input *DeleteDocumentInput) (*DeleteDocumentOutput, error) {
	b.mu.Lock("DeleteDocument")
	defer b.mu.Unlock()

	if _, exists := b.documents[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	delete(b.documents, input.Name)
	delete(b.documentVersions, input.Name)
	delete(b.documentPermissions, input.Name)

	return &DeleteDocumentOutput{}, nil
}

// DescribeDocumentPermission returns the sharing permissions for a document.
func (b *InMemoryBackend) DescribeDocumentPermission(
	input *DescribeDocumentPermissionInput,
) (*DescribeDocumentPermissionOutput, error) {
	b.mu.RLock("DescribeDocumentPermission")
	defer b.mu.RUnlock()

	if _, exists := b.documents[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	accountIDs := b.documentPermissions[input.Name]
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
	input *ModifyDocumentPermissionInput,
) (*ModifyDocumentPermissionOutput, error) {
	b.mu.Lock("ModifyDocumentPermission")
	defer b.mu.Unlock()

	if _, exists := b.documents[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	current := b.documentPermissions[input.Name]

	for _, id := range input.AccountIDsToAdd {
		if !slices.Contains(current, id) {
			current = append(current, id)
		}
	}

	for _, id := range input.AccountIDsToRemove {
		current = slices.DeleteFunc(current, func(v string) bool { return v == id })
	}

	b.documentPermissions[input.Name] = current

	return &ModifyDocumentPermissionOutput{}, nil
}

// ListDocumentVersions returns all versions of a document.
func (b *InMemoryBackend) ListDocumentVersions(input *ListDocumentVersionsInput) (*ListDocumentVersionsOutput, error) {
	b.mu.RLock("ListDocumentVersions")
	defer b.mu.RUnlock()

	if _, exists := b.documents[input.Name]; !exists {
		return nil, ErrDocumentNotFound
	}

	versions := b.documentVersions[input.Name]

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
func (b *InMemoryBackend) SendCommand(input *SendCommandInput) (*SendCommandOutput, error) {
	b.mu.Lock("SendCommand")
	defer b.mu.Unlock()

	if _, exists := b.documents[input.DocumentName]; !exists {
		return nil, ErrDocumentNotFound
	}

	now := UnixTimeFloat(time.Now())
	cmdID := uuid.NewString()

	cmd := Command{
		CommandID:         cmdID,
		DocumentName:      input.DocumentName,
		Parameters:        input.Parameters,
		Status:            commandStatusSuccess,
		RequestedDateTime: now,
		ExpiresAfter:      now + b.commandExpirySecs,
		InstanceIDs:       input.InstanceIDs,
		Targets:           input.Targets,
		Comment:           input.Comment,
	}

	b.commands[cmdID] = cmd

	invocations := make([]CommandInvocation, 0, len(input.InstanceIDs))
	for _, instanceID := range input.InstanceIDs {
		inv := CommandInvocation{
			CommandID:         cmdID,
			InstanceID:        instanceID,
			DocumentName:      input.DocumentName,
			Status:            commandStatusSuccess,
			StatusDetails:     commandStatusSuccess,
			RequestedDateTime: now,
		}
		invocations = append(invocations, inv)
	}
	b.commandInvocations[cmdID] = invocations

	return &SendCommandOutput{Command: cmd}, nil
}

// ListCommands returns recorded commands.
func (b *InMemoryBackend) ListCommands(input *ListCommandsInput) (*ListCommandsOutput, error) {
	b.mu.RLock("ListCommands")
	defer b.mu.RUnlock()

	all := make([]Command, 0, len(b.commands))
	for _, cmd := range b.commands {
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
func (b *InMemoryBackend) GetCommandInvocation(input *GetCommandInvocationInput) (*GetCommandInvocationOutput, error) {
	b.mu.RLock("GetCommandInvocation")
	defer b.mu.RUnlock()

	if _, exists := b.commands[input.CommandID]; !exists {
		return nil, ErrCommandNotFound
	}

	for _, inv := range b.commandInvocations[input.CommandID] {
		if inv.InstanceID == input.InstanceID {
			return &GetCommandInvocationOutput{
				CommandID:     input.CommandID,
				InstanceID:    input.InstanceID,
				DocumentName:  inv.DocumentName,
				Status:        inv.Status,
				StatusDetails: inv.StatusDetails,
			}, nil
		}
	}

	return nil, ErrCommandNotFound
}

// ListCommandInvocations returns invocations for a given command.
func (b *InMemoryBackend) ListCommandInvocations(
	input *ListCommandInvocationsInput,
) (*ListCommandInvocationsOutput, error) {
	b.mu.RLock("ListCommandInvocations")
	defer b.mu.RUnlock()

	all := make([]CommandInvocation, 0, len(b.commandInvocations))
	for cmdID, invs := range b.commandInvocations {
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

	for _, t := range b.tags {
		t.Close()
	}

	b.parameters = make(map[string]Parameter)
	b.history = make(map[string][]ParameterHistory)
	b.tags = make(map[string]*tags.Tags)
	b.documents = make(map[string]Document)
	b.documentVersions = make(map[string][]DocumentVersion)
	b.documentPermissions = make(map[string][]string)
	b.commands = make(map[string]Command)
	b.commandInvocations = make(map[string][]CommandInvocation)
	b.activations = make(map[string]Activation)
	b.associations = make(map[string]Association)
	b.maintenanceWindows = make(map[string]MaintenanceWindow)
	b.maintenanceWindowTargets = make(map[string]MaintenanceWindowTarget)
	b.maintenanceWindowTasks = make(map[string]MaintenanceWindowTask)
	b.sessions = make(map[string]Session)
	b.patchGroupToBaseline = make(map[string]string)
	b.opsItems = make(map[string]OpsItem)
	b.opsItemRelatedItems = make(map[string][]OpsItemRelatedItem)
	b.opsMetadata = make(map[string]OpsMetadata)
	b.patchBaselines = make(map[string]PatchBaseline)
	b.resourceIDToOpsMetadataArn = make(map[string]string)
	b.miscResourceTags = make(map[string]map[string]string)
	b.resourceDataSyncs = make(map[string]*ResourceDataSync)
	b.automationExecutions = make(map[string]*AutomationExecution)
	b.serviceSettings = make(map[string]*ServiceSetting)
	b.resourcePolicies = make(map[string][]*ResourcePolicy)
	b.executionPreviews = make(map[string]*ExecutionPreview)
	b.registerDefaultDocuments()
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
	commandStatusCancelled = "Cancelled"
	commandStatusSuccess   = "Success"
	assocStatusSuccess     = "Success"
	faultClient            = "Client"
	opsItemStatusOpen      = "Open"
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
func (b *InMemoryBackend) CancelCommand(input *CancelCommandInput) (*CancelCommandOutput, error) {
	b.mu.Lock("CancelCommand")
	defer b.mu.Unlock()

	cmd, exists := b.commands[input.CommandID]
	if !exists {
		return nil, ErrCommandNotFound
	}

	cmd.Status = commandStatusCancelled
	b.commands[input.CommandID] = cmd

	invs := b.commandInvocations[input.CommandID]
	for i := range invs {
		invs[i].Status = commandStatusCancelled
		invs[i].StatusDetails = commandStatusCancelled
	}
	b.commandInvocations[input.CommandID] = invs

	return &CancelCommandOutput{}, nil
}

// CancelMaintenanceWindowExecution cancels a maintenance window execution.
func (b *InMemoryBackend) CancelMaintenanceWindowExecution(
	input *CancelMaintenanceWindowExecutionInput,
) (*CancelMaintenanceWindowExecutionOutput, error) {
	return &CancelMaintenanceWindowExecutionOutput{
		WindowExecutionID: input.WindowExecutionID,
	}, nil
}

// CreateActivation creates a new activation for managed instances.
func (b *InMemoryBackend) CreateActivation(input *CreateActivationInput) (*CreateActivationOutput, error) {
	if input.IamRole == "" {
		return nil, fmt.Errorf("%w: IamRole is required", ErrValidationException)
	}

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

	b.activations[activationID] = act

	if len(input.Tags) > 0 {
		if b.miscResourceTags[activationID] == nil {
			b.miscResourceTags[activationID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			b.miscResourceTags[activationID][t.Key] = t.Value
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
func (b *InMemoryBackend) CreateAssociation(input *CreateAssociationInput) (*CreateAssociationOutput, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidationException)
	}

	b.mu.Lock("CreateAssociation")
	defer b.mu.Unlock()

	if _, exists := b.documents[input.Name]; !exists {
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

	b.associations[assocID] = assoc

	return &CreateAssociationOutput{AssociationDescription: assoc}, nil
}

// CreateAssociationBatch creates multiple associations in a batch.
func (b *InMemoryBackend) CreateAssociationBatch(
	input *CreateAssociationBatchInput,
) (*CreateAssociationBatchOutput, error) {
	b.mu.Lock("CreateAssociationBatch")
	defer b.mu.Unlock()

	output := &CreateAssociationBatchOutput{
		Successful: make([]Association, 0, len(input.Entries)),
		Failed:     make([]FailedCreateAssociation, 0, len(input.Entries)),
	}

	now := UnixTimeFloat(time.Now())

	for _, entry := range input.Entries {
		if _, exists := b.documents[entry.Name]; !exists {
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

		b.associations[assocID] = assoc
		output.Successful = append(output.Successful, assoc)
	}

	return output, nil
}

// CreateMaintenanceWindow creates a new maintenance window.
func (b *InMemoryBackend) CreateMaintenanceWindow(
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
		return nil, fmt.Errorf("%w: Duration must be between 1 and 24 hours", ErrValidationException)
	}
	if input.Cutoff >= input.Duration {
		return nil, fmt.Errorf("%w: Cutoff must be less than Duration", ErrValidationException)
	}

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

	b.maintenanceWindows[windowID] = mw

	if len(input.Tags) > 0 {
		if b.miscResourceTags[windowID] == nil {
			b.miscResourceTags[windowID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			b.miscResourceTags[windowID][t.Key] = t.Value
		}
	}

	return &CreateMaintenanceWindowOutput{WindowID: windowID}, nil
}

// CreateOpsItem creates a new OpsItem.
func (b *InMemoryBackend) CreateOpsItem(input *CreateOpsItemInput) (*CreateOpsItemOutput, error) {
	if input.Title == "" {
		return nil, fmt.Errorf("%w: Title is required", ErrValidationException)
	}

	if input.Source == "" {
		return nil, fmt.Errorf("%w: Source is required", ErrValidationException)
	}

	b.mu.Lock("CreateOpsItem")
	defer b.mu.Unlock()

	opsItemID := opsItemIDPrefix + uuid.NewString()
	opsItemArn := fmt.Sprintf("arn:aws:ssm:%s:%s:opsitem/%s", defaultRegion, defaultAccountID, opsItemID)
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

	b.opsItems[opsItemID] = item

	if len(input.Tags) > 0 {
		if b.miscResourceTags[opsItemID] == nil {
			b.miscResourceTags[opsItemID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			b.miscResourceTags[opsItemID][t.Key] = t.Value
		}
	}

	return &CreateOpsItemOutput{
		OpsItemID:  opsItemID,
		OpsItemArn: opsItemArn,
	}, nil
}

// AssociateOpsItemRelatedItem associates a related item to an OpsItem.
func (b *InMemoryBackend) AssociateOpsItemRelatedItem(
	input *AssociateOpsItemRelatedItemInput,
) (*AssociateOpsItemRelatedItemOutput, error) {
	b.mu.Lock("AssociateOpsItemRelatedItem")
	defer b.mu.Unlock()

	if _, exists := b.opsItems[input.OpsItemID]; !exists {
		return nil, ErrOpsItemNotFound
	}

	assocID := uuid.NewString()
	related := OpsItemRelatedItem{
		AssociationID:   assocID,
		AssociationType: input.AssociationType,
		ResourceType:    input.ResourceType,
		ResourceURI:     input.ResourceURI,
	}

	b.opsItemRelatedItems[input.OpsItemID] = append(b.opsItemRelatedItems[input.OpsItemID], related)

	return &AssociateOpsItemRelatedItemOutput{AssociationID: assocID}, nil
}

// CreateOpsMetadata creates OpsMetadata for a resource.
func (b *InMemoryBackend) CreateOpsMetadata(
	input *CreateOpsMetadataInput,
) (*CreateOpsMetadataOutput, error) {
	if input.ResourceID == "" {
		return nil, fmt.Errorf("%w: ResourceId is required", ErrValidationException)
	}

	b.mu.Lock("CreateOpsMetadata")
	defer b.mu.Unlock()

	if _, exists := b.resourceIDToOpsMetadataArn[input.ResourceID]; exists {
		return nil, fmt.Errorf(
			"%w: OpsMetadata already exists for resource %s",
			ErrOpsMetadataAlreadyExists,
			input.ResourceID,
		)
	}

	metaID := uuid.NewString()
	arn := fmt.Sprintf(opsMetadataArnTpl, defaultRegion, defaultAccountID, metaID)
	now := UnixTimeFloat(time.Now())

	meta := OpsMetadata{
		OpsMetadataArn:   arn,
		ResourceID:       input.ResourceID,
		Metadata:         input.Metadata,
		CreationDate:     now,
		LastModifiedDate: now,
	}

	b.opsMetadata[arn] = meta
	b.resourceIDToOpsMetadataArn[input.ResourceID] = arn

	return &CreateOpsMetadataOutput{OpsMetadataArn: arn}, nil
}

// CreatePatchBaseline creates a new patch baseline.
func (b *InMemoryBackend) CreatePatchBaseline(
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

	b.patchBaselines[baselineID] = bl

	if len(input.Tags) > 0 {
		if b.miscResourceTags[baselineID] == nil {
			b.miscResourceTags[baselineID] = make(map[string]string)
		}
		for _, t := range input.Tags {
			b.miscResourceTags[baselineID][t.Key] = t.Value
		}
	}

	return &CreatePatchBaselineOutput{BaselineID: baselineID}, nil
}

// AccountID returns the mocked AWS account ID used by this backend.
func (b *InMemoryBackend) AccountID() string { return defaultAccountID }

// Region returns the mocked AWS region used by this backend.
func (b *InMemoryBackend) Region() string { return defaultRegion }
