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

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/google/uuid"
)

var (
	ErrParameterNotFound      = errors.New("ParameterNotFound")
	ErrParameterAlreadyExists = errors.New("ParameterAlreadyExists")
	ErrInvalidKeyID           = errors.New("InvalidKeyId")
	ErrCiphertextTooShort     = errors.New("ciphertext too short")
	ErrValidationException    = errors.New("ValidationException")
	ErrDocumentAlreadyExists  = errors.New("DocumentAlreadyExists")
	ErrDocumentNotFound       = errors.New("DocumentNotFound")
	ErrInvalidDocumentVersion = errors.New("InvalidDocumentVersion")
	ErrCommandNotFound        = errors.New("CommandNotFound")
)

const (
	SecureStringType  = "SecureString"
	mockKMSKeyStr     = "gopherstack-mock-kms-key-32byte!"
	maxHistoryResults = 50
	commandExpirySecs = 3600
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

// encryptValue encrypts a value using AES-256 (mock KMS encryption).
func encryptValue(plaintext string) (string, error) {
	block, err := aes.NewCipher([]byte(mockKMSKeyStr))
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, nonceErr := io.ReadFull(rand.Reader, nonce); nonceErr != nil {
		return "", nonceErr
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptValue decrypts a value encrypted with encryptValue.
func decryptValue(ciphertext string) (string, error) {
	block, err := aes.NewCipher([]byte(mockKMSKeyStr))
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertextBytes) < nonceSize {
		return "", ErrCiphertextTooShort
	}

	nonce, ciphertextOnly := ciphertextBytes[:nonceSize], ciphertextBytes[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertextOnly, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// StorageBackend defines the interface for an SSM Parameter Store backend.
type StorageBackend interface {
	PutParameter(input *PutParameterInput) (*PutParameterOutput, error)
	GetParameter(input *GetParameterInput) (*GetParameterOutput, error)
	GetParameters(input *GetParametersInput) (*GetParametersOutput, error)
	DeleteParameter(input *DeleteParameterInput) (*DeleteParameterOutput, error)
	DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error)
	GetParameterHistory(input *GetParameterHistoryInput) (*GetParameterHistoryOutput, error)
	GetParametersByPath(input *GetParametersByPathInput) (*GetParametersByPathOutput, error)
	DescribeParameters(input *DescribeParametersInput) (*DescribeParametersOutput, error)
	AddTagsToResource(input *AddTagsToResourceInput) error
	RemoveTagsFromResource(input *RemoveTagsFromResourceInput) error
	ListTagsForResource(input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error)
	ListAll() []Parameter
	// Document operations.
	CreateDocument(input *CreateDocumentInput) (*CreateDocumentOutput, error)
	GetDocument(input *GetDocumentInput) (*GetDocumentOutput, error)
	DescribeDocument(input *DescribeDocumentInput) (*DescribeDocumentOutput, error)
	ListDocuments(input *ListDocumentsInput) (*ListDocumentsOutput, error)
	UpdateDocument(input *UpdateDocumentInput) (*UpdateDocumentOutput, error)
	DeleteDocument(input *DeleteDocumentInput) (*DeleteDocumentOutput, error)
	DescribeDocumentPermission(input *DescribeDocumentPermissionInput) (*DescribeDocumentPermissionOutput, error)
	ModifyDocumentPermission(input *ModifyDocumentPermissionInput) (*ModifyDocumentPermissionOutput, error)
	ListDocumentVersions(input *ListDocumentVersionsInput) (*ListDocumentVersionsOutput, error)
	// Command operations.
	SendCommand(input *SendCommandInput) (*SendCommandOutput, error)
	ListCommands(input *ListCommandsInput) (*ListCommandsOutput, error)
	GetCommandInvocation(input *GetCommandInvocationInput) (*GetCommandInvocationOutput, error)
	ListCommandInvocations(input *ListCommandInvocationsInput) (*ListCommandInvocationsOutput, error)
}

// InMemoryBackend implements StorageBackend using a concurrency-safe map.
type InMemoryBackend struct {
	parameters          map[string]Parameter
	history             map[string][]ParameterHistory // Stores all versions of each parameter
	tags                map[string]*tags.Tags         // paramName -> tags
	documents           map[string]Document
	documentVersions    map[string][]DocumentVersion // docName -> ordered versions
	documentPermissions map[string][]string          // docName -> account IDs
	commands            map[string]Command
	commandInvocations  map[string][]CommandInvocation // commandId -> invocations
	mu                  *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		parameters:          make(map[string]Parameter),
		history:             make(map[string][]ParameterHistory),
		tags:                make(map[string]*tags.Tags),
		documents:           make(map[string]Document),
		documentVersions:    make(map[string][]DocumentVersion),
		documentPermissions: make(map[string][]string),
		commands:            make(map[string]Command),
		commandInvocations:  make(map[string][]CommandInvocation),
		mu:                  lockmetrics.New("ssm"),
	}

	b.registerDefaultDocuments()

	return b
}

// PutParameter creates or updates a parameter.
func (b *InMemoryBackend) PutParameter(input *PutParameterInput) (*PutParameterOutput, error) {
	if err := validateParameterName(input.Name); err != nil {
		return nil, err
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
		Parameters:        make([]Parameter, 0),
		InvalidParameters: make([]string, 0),
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

	return &DeleteParameterOutput{}, nil
}

// DeleteParameters deletes multiple parameters.
func (b *InMemoryBackend) DeleteParameters(input *DeleteParametersInput) (*DeleteParametersOutput, error) {
	b.mu.Lock("DeleteParameters")
	defer b.mu.Unlock()

	output := &DeleteParametersOutput{
		DeletedParameters: make([]string, 0),
		InvalidParameters: make([]string, 0),
	}

	for _, name := range input.Names {
		if _, exists := b.parameters[name]; exists {
			delete(b.parameters, name)
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

	result := make([]Parameter, 0, end-startIdx)

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

// AddTagsToResource adds or updates tags for a parameter.
func (b *InMemoryBackend) AddTagsToResource(input *AddTagsToResourceInput) error {
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

// RemoveTagsFromResource removes tags from a parameter.
func (b *InMemoryBackend) RemoveTagsFromResource(input *RemoveTagsFromResourceInput) error {
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

// ListTagsForResource returns all tags for a parameter.
func (b *InMemoryBackend) ListTagsForResource(input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error) {
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
			DocumentFormat:  "JSON",
			Status:          "Active",
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
				DocumentFormat:   "JSON",
				Status:           "Active",
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
		format = "JSON"
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
		Status:          "Active",
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
			Status:           "Active",
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
		Status:           "Active",
		Content:          input.Content,
	})

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
		Status:            "Success",
		RequestedDateTime: now,
		ExpiresAfter:      now + commandExpirySecs,
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
			Status:            "Success",
			StatusDetails:     "Success",
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

	all := make([]CommandInvocation, 0)
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

	b.parameters = make(map[string]Parameter)
	b.history = make(map[string][]ParameterHistory)
	b.tags = make(map[string]*tags.Tags)
	b.documents = make(map[string]Document)
	b.documentVersions = make(map[string][]DocumentVersion)
	b.documentPermissions = make(map[string][]string)
	b.commands = make(map[string]Command)
	b.commandInvocations = make(map[string][]CommandInvocation)
}
