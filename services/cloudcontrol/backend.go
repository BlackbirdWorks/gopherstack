// Package cloudcontrol provides an in-memory implementation of the AWS CloudControl API service.
package cloudcontrol

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
)

// Resource represents an in-memory CloudControl managed resource.
type Resource struct {
	TypeName   string
	Identifier string
	Properties string // JSON string of current properties
}

// ProgressEvent represents the status of a CloudControl resource operation.
type ProgressEvent struct {
	EventTime       time.Time `json:"EventTime"`
	TypeName        string    `json:"TypeName"`
	Identifier      string    `json:"Identifier,omitempty"`
	RequestToken    string    `json:"RequestToken"`
	Operation       string    `json:"Operation"`
	OperationStatus string    `json:"OperationStatus"`
	StatusMessage   string    `json:"StatusMessage,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CloudControl resources.
type InMemoryBackend struct {
	resources map[string]*Resource      // key: typeName+"/"+identifier
	requests  map[string]*ProgressEvent // key: requestToken
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		resources: make(map[string]*Resource),
		requests:  make(map[string]*ProgressEvent),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("cloudcontrol"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateResource creates a new resource of the given type with the given desired state JSON.
func (b *InMemoryBackend) CreateResource(typeName, desiredState string) (*ProgressEvent, error) {
	identifier := extractIdentifier(desiredState)
	if identifier == "" {
		identifier = uuid.NewString()
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	if _, exists := b.resources[key]; exists {
		return nil, ErrAlreadyExists
	}

	b.resources[key] = &Resource{
		TypeName:   typeName,
		Identifier: identifier,
		Properties: desiredState,
	}

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       time.Now(),
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "CREATE",
		OperationStatus: "SUCCESS",
	}
	b.requests[token] = event

	return event, nil
}

// GetResource returns the resource identified by typeName and identifier.
func (b *InMemoryBackend) GetResource(typeName, identifier string) (*Resource, error) {
	b.mu.RLock("GetResource")
	defer b.mu.RUnlock()

	r, ok := b.resources[resourceKey(typeName, identifier)]
	if !ok {
		return nil, ErrNotFound
	}

	return r, nil
}

// ListResources returns all resources of the given type.
func (b *InMemoryBackend) ListResources(typeName string) []*Resource {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	var out []*Resource

	for key, r := range b.resources {
		if strings.HasPrefix(key, typeName+"/") {
			out = append(out, r)
		}
	}

	return out
}

// ListAllResources returns all resources regardless of type (used by dashboard).
func (b *InMemoryBackend) ListAllResources() []*Resource {
	b.mu.RLock("ListAllResources")
	defer b.mu.RUnlock()

	out := make([]*Resource, 0, len(b.resources))

	for _, r := range b.resources {
		out = append(out, r)
	}

	return out
}

// DeleteResource removes the resource identified by typeName and identifier.
func (b *InMemoryBackend) DeleteResource(typeName, identifier string) (*ProgressEvent, error) {
	key := resourceKey(typeName, identifier)

	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if _, ok := b.resources[key]; !ok {
		return nil, ErrNotFound
	}

	delete(b.resources, key)

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       time.Now(),
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "DELETE",
		OperationStatus: "SUCCESS",
	}
	b.requests[token] = event

	return event, nil
}

// UpdateResource applies a JSON RFC 6902 patch document to the resource.
func (b *InMemoryBackend) UpdateResource(typeName, identifier, patchDocument string) (*ProgressEvent, error) {
	key := resourceKey(typeName, identifier)

	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	r, ok := b.resources[key]
	if !ok {
		return nil, ErrNotFound
	}

	r.Properties = applyPatch(r.Properties, patchDocument)

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       time.Now(),
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "UPDATE",
		OperationStatus: "SUCCESS",
	}
	b.requests[token] = event

	return event, nil
}

// GetResourceRequestStatus returns the ProgressEvent for the given request token.
func (b *InMemoryBackend) GetResourceRequestStatus(requestToken string) (*ProgressEvent, error) {
	b.mu.RLock("GetResourceRequestStatus")
	defer b.mu.RUnlock()

	event, ok := b.requests[requestToken]
	if !ok {
		return nil, ErrNotFound
	}

	return event, nil
}

// CancelResourceRequest cancels the request identified by requestToken.
func (b *InMemoryBackend) CancelResourceRequest(requestToken string) (*ProgressEvent, error) {
	b.mu.Lock("CancelResourceRequest")
	defer b.mu.Unlock()

	event, ok := b.requests[requestToken]
	if !ok {
		return nil, ErrNotFound
	}

	cancelled := &ProgressEvent{
		EventTime:       time.Now(),
		TypeName:        event.TypeName,
		Identifier:      event.Identifier,
		RequestToken:    requestToken,
		Operation:       event.Operation,
		OperationStatus: "CANCEL_COMPLETE",
	}
	b.requests[requestToken] = cancelled

	return cancelled, nil
}

// resourceKey returns the map key for a given typeName and identifier.
func resourceKey(typeName, identifier string) string {
	return typeName + "/" + identifier
}

// identifierKeys is the list of JSON property names used to extract a primary
// identifier from a CloudControl desiredState document. Keys are checked in order;
// the first non-empty string value is used as the resource identifier.
//
// Key mappings to common AWS resource types:
//   - "Id"            — generic identifier (many types)
//   - "Name"          — generic name (e.g. AWS::IAM::Role)
//   - "LogGroupName"  — AWS::Logs::LogGroup
//   - "BucketName"    — AWS::S3::Bucket
//   - "FunctionName"  — AWS::Lambda::Function
//   - "TopicName"     — AWS::SNS::Topic
//   - "QueueName"     — AWS::SQS::Queue
//
//nolint:gochecknoglobals // lookup table
var identifierKeys = []string{
	"Id", "Name", "LogGroupName", "BucketName", "FunctionName", "TopicName", "QueueName",
}

// extractIdentifier tries to pull a primary identifier from a JSON desired-state string.
// It checks identifierKeys in order. Returns "" if none found.
func extractIdentifier(desiredState string) string {
	if desiredState == "" {
		return ""
	}

	var props map[string]any
	if err := json.Unmarshal([]byte(desiredState), &props); err != nil {
		return ""
	}

	for _, key := range identifierKeys {
		v, exists := props[key]
		if !exists {
			continue
		}

		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}

	return ""
}

// applyPatch applies a simplified JSON RFC 6902 patch to a JSON document.
// For each "replace" or "add" operation it sets the field; "remove" deletes it.
// If the document or patch cannot be parsed, the original document is returned unchanged.
func applyPatch(document, patchDocument string) string {
	var doc map[string]any
	if err := json.Unmarshal([]byte(document), &doc); err != nil {
		return document
	}

	var ops []struct {
		Value any    `json:"value"`
		Op    string `json:"op"`
		Path  string `json:"path"`
	}

	if err := json.Unmarshal([]byte(patchDocument), &ops); err != nil {
		return document
	}

	for _, op := range ops {
		field := strings.TrimPrefix(op.Path, "/")

		switch op.Op {
		case "replace", "add":
			doc[field] = op.Value
		case "remove":
			delete(doc, field)
		}
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return document
	}

	return string(out)
}
