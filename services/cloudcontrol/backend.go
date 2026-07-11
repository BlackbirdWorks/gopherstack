// Package cloudcontrol provides an in-memory implementation of the AWS CloudControl API service.
package cloudcontrol

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	opStatusSuccess        = "SUCCESS"
	opStatusCancelComplete = "CANCEL_COMPLETE"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrConflict)
	// ErrValidation is returned when a required field is missing or has an invalid value.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

const (
	// defaultListMaxResults is the default page size for list operations.
	defaultListMaxResults = 100
	// typeNamePartCount is the number of parts in a valid CloudFormation resource type name (namespace::service::type).
	typeNamePartCount = 3
	// typeNameSplitLimit limits SplitN so that a four-part string is detectable as invalid.
	typeNameSplitLimit = typeNamePartCount + 1
)

// validOperations is the set of valid CloudControl operation strings.
//
//nolint:gochecknoglobals // lookup set
var validOperations = map[string]struct{}{
	"CREATE": {},
	"DELETE": {},
	"UPDATE": {},
}

// validOperationStatuses is the set of valid CloudControl operation status strings.
//
//nolint:gochecknoglobals // lookup set
var validOperationStatuses = map[string]struct{}{
	"PENDING":              {},
	"IN_PROGRESS":          {},
	opStatusSuccess:        {},
	"FAILED":               {},
	"CANCEL_IN_PROGRESS":   {},
	opStatusCancelComplete: {},
}

// unixEpochTime wraps [time.Time] and marshals to/from a JSON number (Unix seconds),
// which is the format expected by the AWS CloudControl SDK v2 client.
type unixEpochTime struct {
	time.Time
}

func (t unixEpochTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *unixEpochTime) UnmarshalJSON(b []byte) error {
	var epoch int64
	if err := json.Unmarshal(b, &epoch); err != nil {
		return err
	}

	t.Time = time.Unix(epoch, 0)

	return nil
}

// Resource represents an in-memory CloudControl managed resource.
type Resource struct {
	TypeName   string
	Identifier string
	Properties string // JSON string of current properties
}

// ProgressEvent represents the status of a CloudControl resource operation.
type ProgressEvent struct {
	EventTime       unixEpochTime `json:"EventTime"`
	TypeName        string        `json:"TypeName"`
	Identifier      string        `json:"Identifier,omitempty"`
	RequestToken    string        `json:"RequestToken"`
	Operation       string        `json:"Operation"`
	OperationStatus string        `json:"OperationStatus"`
	StatusMessage   string        `json:"StatusMessage,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CloudControl resources.
type InMemoryBackend struct {
	registry     *store.Registry
	resources    *store.Table[Resource]      // key: typeName+"/"+identifier
	requests     *store.Table[ProgressEvent] // key: requestToken
	clientTokens map[string]string           // clientToken → requestToken (idempotency)
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		clientTokens: make(map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("cloudcontrol"),
		registry:     store.NewRegistry(),
	}
	registerAllTables(b)

	return b
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state from the backend, returning it to a clean initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.clientTokens = make(map[string]string)
}

// CreateResource creates a new resource of the given type with the given desired state JSON.
// An optional clientToken may be supplied for idempotency: if the same token is supplied
// again the original ProgressEvent is returned without creating a duplicate resource.
func (b *InMemoryBackend) CreateResource(typeName, desiredState, clientToken string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	identifier := extractIdentifier(desiredState)
	if identifier == "" {
		identifier = uuid.NewString()
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	// Idempotency: if the same clientToken was used before, return the cached event.
	if clientToken != "" {
		if prevToken, ok := b.clientTokens[clientToken]; ok {
			if cachedEvent, found := b.requests.Get(prevToken); found {
				return copyEvent(cachedEvent), nil
			}
		}
	}

	if b.resources.Has(key) {
		return nil, ErrAlreadyExists
	}

	b.resources.Put(&Resource{
		TypeName:   typeName,
		Identifier: identifier,
		Properties: desiredState,
	})

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "CREATE",
		OperationStatus: opStatusSuccess,
	}
	b.requests.Put(event)

	if clientToken != "" {
		b.clientTokens[clientToken] = token
	}

	return copyEvent(event), nil
}

// GetResource returns a copy of the resource identified by typeName and identifier.
func (b *InMemoryBackend) GetResource(typeName, identifier string) (*Resource, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	b.mu.RLock("GetResource")
	defer b.mu.RUnlock()

	r, ok := b.resources.Get(resourceKey(typeName, identifier))
	if !ok {
		return nil, ErrNotFound
	}

	return copyResource(r), nil
}

// ListResources returns a paginated list of resources of the given type, sorted by Identifier.
func (b *InMemoryBackend) ListResources(typeName string, maxResults int, nextToken string) ([]*Resource, string) {
	if !isValidTypeName(typeName) {
		return nil, ""
	}

	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	var all []*Resource

	b.resources.Range(func(r *Resource) bool {
		if r.TypeName == typeName {
			all = append(all, r)
		}

		return true
	})

	sort.Slice(all, func(i, j int) bool {
		return all[i].Identifier < all[j].Identifier
	})

	pg := page.New(all, nextToken, maxResults, defaultListMaxResults)

	return pg.Data, pg.Next
}

// ListAllResources returns all resources regardless of type, sorted by TypeName then Identifier.
// This is used by the dashboard only and is not a CloudControl API operation.
func (b *InMemoryBackend) ListAllResources() []*Resource {
	b.mu.RLock("ListAllResources")
	defer b.mu.RUnlock()

	out := b.resources.All()

	sort.Slice(out, func(i, j int) bool {
		if out[i].TypeName != out[j].TypeName {
			return out[i].TypeName < out[j].TypeName
		}

		return out[i].Identifier < out[j].Identifier
	})

	return out
}

// DeleteResource removes the resource identified by typeName and identifier.
func (b *InMemoryBackend) DeleteResource(typeName, identifier string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if !b.resources.Has(key) {
		return nil, ErrNotFound
	}

	b.resources.Delete(key)

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "DELETE",
		OperationStatus: opStatusSuccess,
	}
	b.requests.Put(event)

	return copyEvent(event), nil
}

// UpdateResource applies a JSON RFC 6902 patch document to the resource.
func (b *InMemoryBackend) UpdateResource(typeName, identifier, patchDocument string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	r, ok := b.resources.Get(key)
	if !ok {
		return nil, ErrNotFound
	}

	// Properties is not part of resources' key (TypeName+Identifier) or any
	// index, so mutating it in place through the pointer returned by Get is
	// safe without a follow-up Put -- same as the original map[string]*Resource
	// behaviour this replaces.
	r.Properties = applyPatch(r.Properties, patchDocument)

	token := uuid.NewString()
	event := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        typeName,
		Identifier:      identifier,
		RequestToken:    token,
		Operation:       "UPDATE",
		OperationStatus: opStatusSuccess,
	}
	b.requests.Put(event)

	return copyEvent(event), nil
}

// GetResourceRequestStatus returns a copy of the ProgressEvent for the given request token.
// Events are retained in the map until Reset() is called.
func (b *InMemoryBackend) GetResourceRequestStatus(requestToken string) (*ProgressEvent, error) {
	b.mu.RLock("GetResourceRequestStatus")
	defer b.mu.RUnlock()

	event, ok := b.requests.Get(requestToken)
	if !ok {
		return nil, ErrNotFound
	}

	return copyEvent(event), nil
}

// CancelResourceRequest cancels the request identified by requestToken.
// Cancelling an already-terminal request (SUCCESS, FAILED, CANCEL_COMPLETE) returns
// ErrValidation to match the UnsupportedActionException AWS returns for terminal requests.
func (b *InMemoryBackend) CancelResourceRequest(requestToken string) (*ProgressEvent, error) {
	b.mu.Lock("CancelResourceRequest")
	defer b.mu.Unlock()

	event, ok := b.requests.Get(requestToken)
	if !ok {
		return nil, ErrNotFound
	}

	if event.OperationStatus != "IN_PROGRESS" {
		return nil, ErrValidation
	}

	cancelled := &ProgressEvent{
		EventTime:       unixEpochTime{time.Now()},
		TypeName:        event.TypeName,
		Identifier:      event.Identifier,
		RequestToken:    requestToken,
		Operation:       event.Operation,
		OperationStatus: opStatusCancelComplete,
	}
	b.requests.Put(cancelled)

	return copyEvent(cancelled), nil
}

// ResourceRequestFilter holds optional filter criteria for ListResourceRequests.
type ResourceRequestFilter struct {
	TypeName          string
	Operations        []string
	OperationStatuses []string
}

// validateFilter returns ErrValidation if the filter contains unknown operation or status strings.
func validateFilter(filter *ResourceRequestFilter) error {
	if filter == nil {
		return nil
	}

	for _, op := range filter.Operations {
		if _, ok := validOperations[op]; !ok {
			return ErrValidation
		}
	}

	for _, st := range filter.OperationStatuses {
		if _, ok := validOperationStatuses[st]; !ok {
			return ErrValidation
		}
	}

	return nil
}

// eventMatchesFilter reports whether event passes the given filter.
// A nil filter matches every event.
func eventMatchesFilter(event *ProgressEvent, filter *ResourceRequestFilter) bool {
	if filter == nil {
		return true
	}

	if len(filter.Operations) > 0 && !slices.Contains(filter.Operations, event.Operation) {
		return false
	}

	if len(filter.OperationStatuses) > 0 && !slices.Contains(filter.OperationStatuses, event.OperationStatus) {
		return false
	}

	if filter.TypeName != "" && event.TypeName != filter.TypeName {
		return false
	}

	return true
}

// ListResourceRequests returns all tracked resource requests, optionally filtered
// by operation type, operation status, and/or resource type name. Results are sorted
// by EventTime descending (most recent first) for deterministic output.
// Returns ErrValidation if the filter contains unknown operation or status strings.
func (b *InMemoryBackend) ListResourceRequests(
	filter *ResourceRequestFilter, maxResults int, nextToken string,
) ([]*ProgressEvent, string, error) {
	if err := validateFilter(filter); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListResourceRequests")
	defer b.mu.RUnlock()

	var out []*ProgressEvent

	b.requests.Range(func(event *ProgressEvent) bool {
		if eventMatchesFilter(event, filter) {
			out = append(out, event)
		}

		return true
	})

	// Sort by EventTime descending so the most-recent request appears first.
	sort.Slice(out, func(i, j int) bool {
		return out[i].EventTime.After(out[j].EventTime.Time)
	})

	pg := page.New(out, nextToken, maxResults, defaultListMaxResults)

	// Deep-copy the page items so callers cannot mutate backend state.
	result := make([]*ProgressEvent, len(pg.Data))
	for i, e := range pg.Data {
		result[i] = copyEvent(e)
	}

	return result, pg.Next, nil
}

// copyEvent returns a shallow copy of a ProgressEvent so callers cannot mutate backend state.
func copyEvent(e *ProgressEvent) *ProgressEvent {
	if e == nil {
		return nil
	}

	cp := *e

	return &cp
}

// copyResource returns a shallow copy of a Resource so callers cannot mutate backend state.
func copyResource(r *Resource) *Resource {
	if r == nil {
		return nil
	}

	cp := *r

	return &cp
}

// AddProgressEvent inserts a ProgressEvent directly into the requests map.
// This is intended for use in tests to set up specific request states that
// cannot be reached through the normal API (e.g. IN_PROGRESS).
func (b *InMemoryBackend) AddProgressEvent(event *ProgressEvent) {
	b.mu.Lock("AddProgressEvent")
	defer b.mu.Unlock()

	b.requests.Put(event)
}

// isValidTypeName reports whether typeName follows the CloudFormation resource type
// name convention: three non-empty parts separated by "::".
// For example: "AWS::S3::Bucket" or "MyCompany::MyService::MyResource".
func isValidTypeName(typeName string) bool {
	if typeName == "" {
		return false
	}

	parts := strings.SplitN(typeName, "::", typeNameSplitLimit)
	if len(parts) != typeNamePartCount {
		return false
	}

	return !slices.Contains(parts, "")
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
//   - "Id"                    — generic identifier (many types)
//   - "Name"                  — generic name (e.g. AWS::IAM::Role)
//   - "LogGroupName"          — AWS::Logs::LogGroup
//   - "BucketName"            — AWS::S3::Bucket
//   - "FunctionName"          — AWS::Lambda::Function
//   - "TopicName"             — AWS::SNS::Topic
//   - "QueueName"             — AWS::SQS::Queue
//   - "TableName"             — AWS::DynamoDB::Table
//   - "RoleName"              — AWS::IAM::Role
//   - "ClusterName"           — AWS::ECS::Cluster
//   - "StreamName"            — AWS::Kinesis::Stream
//   - "DomainName"            — AWS::Route53::HostedZone / AWS::OpenSearchService::Domain
//   - "DBInstanceIdentifier"  — AWS::RDS::DBInstance
//   - "RestApiId"             — AWS::ApiGateway::RestApi
//   - "StackName"             — AWS::CloudFormation::Stack
//   - "KeyId"                 — AWS::KMS::Key
//   - "GroupName"             — AWS::IAM::Group
//   - "UserName"              — AWS::IAM::User
//
//nolint:gochecknoglobals // lookup table
var identifierKeys = []string{
	"Id", "Name", "LogGroupName", "BucketName", "FunctionName", "TopicName", "QueueName",
	"TableName", "RoleName", "ClusterName", "StreamName", "DomainName",
	"DBInstanceIdentifier", "RestApiId", "StackName", "KeyId", "GroupName", "UserName",
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
