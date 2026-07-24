package cloudcontrol

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	// typeNamePartCount is the number of parts in a valid CloudFormation resource type name (namespace::service::type).
	typeNamePartCount = 3
	// typeNameSplitLimit limits SplitN so that a four-part string is detectable as invalid.
	typeNameSplitLimit = typeNamePartCount + 1
)

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

	if cached := b.cachedEventForToken(clientToken); cached != nil {
		return cached, nil
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
		ResourceModel:   desiredState,
	}
	b.requests.Put(event)
	b.rememberClientToken(clientToken, token)

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

	// store.Table.Range hands back the live pointers held in the backend's
	// map (see pkgs/store's package doc: Table performs no copying, that is
	// the owning backend's job). Every other accessor here (GetResource,
	// Create/Update/DeleteResource's ProgressEvent) already returns a copy so
	// a caller can never mutate backend state without the lock; ListResources
	// previously handed back live *Resource pointers instead, breaking that
	// invariant. Copy on the way out to match.
	out := make([]*Resource, len(pg.Data))
	for i, r := range pg.Data {
		out[i] = copyResource(r)
	}

	return out, pg.Next
}

// ListAllResources returns all resources regardless of type, sorted by TypeName then Identifier.
// This is used by the dashboard only and is not a CloudControl API operation.
func (b *InMemoryBackend) ListAllResources() []*Resource {
	b.mu.RLock("ListAllResources")
	defer b.mu.RUnlock()

	all := b.resources.All()

	sort.Slice(all, func(i, j int) bool {
		if all[i].TypeName != all[j].TypeName {
			return all[i].TypeName < all[j].TypeName
		}

		return all[i].Identifier < all[j].Identifier
	})

	// Copy on the way out -- see the matching comment in ListResources above;
	// b.resources.All() also hands back live internal pointers.
	out := make([]*Resource, len(all))
	for i, r := range all {
		out[i] = copyResource(r)
	}

	return out
}

// DeleteResource removes the resource identified by typeName and identifier.
// An optional clientToken may be supplied for idempotency, matching the real
// DeleteResourceInput.ClientToken field: if the same token is supplied again
// the original ProgressEvent is returned without re-deleting (or erroring on
// an already-deleted resource).
func (b *InMemoryBackend) DeleteResource(typeName, identifier, clientToken string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if cached := b.cachedEventForToken(clientToken); cached != nil {
		return cached, nil
	}

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
	b.rememberClientToken(clientToken, token)

	return copyEvent(event), nil
}

// UpdateResource applies a JSON RFC 6902 patch document to the resource. An
// optional clientToken may be supplied for idempotency, matching the real
// UpdateResourceInput.ClientToken field: if the same token is supplied again
// the original ProgressEvent is returned without re-applying the patch.
func (b *InMemoryBackend) UpdateResource(typeName, identifier, patchDocument, clientToken string) (
	*ProgressEvent, error,
) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)

	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	if cached := b.cachedEventForToken(clientToken); cached != nil {
		return cached, nil
	}

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
		ResourceModel:   r.Properties,
	}
	b.requests.Put(event)
	b.rememberClientToken(clientToken, token)

	return copyEvent(event), nil
}

// cachedEventForToken returns a copy of the ProgressEvent previously recorded
// for clientToken, or nil if clientToken is empty or unrecognized. Callers
// must hold b.mu (read or write) already.
func (b *InMemoryBackend) cachedEventForToken(clientToken string) *ProgressEvent {
	if clientToken == "" {
		return nil
	}

	prevToken, ok := b.clientTokens[clientToken]
	if !ok {
		return nil
	}

	cachedEvent, found := b.requests.Get(prevToken)
	if !found {
		return nil
	}

	return copyEvent(cachedEvent)
}

// rememberClientToken records the mapping from clientToken to requestToken
// for future idempotent replays. A no-op when clientToken is empty. Callers
// must hold b.mu (write) already.
func (b *InMemoryBackend) rememberClientToken(clientToken, requestToken string) {
	if clientToken == "" {
		return
	}

	b.clientTokens[clientToken] = requestToken
}

// copyResource returns a shallow copy of a Resource so callers cannot mutate backend state.
func copyResource(r *Resource) *Resource {
	if r == nil {
		return nil
	}

	cp := *r

	return &cp
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
