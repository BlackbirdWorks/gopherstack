package cloudcontrol

import (
	"encoding/json"
	"slices"
	"sort"
	"strconv"
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

// RFC 6902 patch operation names applyPatch implements.
const (
	patchOpAdd     = "add"
	patchOpReplace = "replace"
	patchOpRemove  = "remove"
)

// CreateResource creates a new resource of the given type with the given desired state JSON.
// An optional clientToken may be supplied for idempotency: if the same token is supplied
// again with the SAME desiredState, the original ProgressEvent is returned without creating
// a duplicate resource. Supplying the same token with a DIFFERENT typeName/desiredState
// returns ErrClientTokenConflict (real ClientTokenConflictException semantics).
func (b *InMemoryBackend) CreateResource(typeName, desiredState, clientToken string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	identifier := extractIdentifier(desiredState)
	if identifier == "" {
		identifier = uuid.NewString()
	}

	key := resourceKey(typeName, identifier)
	fingerprint := clientTokenFingerprint("CREATE", typeName, "", desiredState)

	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	cached, found, err := b.cachedEventForToken(clientToken, fingerprint)
	if err != nil {
		return nil, err
	}

	if found {
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
	b.rememberClientToken(clientToken, token, fingerprint)

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
// resourceModel, when non-empty, is a JSON object of property name/value pairs (the real
// ListResourcesInput.ResourceModel field -- "The resource model to use to select the
// resources to return"); only resources whose current Properties contain every one of
// those key/value pairs are returned. An unparseable resourceModel matches nothing, same
// as AWS rejecting a malformed selector.
func (b *InMemoryBackend) ListResources(
	typeName string, maxResults int, nextToken, resourceModel string,
) ([]*Resource, string) {
	if !isValidTypeName(typeName) {
		return nil, ""
	}

	var modelFilter map[string]any

	if resourceModel != "" {
		if err := json.Unmarshal([]byte(resourceModel), &modelFilter); err != nil {
			return []*Resource{}, ""
		}
	}

	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	var all []*Resource

	b.resources.Range(func(r *Resource) bool {
		if r.TypeName == typeName && matchesResourceModel(r.Properties, modelFilter) {
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
// DeleteResourceInput.ClientToken field: if the same token is supplied again for the
// SAME typeName/identifier, the original ProgressEvent is returned without re-deleting
// (or erroring on an already-deleted resource). Supplying the same token for a
// DIFFERENT typeName/identifier returns ErrClientTokenConflict.
func (b *InMemoryBackend) DeleteResource(typeName, identifier, clientToken string) (*ProgressEvent, error) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)
	fingerprint := clientTokenFingerprint("DELETE", typeName, identifier, "")

	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	cached, found, err := b.cachedEventForToken(clientToken, fingerprint)
	if err != nil {
		return nil, err
	}

	if found {
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
	b.rememberClientToken(clientToken, token, fingerprint)

	return copyEvent(event), nil
}

// UpdateResource applies a JSON RFC 6902 patch document to the resource. An
// optional clientToken may be supplied for idempotency, matching the real
// UpdateResourceInput.ClientToken field: if the same token is supplied again with the
// SAME typeName/identifier/patchDocument, the original ProgressEvent is returned without
// re-applying the patch. Supplying the same token with a DIFFERENT typeName/identifier/
// patchDocument returns ErrClientTokenConflict.
func (b *InMemoryBackend) UpdateResource(typeName, identifier, patchDocument, clientToken string) (
	*ProgressEvent, error,
) {
	if !isValidTypeName(typeName) {
		return nil, ErrValidation
	}

	key := resourceKey(typeName, identifier)
	fingerprint := clientTokenFingerprint("UPDATE", typeName, identifier, patchDocument)

	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	cached, found, err := b.cachedEventForToken(clientToken, fingerprint)
	if err != nil {
		return nil, err
	}

	if found {
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
	b.rememberClientToken(clientToken, token, fingerprint)

	return copyEvent(event), nil
}

// clientTokenFingerprint builds a stable fingerprint identifying the semantic content of
// a Create/Update/DeleteResource request, used to distinguish an idempotent ClientToken
// replay (same fingerprint) from a genuine ClientTokenConflictException (same token,
// different fingerprint). All four inputs are already exactly what the corresponding
// *Input shape declares for that operation (op name, TypeName, Identifier, and
// DesiredState/PatchDocument as applicable), so no data is fabricated here -- this is
// purely a deterministic encoding of the caller-supplied request.
func clientTokenFingerprint(op, typeName, identifier, payload string) string {
	return op + "\x00" + typeName + "\x00" + identifier + "\x00" + payload
}

// cachedEventForToken returns (event, true, nil) when clientToken was previously used
// with the SAME fingerprint (an idempotent replay); (nil, false, nil) when clientToken is
// empty, unrecognized, or its cached request row has gone missing (nothing to return, no
// error); and (nil, false, ErrClientTokenConflict) when clientToken was previously used
// with a DIFFERENT fingerprint. Callers must hold b.mu (read or write) already.
func (b *InMemoryBackend) cachedEventForToken(clientToken, fingerprint string) (*ProgressEvent, bool, error) {
	if clientToken == "" {
		return nil, false, nil
	}

	entry, ok := b.clientTokens[clientToken]
	if !ok {
		return nil, false, nil
	}

	if entry.Fingerprint != fingerprint {
		return nil, false, ErrClientTokenConflict
	}

	cachedEvent, found := b.requests.Get(entry.RequestToken)
	if !found {
		return nil, false, nil
	}

	return copyEvent(cachedEvent), true, nil
}

// rememberClientToken records the mapping from clientToken to its requestToken and
// request fingerprint for future idempotent replays / conflict detection. A no-op when
// clientToken is empty. Callers must hold b.mu (write) already.
func (b *InMemoryBackend) rememberClientToken(clientToken, requestToken, fingerprint string) {
	if clientToken == "" {
		return
	}

	b.clientTokens[clientToken] = clientTokenEntry{RequestToken: requestToken, Fingerprint: fingerprint}
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

// matchesResourceModel reports whether a resource's Properties JSON document contains
// every key/value pair in modelFilter (using Go's == for scalars and reflect.DeepEqual
// semantics via encoding/json's any-decoding for nested values). A nil/empty modelFilter
// matches everything (no ResourceModel filter supplied). An unparseable Properties
// document matches nothing, since its fields cannot be compared.
func matchesResourceModel(properties string, modelFilter map[string]any) bool {
	if len(modelFilter) == 0 {
		return true
	}

	var props map[string]any
	if err := json.Unmarshal([]byte(properties), &props); err != nil {
		return false
	}

	for k, want := range modelFilter {
		got, exists := props[k]
		if !exists {
			return false
		}

		wantJSON, errW := json.Marshal(want)
		gotJSON, errG := json.Marshal(got)

		if errW != nil || errG != nil || string(wantJSON) != string(gotJSON) {
			return false
		}
	}

	return true
}

// applyPatch applies "add"/"replace"/"remove" operations from an RFC 6902 JSON
// Patch document to a JSON document, resolving each op's Path as a real RFC
// 6901 JSON Pointer -- walking into nested objects and array elements, not
// just top-level fields. "move"/"copy"/"test" are accepted (per real
// UpdateResourceInput.PatchDocument, "a JSON document listing the patch
// operations", api_op_UpdateResource.go) but not applied: they need
// cross-path/value semantics this simplified engine doesn't implement, so
// they are silently skipped rather than guessed at. If the document or patch
// cannot be parsed, the original document is returned unchanged.
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
		segments := splitPointer(op.Path)
		if len(segments) == 0 {
			continue
		}

		switch op.Op {
		case patchOpAdd, patchOpReplace, patchOpRemove:
			doc, _ = applyPointerOp(doc, segments, op.Op, op.Value).(map[string]any)
		}
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return document
	}

	return string(out)
}

// splitPointer decodes an RFC 6901 JSON Pointer into its unescaped reference
// tokens (~1 -> "/", then ~0 -> "~", per the RFC's decoding order). The root
// pointer ("" or "/") yields no segments.
func splitPointer(path string) []string {
	if path == "" || path == "/" {
		return nil
	}

	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, p := range parts {
		p = strings.ReplaceAll(p, "~1", "/")
		parts[i] = strings.ReplaceAll(p, "~0", "~")
	}

	return parts
}

// applyPointerOp resolves segments against node and applies op at the target
// location, returning the (possibly new, for array insert/remove) node. A
// segment that fails to resolve (missing map key, out-of-range array index)
// leaves node unchanged rather than erroring, matching applyPatch's
// fail-closed, best-effort contract.
func applyPointerOp(node any, segments []string, op string, value any) any {
	key := segments[0]
	rest := segments[1:]

	switch n := node.(type) {
	case map[string]any:
		if len(rest) > 0 {
			child, ok := n[key]
			if ok {
				n[key] = applyPointerOp(child, rest, op, value)
			}

			return n
		}

		switch op {
		case patchOpAdd, patchOpReplace:
			n[key] = value
		case patchOpRemove:
			delete(n, key)
		}

		return n
	case []any:
		return applyArrayPointerOp(n, key, rest, op, value)
	default:
		return node
	}
}

// applyArrayPointerOp handles one pointer segment against a JSON array,
// including the RFC 6901 "-" token (end-of-array, "add" only).
func applyArrayPointerOp(n []any, key string, rest []string, op string, value any) any {
	if len(rest) > 0 {
		idx, ok := parseArrayIndex(key)
		if !ok || idx >= len(n) {
			return n
		}

		n[idx] = applyPointerOp(n[idx], rest, op, value)

		return n
	}

	if key == "-" && op == patchOpAdd {
		return append(n, value)
	}

	idx, ok := parseArrayIndex(key)
	if !ok {
		return n
	}

	switch op {
	case patchOpAdd:
		if idx > len(n) {
			return n
		}

		n = append(n, nil)
		copy(n[idx+1:], n[idx:])
		n[idx] = value
	case patchOpReplace:
		if idx >= len(n) {
			return n
		}

		n[idx] = value
	case patchOpRemove:
		if idx >= len(n) {
			return n
		}

		n = append(n[:idx], n[idx+1:]...)
	}

	return n
}

// parseArrayIndex parses an RFC 6901 array reference token as a non-negative
// index. Callers apply the op-specific bound ("add" allows == len(array),
// "replace"/"remove" require < len(array)).
func parseArrayIndex(seg string) (int, bool) {
	idx, err := strconv.Atoi(seg)
	if err != nil || idx < 0 {
		return 0, false
	}

	return idx, true
}
