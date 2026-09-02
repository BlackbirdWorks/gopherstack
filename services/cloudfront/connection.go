package cloudfront

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// connectionGroupARN builds an ARN for a connection group.
func (b *InMemoryBackend) connectionGroupARN(id string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("connection-group/%s", id))
}

// connectionFunctionARN builds an ARN for a connection function.
func (b *InMemoryBackend) connectionFunctionARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:connection-function/%s", b.accountID, id)
}

// defaultConnectionFunctionRuntime is used when a caller does not specify a runtime
// (mirrors CloudFront Functions' current default runtime).
const defaultConnectionFunctionRuntime = "cloudfront-js-2.0"

// CreateConnectionFunction creates a new connection function with the default JS runtime and
// no code. It is a convenience wrapper around CreateConnectionFunctionWithCode for callers
// (and tests) that only supply a name and comment. AWS allows multiple connection functions to
// share the same Name — they are keyed and uniqued by ID, not by name.
func (b *InMemoryBackend) CreateConnectionFunction(name, comment string) (*ConnectionFunction, error) {
	return b.CreateConnectionFunctionWithCode(name, comment, "", nil, nil)
}

// CreateConnectionFunctionWithCode creates a new connection function using the full
// CreateConnectionFunction request shape: name, comment, runtime, function code, and tags.
func (b *InMemoryBackend) CreateConnectionFunctionWithCode(
	name, comment, runtime string, code []byte, tags map[string]string,
) (*ConnectionFunction, error) {
	if runtime == "" {
		runtime = defaultConnectionFunctionRuntime
	}
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}
	if err := validateCFTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateConnectionFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	id := generateID()
	now := time.Now().UTC().Format(time.RFC3339)
	fn := &ConnectionFunction{
		ID:               id,
		ARN:              b.connectionFunctionARN(id),
		Name:             name,
		Comment:          comment,
		Runtime:          runtime,
		FunctionCode:     append([]byte(nil), code...),
		Stage:            functionStageDevelopment,
		Status:           statusDeployed,
		ETag:             uuid.NewString(),
		CreatedTime:      now,
		LastModifiedTime: now,
		Tags:             make(map[string]string, len(tags)),
	}
	maps.Copy(fn.Tags, tags)
	b.connectionFunctions.Put(fn)
	b.connectionFunctionARNs[fn.ARN] = id
	b.connectionFunctionByName[name] = id

	return b.copyConnectionFunction(fn), nil
}

// CreateConnectionGroup creates a new connection group with default IPv6-enabled, enabled
// settings and no Anycast IP list. It is a convenience wrapper around
// CreateConnectionGroupWithConfig for callers (and tests) that only supply a name and comment.
func (b *InMemoryBackend) CreateConnectionGroup(name, comment string) (*ConnectionGroup, error) {
	return b.CreateConnectionGroupWithConfig(name, comment, "", true, true, nil)
}

// CreateConnectionGroupWithConfig creates a new connection group using the full
// CreateConnectionGroup request shape: name, comment, Anycast IP list ID, IPv6/enabled flags,
// and tags. Name must be unique among existing connection groups. A routing endpoint domain
// name is generated and indexed for GetConnectionGroupByRoutingEndpoint lookups.
func (b *InMemoryBackend) CreateConnectionGroupWithConfig(
	name, comment, anycastIPListID string, ipv6Enabled, enabled bool, tags map[string]string,
) (*ConnectionGroup, error) {
	if err := validateCFTags(tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateConnectionGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.connectionGroupByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: connection group with name %q already exists",
			ErrConnectionGroupAlreadyExists,
			name,
		)
	}

	id := generateID()
	now := time.Now().UTC().Format(time.RFC3339)
	group := &ConnectionGroup{
		ID:               id,
		ARN:              b.connectionGroupARN(id),
		Name:             name,
		Comment:          comment,
		AnycastIPListID:  anycastIPListID,
		RoutingEndpoint:  strings.ToLower(id) + ".cloudfront.net",
		Status:           statusDeployed,
		ETag:             uuid.NewString(),
		CreatedTime:      now,
		LastModifiedTime: now,
		IPv6Enabled:      ipv6Enabled,
		Enabled:          enabled,
		Tags:             make(map[string]string, len(tags)),
	}
	maps.Copy(group.Tags, tags)
	b.connectionGroups.Put(group)
	b.connectionGroupARNs[group.ARN] = id
	b.connectionGroupByName[name] = id
	b.connectionGroupByRoutingEndpoint[group.RoutingEndpoint] = id

	return b.copyConnectionGroup(group), nil
}

// percentageRange bounds the deterministic ComputeUtilization value TestConnectionFunction
// derives from a function's code size and input size, keeping it in a percentage-like range.
const percentageRange = 100

// ---------------------------------------------------------------------------
// Error sentinels
// ---------------------------------------------------------------------------

// copyConnectionGroup returns a deep copy of a ConnectionGroup. Must be called with the lock held.
func (b *InMemoryBackend) copyConnectionGroup(cg *ConnectionGroup) *ConnectionGroup {
	cp := *cg
	if cg.Tags != nil {
		cp.Tags = make(map[string]string, len(cg.Tags))
		maps.Copy(cp.Tags, cg.Tags)
	}

	return &cp
}

func (b *InMemoryBackend) GetConnectionGroup(id string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroup")
	defer b.mu.RUnlock()

	cg, ok := b.connectionGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	return b.copyConnectionGroup(cg), nil
}

// GetConnectionGroupByRoutingEndpoint looks up a connection group by its generated routing
// endpoint domain name (e.g. "d111111abcdef8.cloudfront.net"), the O(1) index populated at
// creation time.
func (b *InMemoryBackend) GetConnectionGroupByRoutingEndpoint(endpoint string) (*ConnectionGroup, error) {
	b.mu.RLock("GetConnectionGroupByRoutingEndpoint")
	defer b.mu.RUnlock()

	id, ok := b.connectionGroupByRoutingEndpoint[endpoint]
	if !ok {
		return nil, fmt.Errorf(
			"%w: connection group with routing endpoint %q not found",
			ErrConnectionGroupNotFound,
			endpoint,
		)
	}

	cg, _ := b.connectionGroups.Get(id)

	return b.copyConnectionGroup(cg), nil
}

// ListConnectionGroups returns all connection groups sorted by ID.
func (b *InMemoryBackend) ListConnectionGroups() []*ConnectionGroup {
	b.mu.RLock("ListConnectionGroups")
	defer b.mu.RUnlock()

	out := make([]*ConnectionGroup, 0, b.connectionGroups.Len())
	for _, cg := range b.connectionGroups.All() {
		out = append(out, b.copyConnectionGroup(cg))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateConnectionGroup updates an existing connection group. Comment is set only when
// non-empty (comment is a gopherstack-only convenience field, not part of the real AWS shape).
// anycastIPListID, ipv6Enabled, and enabled are optional (nil pointer means "leave unchanged"),
// matching the real UpdateConnectionGroup request shape where only Id and IfMatch are required.
func (b *InMemoryBackend) UpdateConnectionGroup(
	id, comment string, anycastIPListID *string, ipv6Enabled, enabled *bool,
) (*ConnectionGroup, error) {
	b.mu.Lock("UpdateConnectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.connectionGroups.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	if comment != "" {
		cg.Comment = comment
	}
	if anycastIPListID != nil {
		cg.AnycastIPListID = *anycastIPListID
	}
	if ipv6Enabled != nil {
		cg.IPv6Enabled = *ipv6Enabled
	}
	if enabled != nil {
		cg.Enabled = *enabled
	}

	cg.ETag = uuid.NewString()
	cg.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionGroup(cg), nil
}

// DeleteConnectionGroup deletes a connection group by ID.
func (b *InMemoryBackend) DeleteConnectionGroup(id string) error {
	b.mu.Lock("DeleteConnectionGroup")
	defer b.mu.Unlock()

	cg, ok := b.connectionGroups.Get(id)
	if !ok {
		return fmt.Errorf("%w: connection group %s not found", ErrConnectionGroupNotFound, id)
	}

	delete(b.connectionGroupByName, cg.Name)
	delete(b.connectionGroupByRoutingEndpoint, cg.RoutingEndpoint)
	delete(b.connectionGroupARNs, cg.ARN)
	b.connectionGroups.Delete(id)

	return nil
}

// ---------------------------------------------------------------------------
// ConnectionFunction extra operations (Get/Describe/List/Update/Delete/Publish/Test)
// ---------------------------------------------------------------------------

// copyConnectionFunction returns a deep copy of a ConnectionFunction. Must be called with the
// lock held.
func (b *InMemoryBackend) copyConnectionFunction(fn *ConnectionFunction) *ConnectionFunction {
	cp := *fn
	cp.FunctionCode = append([]byte(nil), fn.FunctionCode...)
	if fn.Tags != nil {
		cp.Tags = make(map[string]string, len(fn.Tags))
		maps.Copy(cp.Tags, fn.Tags)
	}

	return &cp
}

// resolveConnectionFunction returns the function and its UUID key by id or name, mirroring the
// real API's "Identifier" request field which accepts either. Must be called with the lock held.
func (b *InMemoryBackend) resolveConnectionFunction(idOrName string) (*ConnectionFunction, string) {
	if fn, ok := b.connectionFunctions.Get(idOrName); ok {
		return fn, idOrName
	}

	if uuid, ok := b.connectionFunctionByName[idOrName]; ok {
		if fn, fnOK := b.connectionFunctions.Get(uuid); fnOK {
			return fn, uuid
		}
	}

	return nil, ""
}

// GetConnectionFunction returns a connection function looked up by ID or name.
func (b *InMemoryBackend) GetConnectionFunction(idOrName string) (*ConnectionFunction, error) {
	b.mu.RLock("GetConnectionFunction")
	defer b.mu.RUnlock()

	fn, _ := b.resolveConnectionFunction(idOrName)
	if fn == nil {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, idOrName)
	}

	return b.copyConnectionFunction(fn), nil
}

// ListConnectionFunctions returns all connection functions sorted by name, with ID as a
// tiebreaker: names are not unique (CreateConnectionFunctionWithCode), and the Marker
// cursor in handleListConnectionFunctions needs a unique key per item to avoid dropping
// same-named functions that straddle a page boundary.
func (b *InMemoryBackend) ListConnectionFunctions() []*ConnectionFunction {
	b.mu.RLock("ListConnectionFunctions")
	defer b.mu.RUnlock()

	out := make([]*ConnectionFunction, 0, b.connectionFunctions.Len())
	for _, fn := range b.connectionFunctions.All() {
		out = append(out, b.copyConnectionFunction(fn))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}

		return out[i].ID < out[j].ID
	})

	return out
}

// UpdateConnectionFunction updates an existing connection function, looked up by ID or name,
// replacing its comment, runtime, and code (the real UpdateConnectionFunction request requires
// the full ConnectionFunctionConfig and ConnectionFunctionCode, so this is a full replace, not a
// merge). Updating a function resets it to the DEVELOPMENT stage, mirroring CloudFront Functions.
func (b *InMemoryBackend) UpdateConnectionFunction(
	idOrName, comment, runtime string, code []byte,
) (*ConnectionFunction, error) {
	if runtime == "" {
		runtime = defaultConnectionFunctionRuntime
	}
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateConnectionFunction")
	defer b.mu.Unlock()

	fn, _ := b.resolveConnectionFunction(idOrName)
	if fn == nil {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, idOrName)
	}

	fn.Comment = comment
	fn.Runtime = runtime
	fn.FunctionCode = append([]byte(nil), code...)
	fn.Stage = functionStageDevelopment
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionFunction(fn), nil
}

// DeleteConnectionFunction deletes a connection function looked up by ID or name.
func (b *InMemoryBackend) DeleteConnectionFunction(idOrName string) error {
	b.mu.Lock("DeleteConnectionFunction")
	defer b.mu.Unlock()

	fn, id := b.resolveConnectionFunction(idOrName)
	if fn == nil {
		return fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, idOrName)
	}

	delete(b.connectionFunctionARNs, fn.ARN)
	b.connectionFunctions.Delete(id)
	delete(b.connectionFunctionByName, fn.Name)

	return nil
}

// PublishConnectionFunction promotes a connection function (looked up by ID or name) from
// DEVELOPMENT to LIVE, bumping its ETag to reflect the new published version.
func (b *InMemoryBackend) PublishConnectionFunction(idOrName string) (*ConnectionFunction, error) {
	b.mu.Lock("PublishConnectionFunction")
	defer b.mu.Unlock()

	fn, _ := b.resolveConnectionFunction(idOrName)
	if fn == nil {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, idOrName)
	}

	fn.Stage = functionStageLive
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyConnectionFunction(fn), nil
}

// TestConnectionFunction "executes" a connection function (looked up by ID or name) against a
// caller-supplied connection object. Since there is no real JS runtime here, the mock computes a
// deterministic result derived from the stored function code and the input connection object:
// ComputeUtilization scales with code+input size, and FunctionOutput echoes the input event,
// matching real CloudFront's contract that the function receives and can transform the
// connection object.
func (b *InMemoryBackend) TestConnectionFunction(
	idOrName string, connectionObject []byte,
) (*ConnectionFunctionTestResult, error) {
	b.mu.RLock("TestConnectionFunction")
	defer b.mu.RUnlock()

	fn, _ := b.resolveConnectionFunction(idOrName)
	if fn == nil {
		return nil, fmt.Errorf("%w: connection function %s not found", ErrConnectionFunctionNotFound, idOrName)
	}

	// Deterministic utilization derived from the size of the function's code and the input,
	// clamped to a percentage-like range so it is stable for a given function+input pair.
	weight := (len(fn.FunctionCode) + len(connectionObject)) % percentageRange

	return &ConnectionFunctionTestResult{
		ComputeUtilization: strconv.Itoa(weight),
		FunctionOutput:     string(connectionObject),
		ExecutionLogs: []string{
			fmt.Sprintf("Running connection function %s (%s) in stage %s", fn.Name, fn.ID, fn.Stage),
			"Test passed",
		},
	}, nil
}

// ---------------------------------------------------------------------------
// AnycastIPList extra operations (Get/List/Update/Delete)
// ---------------------------------------------------------------------------
