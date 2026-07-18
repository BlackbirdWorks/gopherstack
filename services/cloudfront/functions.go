package cloudfront

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// validateRuntime returns ErrValidation when the runtime is not a known CloudFront Function runtime.
func validateRuntime(runtime string) error {
	switch runtime {
	case "cloudfront-js-1.0", "cloudfront-js-2.0":
		return nil
	}

	return fmt.Errorf(
		"%w: Runtime must be one of cloudfront-js-1.0 or cloudfront-js-2.0, got %q",
		ErrValidation, runtime,
	)
}

// functionARN builds an ARN for a CloudFront Function.
func (b *InMemoryBackend) functionARN(name string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("function/%s", name))
}

// CreateFunction creates a new CloudFront Function.
func (b *InMemoryBackend) CreateFunction(
	name, comment, runtime, functionCode string,
) (*Function, error) {
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.functions.Get(name); exists {
		return nil, fmt.Errorf("%w: function with name %q already exists", ErrFunctionAlreadyExists, name)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	fn := &Function{
		Name:             name,
		Comment:          comment,
		Runtime:          runtime,
		FunctionCode:     functionCode,
		Status:           functionStageDevelopment,
		ETag:             uuid.NewString(),
		ARN:              b.functionARN(name),
		CreatedTime:      now,
		LastModifiedTime: now,
	}
	b.functions.Put(fn)
	cp := *fn

	return &cp, nil
}

// GetFunction returns a CloudFront Function by name.
func (b *InMemoryBackend) GetFunction(name string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	cp := *fn

	return &cp, nil
}

// ListFunctions returns all CloudFront Functions sorted by name.
func (b *InMemoryBackend) ListFunctions() []*Function {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	list := make([]*Function, 0, b.functions.Len())
	for _, fn := range b.functions.All() {
		cp := *fn
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// PublishFunction publishes (promotes to LIVE) a CloudFront Function.
func (b *InMemoryBackend) PublishFunction(name string) (*Function, error) {
	b.mu.Lock("PublishFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Status = functionStageLive
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	cp := *fn

	return &cp, nil
}

// UpdateFunction updates an existing CloudFront Function.
func (b *InMemoryBackend) UpdateFunction(
	name, comment, runtime, functionCode string,
) (*Function, error) {
	if err := validateRuntime(runtime); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	fn.Comment = comment
	fn.Runtime = runtime
	fn.FunctionCode = functionCode
	fn.Status = functionStageDevelopment
	fn.ETag = uuid.NewString()
	fn.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	cp := *fn

	return &cp, nil
}

// DeleteFunction deletes a CloudFront Function by name.
func (b *InMemoryBackend) DeleteFunction(name string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(name); !ok {
		return fmt.Errorf("%w: function %s not found", ErrFunctionNotFound, name)
	}

	if b.tokenReferencedByAnyDistribution(b.functionARN(name)) {
		return fmt.Errorf("%w: function %s is associated with a distribution", ErrFunctionInUse, name)
	}

	b.functions.Delete(name)

	return nil
}

// --- Origin Request Policy CRUD ---
