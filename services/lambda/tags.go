package lambda

import "maps"

// TagResource applies the given tags to the named function, updating FunctionConfiguration.Tags.
func (b *InMemoryBackend) TagResource(functionName string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(functionName)
	if !ok {
		return ErrFunctionNotFound
	}

	fn.Tags = maps.Clone(fn.Tags)
	maps.Copy(fn.Tags, tags)

	return nil
}

// UntagResource removes the specified tag keys from the named function's FunctionConfiguration.Tags.
func (b *InMemoryBackend) UntagResource(functionName string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(functionName)
	if !ok {
		return ErrFunctionNotFound
	}

	fn.Tags = maps.Clone(fn.Tags)
	for _, k := range tagKeys {
		delete(fn.Tags, k)
	}

	return nil
}
