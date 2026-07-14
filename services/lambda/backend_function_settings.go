package lambda

// GetRuntimeManagementConfig returns the runtime management config for a function.
func (b *InMemoryBackend) GetRuntimeManagementConfig(
	name string,
) (*RuntimeManagementConfig, error) {
	b.mu.RLock("GetRuntimeManagementConfig")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.runtimeManagementConfigs[name]
	if !ok {
		return &RuntimeManagementConfig{UpdateRuntimeOn: "Auto", FunctionArn: fn.FunctionArn}, nil
	}

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// PutRuntimeManagementConfig sets the runtime management config for a function.
func (b *InMemoryBackend) PutRuntimeManagementConfig(
	name string,
	input *PutRuntimeManagementConfigInput,
) (*RuntimeManagementConfig, error) {
	b.mu.Lock("PutRuntimeManagementConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if input.UpdateRuntimeOn == "" {
		return nil, ErrInvalidParameterValue
	}

	cfg := &RuntimeManagementConfig{
		UpdateRuntimeOn:   input.UpdateRuntimeOn,
		RuntimeVersionArn: input.RuntimeVersionArn,
	}
	b.runtimeManagementConfigs[name] = cfg

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// GetFunctionRecursionConfig returns the recursion config for a function.
func (b *InMemoryBackend) GetFunctionRecursionConfig(
	name string,
) (*FunctionRecursionConfig, error) {
	b.mu.RLock("GetFunctionRecursionConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.functionRecursionConfigs[name]
	if !ok {
		return &FunctionRecursionConfig{RecursiveLoop: "Terminate"}, nil
	}

	return cfg, nil
}

// PutFunctionRecursionConfig sets the recursion config for a function.
func (b *InMemoryBackend) PutFunctionRecursionConfig(
	name string,
	input *PutFunctionRecursionConfigInput,
) (*FunctionRecursionConfig, error) {
	b.mu.Lock("PutFunctionRecursionConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	if input.RecursiveLoop == "" {
		return nil, ErrInvalidParameterValue
	}

	cfg := &FunctionRecursionConfig{RecursiveLoop: input.RecursiveLoop}
	b.functionRecursionConfigs[name] = cfg

	return cfg, nil
}

// GetFunctionScalingConfig returns the scaling config for a function.
func (b *InMemoryBackend) GetFunctionScalingConfig(name string) (*FunctionScalingConfig, error) {
	b.mu.RLock("GetFunctionScalingConfig")
	defer b.mu.RUnlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.functionScalingConfigs[name]
	if !ok {
		return &FunctionScalingConfig{FunctionArn: fn.FunctionArn}, nil
	}

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}

// PutFunctionScalingConfig sets the scaling config for a function.
func (b *InMemoryBackend) PutFunctionScalingConfig(
	name string,
	input *PutFunctionScalingConfigInput,
) (*FunctionScalingConfig, error) {
	b.mu.Lock("PutFunctionScalingConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	cfg := &FunctionScalingConfig{MaximumConcurrency: input.MaximumConcurrency}
	b.functionScalingConfigs[name] = cfg

	out := *cfg
	out.FunctionArn = fn.FunctionArn

	return &out, nil
}
