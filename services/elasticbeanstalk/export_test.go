package elasticbeanstalk

// ApplicationCount returns the number of applications stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ApplicationCount() int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// EnvironmentCount returns the number of environments stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) EnvironmentCount() int {
	b.mu.RLock("EnvironmentCount")
	defer b.mu.RUnlock()

	return len(b.environments)
}

// AppVersionCount returns the number of application versions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) AppVersionCount() int {
	b.mu.RLock("AppVersionCount")
	defer b.mu.RUnlock()

	return len(b.appVersions)
}

// ConfigTemplateCount returns the number of configuration templates stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ConfigTemplateCount() int {
	b.mu.RLock("ConfigTemplateCount")
	defer b.mu.RUnlock()

	return len(b.configTemplates)
}

// PlatformVersionCount returns the number of platform versions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) PlatformVersionCount() int {
	b.mu.RLock("PlatformVersionCount")
	defer b.mu.RUnlock()

	return len(b.platformVersions)
}

// HandlerOpsLen returns the number of operations registered in the handler's dispatch table.
// Used only in tests.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// AddApplicationInternal seeds an application directly into the backend for testing.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	b.addApplicationInternal(app)
}

// AddEnvironmentInternal seeds an environment directly into the backend for testing.
func (b *InMemoryBackend) AddEnvironmentInternal(env *Environment) {
	b.mu.Lock("AddEnvironmentInternal")
	defer b.mu.Unlock()

	b.addEnvironmentInternal(env)
}

// AddAppVersionInternal seeds an application version directly into the backend for testing.
func (b *InMemoryBackend) AddAppVersionInternal(ver *ApplicationVersion) {
	b.mu.Lock("AddAppVersionInternal")
	defer b.mu.Unlock()

	b.addAppVersionInternal(ver)
}

// AddConfigTemplateInternal seeds a configuration template directly into the backend for testing.
func (b *InMemoryBackend) AddConfigTemplateInternal(tmpl *ConfigurationTemplate) {
	b.mu.Lock("AddConfigTemplateInternal")
	defer b.mu.Unlock()

	b.addConfigTemplateInternal(tmpl)
}

// AddPlatformVersionInternal seeds a platform version directly into the backend for testing.
func (b *InMemoryBackend) AddPlatformVersionInternal(pv *PlatformVersion) {
	b.mu.Lock("AddPlatformVersionInternal")
	defer b.mu.Unlock()

	b.addPlatformVersionInternal(pv)
}
