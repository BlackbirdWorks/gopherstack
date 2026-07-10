package elasticbeanstalk

// ApplicationCount returns the total number of applications across all regions.
// Used only in tests.
func (b *InMemoryBackend) ApplicationCount() int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// EnvironmentCount returns the total number of environments across all regions.
// Used only in tests.
func (b *InMemoryBackend) EnvironmentCount() int {
	b.mu.RLock("EnvironmentCount")
	defer b.mu.RUnlock()

	return b.environments.Len()
}

// AppVersionCount returns the total number of application versions across all regions.
// Used only in tests.
func (b *InMemoryBackend) AppVersionCount() int {
	b.mu.RLock("AppVersionCount")
	defer b.mu.RUnlock()

	return b.appVersions.Len()
}

// ConfigTemplateCount returns the total number of configuration templates across all regions.
// Used only in tests.
func (b *InMemoryBackend) ConfigTemplateCount() int {
	b.mu.RLock("ConfigTemplateCount")
	defer b.mu.RUnlock()

	return b.configTemplates.Len()
}

// PlatformVersionCount returns the total number of platform versions across all regions.
// Used only in tests.
func (b *InMemoryBackend) PlatformVersionCount() int {
	b.mu.RLock("PlatformVersionCount")
	defer b.mu.RUnlock()

	return b.platformVersions.Len()
}

// HandlerOpsLen returns the number of operations registered in the handler's dispatch table.
// Used only in tests.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// AddApplicationInternal seeds an application directly into the backend for testing,
// using the backend's default region.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	b.addApplicationInternal(b.region, app)
}

// AddEnvironmentInternal seeds an environment directly into the backend for testing.
// Uses env.Region if set, otherwise the backend's default region.
func (b *InMemoryBackend) AddEnvironmentInternal(env *Environment) {
	b.mu.Lock("AddEnvironmentInternal")
	defer b.mu.Unlock()

	r := env.Region
	if r == "" {
		r = b.region
	}

	b.addEnvironmentInternal(r, env)
}

// AddAppVersionInternal seeds an application version directly into the backend for testing.
func (b *InMemoryBackend) AddAppVersionInternal(ver *ApplicationVersion) {
	b.mu.Lock("AddAppVersionInternal")
	defer b.mu.Unlock()

	b.addAppVersionInternal(b.region, ver)
}

// AddConfigTemplateInternal seeds a configuration template directly into the backend for testing.
func (b *InMemoryBackend) AddConfigTemplateInternal(tmpl *ConfigurationTemplate) {
	b.mu.Lock("AddConfigTemplateInternal")
	defer b.mu.Unlock()

	b.addConfigTemplateInternal(b.region, tmpl)
}

// AddPlatformVersionInternal seeds a platform version directly into the backend for testing.
func (b *InMemoryBackend) AddPlatformVersionInternal(pv *PlatformVersion) {
	b.mu.Lock("AddPlatformVersionInternal")
	defer b.mu.Unlock()

	b.addPlatformVersionInternal(b.region, pv)
}
