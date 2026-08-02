package grafana

// DescribeWorkspaceAuthentication returns the authentication state for a
// workspace.
func (b *InMemoryBackend) DescribeWorkspaceAuthentication(id string) (*Workspace, error) {
	b.mu.RLock("DescribeWorkspaceAuthentication")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	cp := *w

	return &cp, nil
}

// validateUpdateWorkspaceAuthenticationRequest validates the requested
// providers and, when a SamlConfiguration is supplied, that it is
// well-formed: IdpMetadata must specify exactly one of url/xml (matching
// "Specifying both will cause an error" on the real union's doc comment),
// and a SamlConfiguration only makes sense when SAML is one of the
// requested providers.
func validateUpdateWorkspaceAuthenticationRequest(req *updateWorkspaceAuthenticationRequest) error {
	if err := validateAuthenticationProviders(req.AuthenticationProviders); err != nil {
		return err
	}

	if req.SamlConfiguration == nil {
		return nil
	}

	hasSAML := false

	for _, p := range req.AuthenticationProviders {
		if p == AuthProviderSAML {
			hasSAML = true
		}
	}

	if !hasSAML {
		return validationError("samlConfiguration requires SAML in authenticationProviders")
	}

	meta := req.SamlConfiguration.IdpMetadata
	if meta == nil || (meta.URL == "" && meta.XML == "") {
		return validationError("samlConfiguration.idpMetadata requires exactly one of url or xml")
	}

	if meta.URL != "" && meta.XML != "" {
		return validationError("samlConfiguration.idpMetadata must not specify both url and xml")
	}

	return nil
}

// UpdateWorkspaceAuthentication updates a workspace's authentication
// providers and, when SAML is enabled, its SAML configuration.
func (b *InMemoryBackend) UpdateWorkspaceAuthentication(
	id string, req *updateWorkspaceAuthenticationRequest,
) (*Workspace, error) {
	if err := validateUpdateWorkspaceAuthenticationRequest(req); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWorkspaceAuthentication")
	defer b.mu.Unlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return nil, notFoundError(resourceTypeWorkspace, id)
	}

	ssoClientID, samlStatus := newWorkspaceAuthState(req.AuthenticationProviders)
	// Preserve an existing AWS SSO client ID across an update that keeps
	// AWS_SSO enabled, rather than rotating it on every call.
	if ssoClientID != "" && w.SsoClientID != "" {
		ssoClientID = w.SsoClientID
	}

	w.AuthenticationProviders = cloneStrs(req.AuthenticationProviders)
	w.SsoClientID = ssoClientID

	if req.SamlConfiguration != nil {
		w.SamlConfig = fromSamlConfigWire(req.SamlConfiguration)
		samlStatus = SamlStatusConfigured
	} else if samlStatus == "" {
		// SAML no longer requested: drop any stored configuration.
		w.SamlConfig = nil
	}

	w.SamlConfigurationStatus = samlStatus

	cp := *w

	return &cp, nil
}
