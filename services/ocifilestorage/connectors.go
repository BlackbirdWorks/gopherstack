package ocifilestorage

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	connectorLdapBind = "LDAPBIND"
	maxEndpoints      = 5
	maxPort           = 65535
)

type createConnectorIn struct {
	tagsIn
	PasswordSecretVersion           *int       `json:"passwordSecretVersion"`
	TrustedCertificateSecretVersion *int       `json:"trustedCertificateSecretVersion"`
	DisplayName                     *string    `json:"displayName"`
	AvailabilityDomain              string     `json:"availabilityDomain"`
	CompartmentID                   string     `json:"compartmentId"`
	ConnectorType                   string     `json:"connectorType"`
	BindDistinguishedName           string     `json:"bindDistinguishedName"`
	PasswordSecretID                string     `json:"passwordSecretId"`
	TrustedCertificateSecretID      string     `json:"trustedCertificateSecretId"`
	Endpoints                       []endpoint `json:"endpoints"`
	Locks                           []lock     `json:"locks"`
}

type updateConnectorIn struct {
	tagsIn
	DisplayName *string `json:"displayName"`
}

func (b *InMemoryBackend) connView(e *entry[*Connector]) Connector {
	v := *e.W
	v.base = cloneBase(e.W.base)
	v.Endpoints = slices.Clone(e.W.Endpoints)

	return v
}

func connSummary(v Connector) map[string]any {
	return project(v, "availabilityDomain", "locks", "freeformTags", "definedTags", "systemTags", "compartmentId", "id",
		"lifecycleState", "displayName", "timeCreated", "connectorType")
}

func (b *InMemoryBackend) checkSecret(id string, version *int) *APIError {
	if id == "" || b.secret == nil {
		return nil
	}

	if _, ok := b.secret(id, version); !ok {
		return errNotFound(id)
	}

	return nil
}

func (b *InMemoryBackend) CreateConnector(in createConnectorIn, token string) (Connector, string, error) {
	if aerr := validateConnector(in); aerr != nil {
		return Connector{}, "", aerr
	}

	b.mu.Lock("CreateConnector")
	defer b.mu.Unlock()

	if id := b.retryHit(kindConnector, token); id != "" {
		if e, ok := b.conns[id]; ok {
			return b.connView(e), e.Etag, nil
		}
	}

	name := "connector-" + b.nowUTC().Format("20060102-150405")
	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	for _, aerr := range []*APIError{
		b.checkCompartment(in.CompartmentID), checkName(name), checkLocks(in.Locks),
		b.checkSecret(in.PasswordSecretID, in.PasswordSecretVersion),
		b.checkSecret(in.TrustedCertificateSecretID, in.TrustedCertificateSecretVersion),
	} {
		if aerr != nil {
			return Connector{}, "", aerr
		}
	}

	c := &Connector{
		base: b.newBase(kindConnector, in.tagsIn, in.Locks), CompartmentID: in.CompartmentID, DisplayName: name,
		AvailabilityDomain: in.AvailabilityDomain, ConnectorType: connectorLdapBind,
		BindDistinguishedName: in.BindDistinguishedName, PasswordSecretID: in.PasswordSecretID,
		PasswordSecretVersion: in.PasswordSecretVersion, TrustedCertificateSecretID: in.TrustedCertificateSecretID,
		TrustedCertificateSecretVersion: in.TrustedCertificateSecretVersion, Endpoints: slices.Clone(in.Endpoints),
	}
	e := put(b, b.conns, c, stActive)
	b.remember(kindConnector, token, c.ID)

	return b.connView(e), e.Etag, nil
}

func validateConnector(in createConnectorIn) *APIError {
	if in.ConnectorType != connectorLdapBind {
		return errParam("connectorType must be LDAPBIND")
	}

	if in.AvailabilityDomain == "" || in.BindDistinguishedName == "" || in.PasswordSecretID == "" {
		return errMissing("availabilityDomain, bindDistinguishedName and passwordSecretId are required")
	}

	if len(in.Endpoints) == 0 || len(in.Endpoints) > maxEndpoints {
		return errParam("between 1 and 5 endpoints are required")
	}

	for _, ep := range in.Endpoints {
		if ep.Hostname == "" || ep.Port < 1 || ep.Port > maxPort {
			return errParam("each endpoint needs a hostname and a port between 1 and 65535")
		}
	}

	return nil
}

// GetConnector returns one outbound connector.
func (b *InMemoryBackend) GetConnector(id string) (Connector, string, error) {
	b.mu.RLock("GetConnector")
	defer b.mu.RUnlock()

	e, aerr := b.conns.get(id)
	if aerr != nil {
		return Connector{}, "", aerr
	}

	return b.connView(e), e.Etag, nil
}

// ListConnectors lists outbound connectors of a compartment and availability domain.
func (b *InMemoryBackend) ListConnectors(f MTFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListConnectors")
	defer b.mu.RUnlock()

	return listScoped(b, b.conns, f, o, scope[*Connector]{
		fields: func(w *Connector) (string, string, string) {
			return w.CompartmentID, w.AvailabilityDomain, w.DisplayName
		},
		render: func(e *entry[*Connector]) map[string]any { return connSummary(b.connView(e)) },
	})
}

// UpdateConnector changes a connector's name and tags.
func (b *InMemoryBackend) UpdateConnector(
	id, ifMatch string,
	override bool,
	in updateConnectorIn,
) (Connector, string, error) {
	b.mu.Lock("UpdateConnector")
	defer b.mu.Unlock()

	e, aerr := b.conns.usable(id)
	if aerr != nil {
		return Connector{}, "", aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardChange(&e.W.base, override)); aerr != nil {
		return Connector{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return Connector{}, "", aerr
		}

		e.W.DisplayName = *in.DisplayName
	}

	applyTags(&e.W.base, in.tagsIn)
	e.Etag = newEtag()

	return b.connView(e), e.Etag, nil
}

// DeleteConnector deletes a connector that no mount target uses.
func (b *InMemoryBackend) DeleteConnector(id, ifMatch string, override bool) error {
	b.mu.Lock("DeleteConnector")
	defer b.mu.Unlock()

	e, aerr := b.conns.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(checkEtag(e.Etag, ifMatch), guardDelete(&e.W.base, override)); aerr != nil {
		return aerr
	}

	for _, mt := range b.mts {
		l := mt.W.LdapIdmap
		if mt.W.State != stDeleted && l != nil && (l.OutboundConnector1ID == id || l.OutboundConnector2ID == id) {
			return errConflict("The outbound connector " + id + " is used by the mount target " + mt.W.ID)
		}
	}

	begin(b, e, stDeleting, stDeleted)

	return nil
}

// ChangeConnectorCompartment moves an outbound connector.
func (b *InMemoryBackend) ChangeConnectorCompartment(id, ifMatch, compartment string) error {
	b.mu.Lock("ChangeConnectorCompartment")
	defer b.mu.Unlock()

	e, aerr := b.conns.usable(id)
	if aerr != nil {
		return aerr
	}

	if aerr = firstErr(
		checkEtag(e.Etag, ifMatch),
		guardChange(&e.W.base, false),
		b.checkCompartment(compartment),
	); aerr != nil {
		return aerr
	}

	e.W.CompartmentID = compartment
	e.Etag = newEtag()

	return nil
}
