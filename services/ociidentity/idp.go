package ociidentity

import (
	"encoding/xml"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	opCreateIDP        = "CreateIdentityProvider"
	opGetIDP           = "GetIdentityProvider"
	opListIDPs         = "ListIdentityProviders"
	opUpdateIDP        = "UpdateIdentityProvider"
	opDeleteIDP        = "DeleteIdentityProvider"
	opCreateIdpMapping = "CreateIdpGroupMapping"
	opGetIdpMapping    = "GetIdpGroupMapping"
	opListIdpMappings  = "ListIdpGroupMappings"
	opUpdateIdpMapping = "UpdateIdpGroupMapping"
	opDeleteIdpMapping = "DeleteIdpGroupMapping"

	protocolSAML2     = "SAML2"
	maxIDPs           = 50
	maxIDPNameLen     = 100
	maxMetadataBytes  = 256 << 10
	maxMetadataDepth  = 32
	maxMappingsPerIDP = 250
	maxIdpGroupName   = 255
	maxFreeformAttrs  = 50
	samlMetadataNS    = "urn:oasis:names:tc:SAML:2.0:metadata"
	samlRedirectBind  = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect"
)

// IdentityProvider is a stored SAML2 identity provider.
type IdentityProvider struct {
	Meta
	ProductType        string            `json:"productType"`
	MetadataURL        string            `json:"metadataUrl"`
	Metadata           string            `json:"metadata"`
	SigningCertificate string            `json:"signingCertificate"`
	RedirectURL        string            `json:"redirectUrl"`
	FreeformAttributes map[string]string `json:"freeformAttributes"`
	Internal           Internal          `json:"internal"`
}

// IdentityProviderWire is the SAML2 identity provider wire shape.
type IdentityProviderWire struct {
	FreeformAttributes map[string]string `json:"freeformAttributes"`
	Meta
	Protocol           string `json:"protocol"`
	ProductType        string `json:"productType"`
	MetadataURL        string `json:"metadataUrl"`
	Metadata           string `json:"metadata"`
	SigningCertificate string `json:"signingCertificate"`
	RedirectURL        string `json:"redirectUrl"`
}

// IdpGroupMapping maps an identity provider group to an IAM group.
type IdpGroupMapping struct {
	TimeCreated   time.Time `json:"timeCreated"`
	ID            string    `json:"id"`
	IdpID         string    `json:"idpId"`
	IdpGroupName  string    `json:"idpGroupName"`
	GroupID       string    `json:"groupId"`
	CompartmentID string    `json:"compartmentId"`
	Etag          string    `json:"etag"`
	Seq           int64     `json:"seq"`
}

// IdpGroupMappingWire is the mapping wire shape.
type IdpGroupMappingWire struct {
	TimeCreated    time.Time `json:"timeCreated"`
	ID             string    `json:"id"`
	IdpID          string    `json:"idpId"`
	IdpGroupName   string    `json:"idpGroupName"`
	GroupID        string    `json:"groupId"`
	CompartmentID  string    `json:"compartmentId"`
	LifecycleState string    `json:"lifecycleState"`
}

// CreateIDPIn is the CreateIdentityProvider request body.
type CreateIDPIn struct {
	RetryToken string `json:"-"`
	Tags
	FreeformAttributes map[string]string `json:"freeformAttributes"`
	Protocol           string            `json:"protocol"`
	CompartmentID      string            `json:"compartmentId"`
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	ProductType        string            `json:"productType"`
	MetadataURL        string            `json:"metadataUrl"`
	Metadata           string            `json:"metadata"`
}

// UpdateIDPIn is the UpdateIdentityProvider request body.
type UpdateIDPIn struct {
	Tags
	Description        *string           `json:"description"`
	MetadataURL        *string           `json:"metadataUrl"`
	Metadata           *string           `json:"metadata"`
	FreeformAttributes map[string]string `json:"freeformAttributes"`
	Protocol           string            `json:"protocol"`
}

// samlInfo is what the service derives from SAML metadata.
type samlInfo struct {
	certificate string
	redirectURL string
}

type samlParser struct {
	dec        *xml.Decoder
	info       samlInfo
	firstSSO   string
	keyUse     string
	stack      []string
	sawEntity  bool
	sawIDPDesc bool
}

func (p *samlParser) inIDP() bool { return slices.Contains(p.stack, "IDPSSODescriptor") }

func (p *samlParser) start(t xml.StartElement) *APIError {
	if len(p.stack) >= maxMetadataDepth {
		return errParam("metadata is nested too deeply")
	}

	p.stack = append(p.stack, t.Name.Local)
	samlName := t.Name.Space == samlMetadataNS

	switch {
	case len(p.stack) == 1:
		p.sawEntity = t.Name.Local == "EntityDescriptor" && samlName
		if !p.sawEntity {
			return errParam("metadata root must be a SAML2 EntityDescriptor")
		}
	case t.Name.Local == "IDPSSODescriptor" && samlName:
		p.sawIDPDesc = true
	case t.Name.Local == "KeyDescriptor" && p.inIDP():
		p.keyUse = attr(t.Attr, "use")
	case t.Name.Local == "SingleSignOnService" && p.inIDP():
		p.sso(t)
	case t.Name.Local == "X509Certificate" && p.inIDP():
		return p.certificate(t)
	}

	return nil
}

func (p *samlParser) sso(t xml.StartElement) {
	loc := attr(t.Attr, "Location")
	if p.firstSSO == "" {
		p.firstSSO = loc
	}

	if attr(t.Attr, "Binding") == samlRedirectBind && p.info.redirectURL == "" {
		p.info.redirectURL = loc
	}
}

func (p *samlParser) certificate(t xml.StartElement) *APIError {
	if (p.keyUse != "" && p.keyUse != "signing") || p.info.certificate != "" {
		return nil
	}

	var cert string
	if err := p.dec.DecodeElement(&cert, &t); err != nil {
		return errParam("metadata is not well-formed XML")
	}

	p.stack = p.stack[:len(p.stack)-1]
	p.info.certificate = strings.Join(strings.Fields(cert), "")

	return nil
}

func (p *samlParser) end() {
	if len(p.stack) == 0 {
		return
	}

	if p.stack[len(p.stack)-1] == "KeyDescriptor" {
		p.keyUse = ""
	}

	p.stack = p.stack[:len(p.stack)-1]
}

// parseSAMLMetadata checks that doc is well-formed SAML2 IdP metadata and extracts the signing certificate and
// the single sign-on redirect URL. The document is never fetched or resolved: DTDs are rejected.
func parseSAMLMetadata(doc string) (samlInfo, *APIError) {
	if strings.TrimSpace(doc) == "" {
		return samlInfo{}, errMissing("metadata is required")
	}

	if len(doc) > maxMetadataBytes {
		return samlInfo{}, errParam("metadata must be at most 262144 bytes")
	}

	p := &samlParser{dec: xml.NewDecoder(strings.NewReader(doc))}
	p.dec.Strict = true

	for {
		tok, err := p.dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return samlInfo{}, errParam("metadata is not well-formed XML")
		}

		switch t := tok.(type) {
		case xml.Directive:
			return samlInfo{}, errParam("metadata must not contain a DTD")
		case xml.StartElement:
			if aerr := p.start(t); aerr != nil {
				return samlInfo{}, aerr
			}
		case xml.EndElement:
			p.end()
		}
	}

	if !p.sawEntity || !p.sawIDPDesc {
		return samlInfo{}, errParam("metadata must describe a SAML2 IDPSSODescriptor")
	}

	if p.info.redirectURL == "" {
		p.info.redirectURL = p.firstSSO
	}

	if p.info.certificate == "" || p.info.redirectURL == "" {
		return samlInfo{}, errParam("metadata must contain a signing certificate and a SingleSignOnService location")
	}

	return p.info, nil
}

func attr(attrs []xml.Attr, name string) string {
	for _, a := range attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}

	return ""
}

func validFreeformAttrs(m map[string]string) *APIError {
	if len(m) > maxFreeformAttrs {
		return errLimit("freeformAttributes holds at most 50 entries")
	}

	return nil
}

func (p *IdentityProvider) view() IdentityProviderWire {
	return IdentityProviderWire{
		Meta: p.Meta, Protocol: protocolSAML2, ProductType: p.ProductType, MetadataURL: p.MetadataURL,
		Metadata: p.Metadata, SigningCertificate: p.SigningCertificate, RedirectURL: p.RedirectURL,
		FreeformAttributes: nonNilMap(p.FreeformAttributes),
	}
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return maps.Clone(m)
}

func validIDPCreate(in CreateIDPIn) (samlInfo, *APIError) {
	if in.Protocol != "" && in.Protocol != protocolSAML2 {
		return samlInfo{}, errParam("protocol must be SAML2")
	}

	if aerr := requireTenancy(in.CompartmentID); aerr != nil {
		return samlInfo{}, aerr
	}

	if aerr := validateName(in.Name, maxIDPNameLen); aerr != nil {
		return samlInfo{}, aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return samlInfo{}, aerr
	}

	if in.ProductType != "IDCS" && in.ProductType != "ADFS" {
		return samlInfo{}, errParam("productType must be IDCS or ADFS")
	}

	if in.MetadataURL == "" {
		return samlInfo{}, errMissing("metadataUrl is required")
	}

	if aerr := validFreeformAttrs(in.FreeformAttributes); aerr != nil {
		return samlInfo{}, aerr
	}

	return parseSAMLMetadata(in.Metadata)
}

func (b *InMemoryBackend) idpByID(id string) (*IdentityProvider, *APIError) {
	p, ok := b.idps[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return p, nil
}

// CreateIdentityProvider creates a SAML2 identity provider from its metadata document.
func (b *InMemoryBackend) CreateIdentityProvider(in CreateIDPIn) (IdentityProviderWire, string, error) {
	info, aerr := validIDPCreate(in)
	if aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	b.mu.Lock("CreateIdentityProvider")
	defer b.mu.Unlock()

	r, hit, aerr := replayCreate(b, "CreateIdentityProvider", in.RetryToken, b.idps, nil)
	if aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	if hit {
		return r.view(), r.Internal.Etag, nil
	}

	if len(b.idps) >= maxIDPs {
		return IdentityProviderWire{}, "", errLimit("The tenancy identity provider limit (50) has been reached")
	}

	for _, p := range b.idps {
		if strs.Equal(p.Name, in.Name) {
			return IdentityProviderWire{}, "", errConflict(
				"Conflict",
				"Identity provider "+in.Name+" already exists in the tenancy",
			)
		}
	}

	p := &IdentityProvider{
		Meta:        b.newMeta("saml2idp", TenancyID, in.Name, in.Description, in.Tags),
		ProductType: in.ProductType, MetadataURL: in.MetadataURL, Metadata: in.Metadata,
		SigningCertificate: info.certificate, RedirectURL: info.redirectURL,
		FreeformAttributes: nonNilMap(in.FreeformAttributes), Internal: b.newInternal(),
	}
	b.idps[p.ID] = p
	b.retry.Remember("CreateIdentityProvider", in.RetryToken, p.ID, b.nowUTC())

	return p.view(), p.Internal.Etag, nil
}

// GetIdentityProvider returns one identity provider.
func (b *InMemoryBackend) GetIdentityProvider(id string) (IdentityProviderWire, string, error) {
	b.mu.RLock("GetIdentityProvider")
	defer b.mu.RUnlock()

	p, aerr := b.idpByID(id)
	if aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	return p.view(), p.Internal.Etag, nil
}

// ListIdentityProviders lists identity providers of a protocol.
func (b *InMemoryBackend) ListIdentityProviders(
	protocol string,
	f NameFilter,
	o ListOpts,
) (page.Page[IdentityProviderWire], error) {
	if protocol == "" {
		return page.Page[IdentityProviderWire]{}, errMissing("protocol is required")
	}

	if protocol != protocolSAML2 {
		return page.Page[IdentityProviderWire]{}, errParam("protocol must be SAML2")
	}

	if aerr := requireTenancy(f.CompartmentID); aerr != nil {
		return page.Page[IdentityProviderWire]{}, aerr
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return page.Page[IdentityProviderWire]{}, aerr
	}

	if aerr := validState(f.State, stateCreating, stateActive, stateDeleting, stateDeleted, "INACTIVE"); aerr != nil {
		return page.Page[IdentityProviderWire]{}, aerr
	}

	b.mu.RLock("ListIdentityProviders")
	defer b.mu.RUnlock()

	acc := listAccess[IdentityProvider]{fields: func(v *IdentityProvider) (string, string, time.Time, int64) {
		return v.Name, v.LifecycleState, v.TimeCreated, v.Internal.Seq
	}}

	return listNamed(b.idps, f, o, acc, (*IdentityProvider).view), nil
}

// UpdateIdentityProvider changes the description, metadata, attributes and tags.
func (b *InMemoryBackend) UpdateIdentityProvider(
	id, ifMatch string,
	in UpdateIDPIn,
) (IdentityProviderWire, string, error) {
	if in.Protocol != "" && in.Protocol != protocolSAML2 {
		return IdentityProviderWire{}, "", errParam("protocol must be SAML2")
	}

	var info samlInfo

	if in.Metadata != nil {
		var aerr *APIError

		if info, aerr = parseSAMLMetadata(*in.Metadata); aerr != nil {
			return IdentityProviderWire{}, "", aerr
		}
	}

	if aerr := validFreeformAttrs(in.FreeformAttributes); aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	b.mu.Lock("UpdateIdentityProvider")
	defer b.mu.Unlock()

	p, aerr := b.idpByID(id)
	if aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	if aerr = checkEtag(p.Internal.Etag, ifMatch); aerr != nil {
		return IdentityProviderWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return IdentityProviderWire{}, "", aerr
		}

		p.Description = *in.Description
	}

	if in.MetadataURL != nil {
		if *in.MetadataURL == "" {
			return IdentityProviderWire{}, "", errParam("metadataUrl must not be empty")
		}

		p.MetadataURL = *in.MetadataURL
	}

	if in.Metadata != nil {
		p.Metadata, p.SigningCertificate, p.RedirectURL = *in.Metadata, info.certificate, info.redirectURL
	}

	if in.FreeformAttributes != nil {
		p.FreeformAttributes = maps.Clone(in.FreeformAttributes)
	}

	applyTags(&p.Tags, in.Tags)
	p.Internal.Etag = newEtag()

	return p.view(), p.Internal.Etag, nil
}

// DeleteIdentityProvider deletes an identity provider and its group mappings.
func (b *InMemoryBackend) DeleteIdentityProvider(id, ifMatch string) error {
	b.mu.Lock("DeleteIdentityProvider")
	defer b.mu.Unlock()

	p, aerr := b.idpByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(p.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	for mid, m := range b.idpMappings {
		if m.IdpID == id {
			delete(b.idpMappings, mid)
		}
	}

	delete(b.idps, id)

	return nil
}

func (m *IdpGroupMapping) view() IdpGroupMappingWire {
	return IdpGroupMappingWire{
		TimeCreated: m.TimeCreated, ID: m.ID, IdpID: m.IdpID, IdpGroupName: m.IdpGroupName, GroupID: m.GroupID,
		CompartmentID: m.CompartmentID, LifecycleState: stateActive,
	}
}

func (b *InMemoryBackend) mappingByID(idpID, id string) (*IdpGroupMapping, *APIError) {
	m, ok := b.idpMappings[id]
	if !ok || m.IdpID != idpID {
		return nil, errNotFound(id)
	}

	return m, nil
}

func validIdpGroupName(name string) *APIError {
	if strings.TrimSpace(name) == "" {
		return errMissing("idpGroupName is required")
	}

	if len(name) > maxIdpGroupName {
		return errParam("idpGroupName must be at most 255 characters")
	}

	return nil
}

func (b *InMemoryBackend) mappingConflict(idpID, group, groupID, except string) bool {
	for _, m := range b.idpMappings {
		if m.IdpID == idpID && m.ID != except && m.IdpGroupName == group && m.GroupID == groupID {
			return true
		}
	}

	return false
}

// CreateIdpGroupMapping maps an identity provider group to an existing IAM group.
func (b *InMemoryBackend) CreateIdpGroupMapping(
	idpID, group, groupID, token string,
) (IdpGroupMappingWire, string, error) {
	if aerr := validIdpGroupName(group); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	if groupID == "" {
		return IdpGroupMappingWire{}, "", errMissing("groupId is required")
	}

	b.mu.Lock("CreateIdpGroupMapping")
	defer b.mu.Unlock()

	if _, aerr := b.idpByID(idpID); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	scope := "CreateIdpGroupMapping/" + idpID

	r, hit, aerr := replayCreate(b, scope, token, b.idpMappings, nil)
	if aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	if hit {
		return r.view(), r.Etag, nil
	}

	if _, aerr = b.groupByID(groupID); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	count := 0

	for _, m := range b.idpMappings {
		if m.IdpID == idpID {
			count++
		}
	}

	if count >= maxMappingsPerIDP {
		return IdpGroupMappingWire{}, "", errLimit("An identity provider has at most 250 group mappings")
	}

	if b.mappingConflict(idpID, group, groupID, "") {
		return IdpGroupMappingWire{}, "", errConflict(
			"Conflict",
			"The identity provider group is already mapped to that group",
		)
	}

	m := &IdpGroupMapping{
		TimeCreated: b.nowUTC(), ID: newOCID("idpgroupmapping"), IdpID: idpID, IdpGroupName: group, GroupID: groupID,
		CompartmentID: TenancyID, Etag: newEtag(), Seq: b.nextSeq(),
	}
	b.idpMappings[m.ID] = m
	b.retry.Remember(scope, token, m.ID, b.nowUTC())

	return m.view(), m.Etag, nil
}

// GetIdpGroupMapping returns one mapping.
func (b *InMemoryBackend) GetIdpGroupMapping(idpID, id string) (IdpGroupMappingWire, string, error) {
	b.mu.RLock("GetIdpGroupMapping")
	defer b.mu.RUnlock()

	if _, aerr := b.idpByID(idpID); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	m, aerr := b.mappingByID(idpID, id)
	if aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	return m.view(), m.Etag, nil
}

// ListIdpGroupMappings lists the mappings of an identity provider in creation order.
func (b *InMemoryBackend) ListIdpGroupMappings(idpID string, o ListOpts) (page.Page[IdpGroupMappingWire], error) {
	b.mu.RLock("ListIdpGroupMappings")
	defer b.mu.RUnlock()

	if _, aerr := b.idpByID(idpID); aerr != nil {
		return page.Page[IdpGroupMappingWire]{}, aerr
	}

	var all []*IdpGroupMapping

	for _, m := range b.idpMappings {
		if m.IdpID == idpID {
			all = append(all, m)
		}
	}

	pg := paginate(all, ListOpts{Page: o.Page, Limit: o.Limit}, func(m *IdpGroupMapping) string { return m.ID },
		func(m *IdpGroupMapping) (time.Time, int64) { return m.TimeCreated, m.Seq })

	out := make([]IdpGroupMappingWire, 0, len(pg.Data))
	for _, m := range pg.Data {
		out = append(out, m.view())
	}

	return page.Page[IdpGroupMappingWire]{Data: out, Next: pg.Next}, nil
}

// UpdateIdpGroupMapping changes the IDP group name or the IAM group of a mapping.
func (b *InMemoryBackend) UpdateIdpGroupMapping(
	idpID, id, ifMatch string,
	name, groupID *string,
) (IdpGroupMappingWire, string, error) {
	b.mu.Lock("UpdateIdpGroupMapping")
	defer b.mu.Unlock()

	if _, aerr := b.idpByID(idpID); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	m, aerr := b.mappingByID(idpID, id)
	if aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	if aerr = checkEtag(m.Etag, ifMatch); aerr != nil {
		return IdpGroupMappingWire{}, "", aerr
	}

	newName, newGroup := m.IdpGroupName, m.GroupID

	if name != nil {
		if aerr = validIdpGroupName(*name); aerr != nil {
			return IdpGroupMappingWire{}, "", aerr
		}

		newName = *name
	}

	if groupID != nil {
		if _, aerr = b.groupByID(*groupID); aerr != nil {
			return IdpGroupMappingWire{}, "", aerr
		}

		newGroup = *groupID
	}

	if b.mappingConflict(idpID, newName, newGroup, id) {
		return IdpGroupMappingWire{}, "", errConflict(
			"Conflict",
			"The identity provider group is already mapped to that group",
		)
	}

	m.IdpGroupName, m.GroupID, m.Etag = newName, newGroup, newEtag()

	return m.view(), m.Etag, nil
}

// DeleteIdpGroupMapping deletes a mapping.
func (b *InMemoryBackend) DeleteIdpGroupMapping(idpID, id, ifMatch string) error {
	b.mu.Lock("DeleteIdpGroupMapping")
	defer b.mu.Unlock()

	if _, aerr := b.idpByID(idpID); aerr != nil {
		return aerr
	}

	m, aerr := b.mappingByID(idpID, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(m.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.idpMappings, id)

	return nil
}

func (b *InMemoryBackend) groupHasMapping(groupID string) bool {
	return slices.ContainsFunc(slices.Collect(maps.Values(b.idpMappings)), func(m *IdpGroupMapping) bool {
		return m.GroupID == groupID
	})
}

func (h *Handler) createIdentityProvider(q *req) error {
	var in CreateIDPIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)
	v, etag, err := h.Backend.CreateIdentityProvider(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getIdentityProvider(q *req) error {
	v, etag, err := h.Backend.GetIdentityProvider(q.param("identityProviderId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listIdentityProviders(q *req) error {
	pg, err := h.Backend.ListIdentityProviders(q.query("protocol"), nameFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateIdentityProvider(q *req) error {
	var in UpdateIDPIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateIdentityProvider(q.param("identityProviderId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteIdentityProvider(q *req) error {
	return done(q, h.Backend.DeleteIdentityProvider(q.param("identityProviderId"), q.ifMatch()))
}

func (h *Handler) createIdpGroupMapping(q *req) error {
	var in struct {
		IdpGroupName string `json:"idpGroupName"`
		GroupID      string `json:"groupId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.CreateIdpGroupMapping(q.param("identityProviderId"), in.IdpGroupName, in.GroupID,
		q.r.Header.Get(headerRetry))

	return reply(q, v, etag, err)
}

func (h *Handler) getIdpGroupMapping(q *req) error {
	v, etag, err := h.Backend.GetIdpGroupMapping(q.param("identityProviderId"), q.param("mappingId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listIdpGroupMappings(q *req) error {
	pg, err := h.Backend.ListIdpGroupMappings(q.param("identityProviderId"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateIdpGroupMapping(q *req) error {
	var in struct {
		IdpGroupName *string `json:"idpGroupName"`
		GroupID      *string `json:"groupId"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateIdpGroupMapping(q.param("identityProviderId"), q.param("mappingId"), q.ifMatch(),
		in.IdpGroupName, in.GroupID)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteIdpGroupMapping(q *req) error {
	return done(q, h.Backend.DeleteIdpGroupMapping(q.param("identityProviderId"), q.param("mappingId"), q.ifMatch()))
}
