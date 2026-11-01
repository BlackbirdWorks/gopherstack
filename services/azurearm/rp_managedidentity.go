package azurearm

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftManagedIdentity = "Microsoft.ManagedIdentity"
	msiIdentitiesType                 = "userAssignedIdentities"
	msiFederatedType                  = "federatedIdentityCredentials"
	msiCodeBad                        = "InvalidRequestContent"
)

var (
	msiNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{2,127}$`)
	msiFedRe  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{2,119}$`)
)

func msiAPIVersions() []string {
	return []string{
		"2015-08-31-preview", "2018-11-30", "2021-09-30-preview", "2022-01-31-preview", "2023-01-31",
		"2023-07-31-preview", "2024-11-30",
	}
}

type storedFederated struct {
	Name      string   `json:"name"`
	Issuer    string   `json:"issuer"`
	Subject   string   `json:"subject"`
	Audiences []string `json:"audiences"`
}

type storedIdentity struct {
	Tags        map[string]string  `json:"tags,omitempty"`
	Name        string             `json:"name"`
	ResourceGrp string             `json:"resourceGroup"`
	Location    string             `json:"location"`
	PrincipalID string             `json:"principalId"`
	ClientID    string             `json:"clientId"`
	Federated   []*storedFederated `json:"federated,omitempty"`
}

// ManagedIdentityProvider implements ResourceProvider for Microsoft.ManagedIdentity user-assigned identities.
type ManagedIdentityProvider struct {
	mu         *lockmetrics.RWMutex
	identities map[string]*storedIdentity
	tenantID   string
}

var _ ResourceProvider = (*ManagedIdentityProvider)(nil)

// NewManagedIdentityProvider creates a provider whose identities belong to tenantID.
func NewManagedIdentityProvider(tenantID string) *ManagedIdentityProvider {
	return &ManagedIdentityProvider{
		mu: lockmetrics.New("azurearm.msiprovider"), identities: map[string]*storedIdentity{}, tenantID: tenantID,
	}
}

// Namespace implements ResourceProvider.
func (p *ManagedIdentityProvider) Namespace() string { return namespaceMicrosoftManagedIdentity }

// ResourceTypes implements ResourceProvider.
func (p *ManagedIdentityProvider) ResourceTypes() []ResourceTypeDef {
	v := msiAPIVersions()

	return []ResourceTypeDef{
		{Type: msiIdentitiesType, APIVersions: v, HasChildren: true},
		{Type: msiIdentitiesType + "/" + msiFederatedType, APIVersions: v},
	}
}

// ListKeys implements ResourceProvider.
func (p *ManagedIdentityProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *ManagedIdentityProvider) Reset() {
	p.mu.Lock("Reset")
	p.identities = map[string]*storedIdentity{}
	p.mu.Unlock()
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *ManagedIdentityProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for k, i := range p.identities {
		if resourceGroupsEqual(i.ResourceGrp, resourceGroup) {
			delete(p.identities, k)
		}
	}
}

func msiKey(rg, name string) string { return strings.ToLower(rg + "/" + name) }

func msiKind(id ResourceID) (bool, bool) {
	if len(id.Types) == 0 || !strings.EqualFold(id.Types[0], msiIdentitiesType) {
		return false, false
	}

	if len(id.Types) == 1 {
		return true, false
	}

	return false, len(id.Types) == 2 && strings.EqualFold(id.Types[1], msiFederatedType)
}

func (p *ManagedIdentityProvider) identityBody(id ResourceID, i *storedIdentity) map[string]any {
	out := map[string]any{
		netFieldID:    id.ARMID(),
		fieldName:     i.Name,
		fieldType:     namespaceMicrosoftManagedIdentity + "/" + msiIdentitiesType,
		fieldLocation: i.Location,
		fieldTags:     tagsOrEmpty(i.Tags),
		fieldProperties: map[string]any{
			netFieldTenantID: p.tenantID, "principalId": i.PrincipalID, "clientId": i.ClientID,
		},
	}

	return out
}

func federatedBody(id ResourceID, f *storedFederated) map[string]any {
	return map[string]any{
		netFieldID: id.ARMID(), fieldName: f.Name,
		fieldType: namespaceMicrosoftManagedIdentity + "/" + msiIdentitiesType + "/" + msiFederatedType,
		fieldProperties: map[string]any{
			"issuer": f.Issuer, "subject": f.Subject, "audiences": f.Audiences,
		},
	}
}

// Put implements ResourceProvider.
func (p *ManagedIdentityProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	ident, fed := msiKind(id)

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	switch {
	case ident:
		return p.putIdentity(id, body)
	case fed:
		return p.putFederated(id, body)
	}

	return nil, ErrResourceNotFound
}

func (p *ManagedIdentityProvider) putIdentity(id ResourceID, body map[string]any) (map[string]any, error) {
	name := id.Names[0]
	if !msiNameRe.MatchString(name) {
		return nil, netErr(http.StatusBadRequest, netCodeName,
			"The resource name %s is invalid. Names must be 3-128 characters of letters, digits, '-' and '_' and "+
				"start with a letter or digit.", name)
	}

	key := msiKey(id.ResourceGroup, name)
	cur, exists := p.identities[key]
	loc := normLocation(strOf(body, fieldLocation))

	if loc == "" && !exists {
		return nil, netErr(
			http.StatusBadRequest,
			netCodeLocation,
			"The location property is required for this definition.",
		)
	}

	rec := &storedIdentity{
		Name: name, ResourceGrp: id.ResourceGroup, Location: loc, Tags: stringTags(body[fieldTags]),
		PrincipalID: uuid.NewString(), ClientID: uuid.NewString(),
	}

	if exists {
		rec.PrincipalID, rec.ClientID, rec.Federated = cur.PrincipalID, cur.ClientID, cur.Federated

		if loc == "" {
			rec.Location = cur.Location
		}
	}

	p.identities[key] = rec

	return p.identityBody(id, rec), nil
}

func (p *ManagedIdentityProvider) putFederated(id ResourceID, body map[string]any) (map[string]any, error) {
	parent, ok := p.identities[msiKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	name := id.Names[1]
	if !msiFedRe.MatchString(name) {
		return nil, netErr(http.StatusBadRequest, netCodeName,
			"The resource name %s is invalid. Names must be 3-120 characters of letters, digits, '-' and '_' and "+
				"start with a letter or digit.", name)
	}

	props := propsOf(body)
	f := &storedFederated{
		Name:      name,
		Issuer:    strOf(props, "issuer"),
		Subject:   strOf(props, "subject"),
		Audiences: strList(props["audiences"]),
	}

	if u, err := url.Parse(f.Issuer); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.issuer must be an https URL.")
	}

	if f.Subject == "" {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.subject is required.")
	}

	if len(f.Audiences) != 1 {
		return nil, netErr(http.StatusBadRequest, msiCodeBad, "properties.audiences must contain exactly one value.")
	}

	for _, o := range parent.Federated {
		if !strings.EqualFold(o.Name, name) && o.Issuer == f.Issuer && o.Subject == f.Subject {
			return nil, netErr(http.StatusConflict, "Conflict",
				"A federated identity credential with the same issuer and subject already exists on this identity.")
		}
	}

	replaced := false

	for i, o := range parent.Federated {
		if strings.EqualFold(o.Name, name) {
			parent.Federated[i], replaced = f, true
		}
	}

	if !replaced {
		parent.Federated = append(parent.Federated, f)
	}

	return federatedBody(id, f), nil
}

// Get implements ResourceProvider.
func (p *ManagedIdentityProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	ident, fed := msiKind(id)

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	parent, ok := p.identities[msiKey(id.ResourceGroup, firstName(id))]
	if !ok || (!ident && !fed) {
		return nil, ErrResourceNotFound
	}

	if ident {
		return p.identityBody(id, parent), nil
	}

	for _, f := range parent.Federated {
		if strings.EqualFold(f.Name, id.Names[1]) {
			return federatedBody(id, f), nil
		}
	}

	return nil, ErrResourceNotFound
}

func firstName(id ResourceID) string {
	if len(id.Names) == 0 {
		return ""
	}

	return id.Names[0]
}

// Delete implements ResourceProvider.
func (p *ManagedIdentityProvider) Delete(_ context.Context, id ResourceID) error {
	ident, fed := msiKind(id)

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	key := msiKey(id.ResourceGroup, firstName(id))
	parent, ok := p.identities[key]

	switch {
	case !ok || (!ident && !fed):
		return ErrResourceNotFound
	case ident:
		delete(p.identities, key)

		return nil
	}

	for i, f := range parent.Federated {
		if strings.EqualFold(f.Name, id.Names[1]) {
			parent.Federated = append(parent.Federated[:i], parent.Federated[i+1:]...)

			return nil
		}
	}

	return ErrResourceNotFound
}

// List implements ResourceProvider.
func (p *ManagedIdentityProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	ident, fed := msiKind(id)
	out := []map[string]any{}

	if fed {
		parent, ok := p.identities[msiKey(id.ResourceGroup, id.Names[0])]
		if !ok {
			return nil, ErrResourceNotFound
		}

		for _, f := range parent.Federated {
			fid := id
			fid.Names = []string{id.Names[0], f.Name}
			out = append(out, federatedBody(fid, f))
		}

		return out, nil
	}

	if !ident {
		return nil, ErrResourceNotFound
	}

	recs := make([]*storedIdentity, 0, len(p.identities))

	for _, i := range p.identities {
		if id.ResourceGroup == "" || resourceGroupsEqual(i.ResourceGrp, id.ResourceGroup) {
			recs = append(recs, i)
		}
	}

	sort.Slice(recs, func(a, b int) bool {
		return msiKey(recs[a].ResourceGrp, recs[a].Name) < msiKey(recs[b].ResourceGrp, recs[b].Name)
	})

	for _, i := range recs {
		iid := ResourceID{
			SubscriptionID: id.SubscriptionID,
			ResourceGroup:  i.ResourceGrp,
			Namespace:      namespaceMicrosoftManagedIdentity,
			Types:          []string{msiIdentitiesType},
			Names:          []string{i.Name},
		}
		out = append(out, p.identityBody(iid, i))
	}

	return out, nil
}

// PatchTags implements the identity Update operation.
func (p *ManagedIdentityProvider) PatchTags(id ResourceID, tags map[string]string) (map[string]any, error) {
	if ident, _ := msiKind(id); !ident {
		return nil, ErrResourceNotFound
	}

	p.mu.Lock("PatchTags")
	defer p.mu.Unlock()

	i, ok := p.identities[msiKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	i.Tags = tags

	return p.identityBody(id, i), nil
}

// lookupUserAssigned resolves a user-assigned identity's ARM id to its principal and client ids.
func (p *ManagedIdentityProvider) lookupUserAssigned(idStr string) (string, string, bool) {
	rid, err := ParseGenericResourcePath(idStr)
	if err != nil || !strings.EqualFold(rid.Namespace, namespaceMicrosoftManagedIdentity) ||
		len(rid.Types) != 1 || !strings.EqualFold(rid.Types[0], msiIdentitiesType) {
		return "", "", false
	}

	p.mu.RLock("lookupUserAssigned")
	defer p.mu.RUnlock()

	i, ok := p.identities[msiKey(rid.ResourceGroup, rid.Names[0])]
	if !ok {
		return "", "", false
	}

	return i.PrincipalID, i.ClientID, true
}
