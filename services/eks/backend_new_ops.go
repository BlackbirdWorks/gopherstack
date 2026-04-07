package eks

import (
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AccessEntry represents an EKS access entry that grants a principal access to a cluster.
type AccessEntry struct {
	CreatedAt    time.Time  `json:"createdAt"`
	Tags         *tags.Tags `json:"tags,omitempty"`
	PrincipalARN string     `json:"principalArn"`
	ClusterName  string     `json:"clusterName"`
	ARN          string     `json:"accessEntryArn"`
	Type         string     `json:"type"`
	Username     string     `json:"username,omitempty"`
}

// AccessPolicyAssociation represents an access policy associated with an access entry.
type AccessPolicyAssociation struct {
	AssociatedAt time.Time      `json:"associatedAt"`
	AccessScope  map[string]any `json:"accessScope,omitempty"`
	PolicyARN    string         `json:"policyArn"`
	ClusterName  string         `json:"clusterName"`
	PrincipalARN string         `json:"principalArn"`
}

// EncryptionConfig represents a cluster encryption configuration.
type EncryptionConfig struct {
	Provider  map[string]string `json:"provider,omitempty"`
	Resources []string          `json:"resources,omitempty"`
}

// IdentityProviderConfig represents an identity provider configuration for a cluster.
type IdentityProviderConfig struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        *tags.Tags        `json:"tags,omitempty"`
	OIDC        map[string]string `json:"oidc,omitempty"`
	ClusterName string            `json:"clusterName"`
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Status      string            `json:"status"`
}

// Addon represents an EKS managed add-on.
type Addon struct {
	CreatedAt             time.Time  `json:"createdAt"`
	Tags                  *tags.Tags `json:"tags,omitempty"`
	ClusterName           string     `json:"clusterName"`
	AddonName             string     `json:"addonName"`
	ARN                   string     `json:"addonArn"`
	AddonVersion          string     `json:"addonVersion,omitempty"`
	Status                string     `json:"status"`
	ServiceAccountRoleARN string     `json:"serviceAccountRoleArn,omitempty"`
}

// Capability represents an EKS capability.
type Capability struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Status  string `json:"status"`
}

// AnywhereSubscription represents an EKS Anywhere subscription.
type AnywhereSubscription struct {
	CreatedAt       time.Time  `json:"createdAt"`
	Tags            *tags.Tags `json:"tags,omitempty"`
	ID              string     `json:"id"`
	ARN             string     `json:"arn"`
	Name            string     `json:"name"`
	Status          string     `json:"status"`
	LicenseType     string     `json:"licenseType,omitempty"`
	LicenseQuantity int32      `json:"licenseQuantity,omitempty"`
}

// FargateProfileSelector is a namespace/labels selector for a Fargate profile.
type FargateProfileSelector struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Namespace string            `json:"namespace"`
}

// FargateProfile represents an EKS Fargate profile.
type FargateProfile struct {
	CreatedAt           time.Time                `json:"createdAt"`
	Tags                *tags.Tags               `json:"tags,omitempty"`
	ClusterName         string                   `json:"clusterName"`
	FargateProfileName  string                   `json:"fargateProfileName"`
	ARN                 string                   `json:"fargateProfileArn"`
	PodExecutionRoleARN string                   `json:"podExecutionRoleArn,omitempty"`
	Status              string                   `json:"status"`
	Selectors           []FargateProfileSelector `json:"selectors,omitempty"`
}

// PodIdentityAssociation represents an EKS pod identity association.
type PodIdentityAssociation struct {
	CreatedAt      time.Time  `json:"createdAt"`
	Tags           *tags.Tags `json:"tags,omitempty"`
	ClusterName    string     `json:"clusterName"`
	AssociationID  string     `json:"associationId"`
	ARN            string     `json:"associationArn"`
	Namespace      string     `json:"namespace"`
	ServiceAccount string     `json:"serviceAccount"`
	RoleARN        string     `json:"roleArn,omitempty"`
}

// CreateAccessEntry creates an access entry that grants a principal access to a cluster.
func (b *InMemoryBackend) CreateAccessEntry(
	clusterName, principalARN, entryType, username string,
	kv map[string]string,
) (*AccessEntry, error) {
	b.mu.Lock("CreateAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.accessEntries[clusterName] == nil {
		b.accessEntries[clusterName] = make(map[string]*AccessEntry)
	}

	if _, ok := b.accessEntries[clusterName][principalARN]; ok {
		return nil, fmt.Errorf(
			"%w: access entry for %s already exists in cluster %s",
			ErrAlreadyExists,
			principalARN,
			clusterName,
		)
	}

	entryARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"access-entry/"+clusterName+"/"+stableID(clusterName+"/"+principalARN),
	)

	if entryType == "" {
		entryType = "STANDARD"
	}

	t := tags.New("eks.access-entry." + clusterName + "." + stableID(principalARN) + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	entry := &AccessEntry{
		PrincipalARN: principalARN,
		ClusterName:  clusterName,
		ARN:          entryARN,
		Type:         entryType,
		Username:     username,
		CreatedAt:    time.Now().UTC(),
		Tags:         t,
	}
	b.accessEntries[clusterName][principalARN] = entry
	cp := *entry

	return &cp, nil
}

// DeleteAccessEntry removes an access entry from a cluster.
func (b *InMemoryBackend) DeleteAccessEntry(clusterName, principalARN string) error {
	b.mu.Lock("DeleteAccessEntry")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.accessEntries[clusterName] == nil {
		return fmt.Errorf("%w: access entry for %s not found in cluster %s", ErrNotFound, principalARN, clusterName)
	}

	entry, ok := b.accessEntries[clusterName][principalARN]
	if !ok {
		return fmt.Errorf("%w: access entry for %s not found in cluster %s", ErrNotFound, principalARN, clusterName)
	}

	if entry.Tags != nil {
		entry.Tags.Close()
	}

	delete(b.accessEntries[clusterName], principalARN)
	delete(b.accessPolicies[clusterName], principalARN)

	return nil
}

// AssociateAccessPolicy associates an access policy with an access entry.
func (b *InMemoryBackend) AssociateAccessPolicy(
	clusterName, principalARN, policyARN string,
	accessScope map[string]any,
) (*AccessPolicyAssociation, error) {
	b.mu.Lock("AssociateAccessPolicy")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.accessEntries[clusterName] == nil || b.accessEntries[clusterName][principalARN] == nil {
		return nil, fmt.Errorf(
			"%w: access entry for %s not found in cluster %s",
			ErrNotFound,
			principalARN,
			clusterName,
		)
	}

	if b.accessPolicies[clusterName] == nil {
		b.accessPolicies[clusterName] = make(map[string][]*AccessPolicyAssociation)
	}

	assoc := &AccessPolicyAssociation{
		PolicyARN:    policyARN,
		ClusterName:  clusterName,
		PrincipalARN: principalARN,
		AccessScope:  accessScope,
		AssociatedAt: time.Now().UTC(),
	}
	b.accessPolicies[clusterName][principalARN] = append(b.accessPolicies[clusterName][principalARN], assoc)
	cp := *assoc

	return &cp, nil
}

// AssociateEncryptionConfig associates encryption configuration with a cluster.
func (b *InMemoryBackend) AssociateEncryptionConfig(
	clusterName string,
	configs []EncryptionConfig,
) ([]EncryptionConfig, error) {
	b.mu.Lock("AssociateEncryptionConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	stored := make([]EncryptionConfig, len(configs))
	copy(stored, configs)
	b.encryptionConfigs[clusterName] = append(b.encryptionConfigs[clusterName], stored...)

	result := make([]EncryptionConfig, len(b.encryptionConfigs[clusterName]))
	copy(result, b.encryptionConfigs[clusterName])

	return result, nil
}

// AssociateIdentityProviderConfig associates an identity provider configuration with a cluster.
func (b *InMemoryBackend) AssociateIdentityProviderConfig(
	clusterName, configType, name string,
	params, kv map[string]string,
) (*IdentityProviderConfig, error) {
	b.mu.Lock("AssociateIdentityProviderConfig")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.identityProviderConfigs[clusterName] == nil {
		b.identityProviderConfigs[clusterName] = make(map[string]*IdentityProviderConfig)
	}

	if _, ok := b.identityProviderConfigs[clusterName][name]; ok {
		return nil, fmt.Errorf(
			"%w: identity provider config %s already exists in cluster %s",
			ErrAlreadyExists,
			name,
			clusterName,
		)
	}

	t := tags.New("eks.idp." + clusterName + "." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	cfg := &IdentityProviderConfig{
		ClusterName: clusterName,
		Name:        name,
		Type:        configType,
		Status:      "ACTIVE",
		OIDC:        params,
		CreatedAt:   time.Now().UTC(),
		Tags:        t,
	}
	b.identityProviderConfigs[clusterName][name] = cfg
	cp := *cfg

	return &cp, nil
}

// CreateAddon creates a new managed add-on in a cluster.
func (b *InMemoryBackend) CreateAddon(
	clusterName, addonName, addonVersion, serviceAccountRoleARN string,
	kv map[string]string,
) (*Addon, error) {
	b.mu.Lock("CreateAddon")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.addons[clusterName] == nil {
		b.addons[clusterName] = make(map[string]*Addon)
	}

	if _, ok := b.addons[clusterName][addonName]; ok {
		return nil, fmt.Errorf("%w: addon %s already exists in cluster %s", ErrAlreadyExists, addonName, clusterName)
	}

	addonARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"addon/"+clusterName+"/"+addonName+"/"+stableID(clusterName+"/"+addonName),
	)

	t := tags.New("eks.addon." + clusterName + "." + addonName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	addon := &Addon{
		ClusterName:           clusterName,
		AddonName:             addonName,
		ARN:                   addonARN,
		AddonVersion:          addonVersion,
		ServiceAccountRoleARN: serviceAccountRoleARN,
		Status:                "ACTIVE",
		CreatedAt:             time.Now().UTC(),
		Tags:                  t,
	}
	b.addons[clusterName][addonName] = addon
	cp := *addon

	return &cp, nil
}

// CreateCapability creates a new EKS capability.
func (b *InMemoryBackend) CreateCapability(name, version string) (*Capability, error) {
	b.mu.Lock("CreateCapability")
	defer b.mu.Unlock()

	if _, ok := b.capabilities[name]; ok {
		return nil, fmt.Errorf("%w: capability %s already exists", ErrAlreadyExists, name)
	}

	capa := &Capability{
		Name:    name,
		Version: version,
		Status:  "ACTIVE",
	}
	b.capabilities[name] = capa
	cp := *capa

	return &cp, nil
}

// CreateEksAnywhereSubscription creates a new EKS Anywhere subscription.
func (b *InMemoryBackend) CreateEksAnywhereSubscription(
	name string,
	licenseQuantity int32,
	licenseType string,
	kv map[string]string,
) (*AnywhereSubscription, error) {
	b.mu.Lock("CreateEksAnywhereSubscription")
	defer b.mu.Unlock()

	id := stableID(name + strconv.FormatInt(time.Now().UnixNano(), 10))
	subARN := arn.Build("eks", b.region, b.accountID, "eks-anywhere-subscription/"+id)

	t := tags.New("eks.subscription." + id + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	sub := &AnywhereSubscription{
		ID:              id,
		ARN:             subARN,
		Name:            name,
		Status:          "ACTIVE",
		LicenseType:     licenseType,
		LicenseQuantity: licenseQuantity,
		CreatedAt:       time.Now().UTC(),
		Tags:            t,
	}
	b.subscriptions[id] = sub
	cp := *sub

	return &cp, nil
}

// CreateFargateProfile creates a new Fargate profile in a cluster.
func (b *InMemoryBackend) CreateFargateProfile(
	clusterName, profileName, podExecutionRoleARN string,
	selectors []FargateProfileSelector,
	kv map[string]string,
) (*FargateProfile, error) {
	b.mu.Lock("CreateFargateProfile")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.fargateProfiles[clusterName] == nil {
		b.fargateProfiles[clusterName] = make(map[string]*FargateProfile)
	}

	if _, ok := b.fargateProfiles[clusterName][profileName]; ok {
		return nil, fmt.Errorf(
			"%w: fargate profile %s already exists in cluster %s",
			ErrAlreadyExists,
			profileName,
			clusterName,
		)
	}

	profileARN := arn.Build(
		"eks",
		b.region,
		b.accountID,
		"fargateprofile/"+clusterName+"/"+profileName+"/"+stableID(clusterName+"/"+profileName),
	)

	t := tags.New("eks.fargate." + clusterName + "." + profileName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	sels := make([]FargateProfileSelector, len(selectors))
	copy(sels, selectors)

	profile := &FargateProfile{
		ClusterName:         clusterName,
		FargateProfileName:  profileName,
		ARN:                 profileARN,
		PodExecutionRoleARN: podExecutionRoleARN,
		Status:              "ACTIVE",
		Selectors:           sels,
		CreatedAt:           time.Now().UTC(),
		Tags:                t,
	}
	b.fargateProfiles[clusterName][profileName] = profile
	cp := *profile
	cp.Selectors = make([]FargateProfileSelector, len(profile.Selectors))
	copy(cp.Selectors, profile.Selectors)

	return &cp, nil
}

// CreatePodIdentityAssociation creates a new pod identity association in a cluster.
func (b *InMemoryBackend) CreatePodIdentityAssociation(
	clusterName, namespace, serviceAccount, roleARN string,
	kv map[string]string,
) (*PodIdentityAssociation, error) {
	b.mu.Lock("CreatePodIdentityAssociation")
	defer b.mu.Unlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if b.podIdentityAssociations[clusterName] == nil {
		b.podIdentityAssociations[clusterName] = make(map[string]*PodIdentityAssociation)
	}

	assocID := stableID(clusterName + "/" + namespace + "/" + serviceAccount)
	assocARN := arn.Build("eks", b.region, b.accountID, "podidentityassociation/"+clusterName+"/"+assocID)

	t := tags.New("eks.podidentity." + clusterName + "." + assocID + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	assoc := &PodIdentityAssociation{
		ClusterName:    clusterName,
		AssociationID:  assocID,
		ARN:            assocARN,
		Namespace:      namespace,
		ServiceAccount: serviceAccount,
		RoleARN:        roleARN,
		CreatedAt:      time.Now().UTC(),
		Tags:           t,
	}
	b.podIdentityAssociations[clusterName][assocID] = assoc
	cp := *assoc

	return &cp, nil
}
