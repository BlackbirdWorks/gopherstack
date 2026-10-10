package iot

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// RoleAlias represents an IoT role alias.
type RoleAlias struct {
	Tags                      map[string]string `json:"tags,omitempty"`
	RoleAlias                 string            `json:"roleAlias"`
	RoleAliasARN              string            `json:"roleAliasArn"`
	RoleARN                   string            `json:"roleArn"`
	Owner                     string            `json:"owner,omitempty"`
	CredentialDurationSeconds int               `json:"credentialDurationSeconds,omitempty"`
	CreationDate              float64           `json:"creationDate,omitempty"`
	LastModifiedDate          float64           `json:"lastModifiedDate,omitempty"`
}

func cloneRoleAlias(ra *RoleAlias) *RoleAlias {
	cp := *ra

	return &cp
}

func (b *InMemoryBackend) roleAliasARN(alias string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("rolealias/%s", alias))
}

// CreateRoleAliasInput holds input for CreateRoleAlias.
type CreateRoleAliasInput struct {
	RoleAlias string `json:"roleAlias"`
	RoleARN   string `json:"roleArn"`
	// []types.Tag on the wire, not a map (serializers.go:4283, aws-sdk-go-v2/service/iot@v1.77.4).
	Tags                      []tags.KV `json:"tags,omitempty"`
	CredentialDurationSeconds int       `json:"credentialDurationSeconds,omitempty"`
}

func (b *InMemoryBackend) CreateRoleAlias(input *CreateRoleAliasInput) (*RoleAlias, error) {
	b.mu.Lock("CreateRoleAlias")
	defer b.mu.Unlock()

	if b.roleAliases.Has(input.RoleAlias) {
		return nil, fmt.Errorf(
			"role alias %q already exists: %w",
			input.RoleAlias,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().Unix())
	ra := &RoleAlias{
		RoleAlias:                 input.RoleAlias,
		RoleAliasARN:              b.roleAliasARN(input.RoleAlias),
		RoleARN:                   input.RoleARN,
		CredentialDurationSeconds: input.CredentialDurationSeconds,
		Tags:                      tags.MapFromKV(input.Tags),
		CreationDate:              now,
		LastModifiedDate:          now,
	}
	if ra.CredentialDurationSeconds == 0 {
		ra.CredentialDurationSeconds = 3600
	}
	b.roleAliases.Put(ra)
	b.putResourceTagsLocked(ra.RoleAliasARN, ra.Tags)

	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) DescribeRoleAlias(alias string) (*RoleAlias, error) {
	b.mu.RLock("DescribeRoleAlias")
	defer b.mu.RUnlock()

	ra, ok := b.roleAliases.Get(alias)
	if !ok {
		return nil, fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}

	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) ListRoleAliases() []*RoleAlias {
	b.mu.RLock("ListRoleAliases")
	defer b.mu.RUnlock()

	out := make([]*RoleAlias, 0, b.roleAliases.Len())
	for _, v := range b.roleAliases.Snapshot() {
		out = append(out, cloneRoleAlias(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateRoleAlias(
	alias, roleARN string,
	credDuration int,
) (*RoleAlias, error) {
	b.mu.Lock("UpdateRoleAlias")
	defer b.mu.Unlock()

	ra, ok := b.roleAliases.Get(alias)
	if !ok {
		return nil, fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}
	if roleARN != "" {
		ra.RoleARN = roleARN
	}
	if credDuration > 0 {
		ra.CredentialDurationSeconds = credDuration
	}
	ra.LastModifiedDate = float64(time.Now().Unix())

	return cloneRoleAlias(ra), nil
}

func (b *InMemoryBackend) DeleteRoleAlias(alias string) error {
	b.mu.Lock("DeleteRoleAlias")
	defer b.mu.Unlock()

	if !b.roleAliases.Has(alias) {
		return fmt.Errorf("role alias %q not found: %w", alias, ErrResourceNotFound)
	}
	b.roleAliases.Delete(alias)
	delete(b.resourceTags, b.roleAliasARN(alias))

	return nil
}

// DomainConfiguration represents an IoT domain configuration.
type DomainConfiguration struct {
	Tags                      map[string]string                `json:"tags,omitempty"`
	AuthorizerConfig          *DomainAuthorizerConfig          `json:"authorizerConfig,omitempty"`
	ClientCertificateConfig   *DomainClientCertificateConfig   `json:"clientCertificateConfig,omitempty"`
	ServerCertificateConfig   *DomainServerCertificateConfig   `json:"serverCertificateConfig,omitempty"`
	TLSConfig                 *DomainTLSConfig                 `json:"tlsConfig,omitempty"`
	ServiceType               string                           `json:"serviceType,omitempty"`
	DomainConfigurationARN    string                           `json:"domainConfigurationArn"`
	DomainName                string                           `json:"domainName,omitempty"`
	DomainConfigurationName   string                           `json:"domainConfigurationName"`
	DomainConfigurationStatus string                           `json:"domainConfigurationStatus"`
	DomainType                string                           `json:"domainType,omitempty"`
	ApplicationProtocol       string                           `json:"applicationProtocol,omitempty"`
	AuthenticationType        string                           `json:"authenticationType,omitempty"`
	ValidationCertificateARN  string                           `json:"validationCertificateArn,omitempty"`
	ServerCertificateARNs     []string                         `json:"serverCertificateArns,omitempty"`
	ServerCertificates        []DomainServerCertificateSummary `json:"serverCertificates,omitempty"`
	CreationDate              float64                          `json:"creationDate,omitempty"`
	LastModifiedDate          float64                          `json:"lastModifiedDate,omitempty"`
}

// DomainServerCertificateSummary mirrors types.ServerCertificateSummary.
type DomainServerCertificateSummary struct {
	ServerCertificateARN          string `json:"serverCertificateArn"`
	ServerCertificateStatus       string `json:"serverCertificateStatus,omitempty"`
	ServerCertificateStatusDetail string `json:"serverCertificateStatusDetail,omitempty"`
}

// ServerCertificateChecker reports the ACM validity of a domain configuration's server certificate.
type ServerCertificateChecker interface {
	ServerCertificateStatus(certificateARN string) (status, detail string)
}

// DomainAuthorizerConfig mirrors types.AuthorizerConfig.
type DomainAuthorizerConfig struct {
	AllowAuthorizerOverride *bool  `json:"allowAuthorizerOverride,omitempty"`
	DefaultAuthorizerName   string `json:"defaultAuthorizerName,omitempty"`
}

// DomainClientCertificateConfig mirrors types.ClientCertificateConfig.
type DomainClientCertificateConfig struct {
	ClientCertificateCallbackArn string `json:"clientCertificateCallbackArn,omitempty"`
}

// DomainServerCertificateConfig mirrors types.ServerCertificateConfig.
type DomainServerCertificateConfig struct {
	EnableOCSPCheck            *bool  `json:"enableOCSPCheck,omitempty"`
	OcspAuthorizedResponderArn string `json:"ocspAuthorizedResponderArn,omitempty"`
	OcspLambdaArn              string `json:"ocspLambdaArn,omitempty"`
}

// DomainTLSConfig mirrors types.TlsConfig.
type DomainTLSConfig struct {
	SecurityPolicy string `json:"securityPolicy,omitempty"`
}

func cloneDomainConfig(dc *DomainConfiguration) *DomainConfiguration {
	cp := *dc
	if dc.AuthorizerConfig != nil {
		v := *dc.AuthorizerConfig
		cp.AuthorizerConfig = &v
	}

	if dc.ClientCertificateConfig != nil {
		v := *dc.ClientCertificateConfig
		cp.ClientCertificateConfig = &v
	}

	if dc.ServerCertificateConfig != nil {
		v := *dc.ServerCertificateConfig
		cp.ServerCertificateConfig = &v
	}

	if dc.TLSConfig != nil {
		v := *dc.TLSConfig
		cp.TLSConfig = &v
	}

	cp.ServerCertificateARNs = append([]string(nil), dc.ServerCertificateARNs...)

	return &cp
}

func (b *InMemoryBackend) domainConfigARN(name string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("domainconfiguration/%s", name))
}

// CreateDomainConfigurationInput holds input for CreateDomainConfiguration.
type CreateDomainConfigurationInput struct {
	AuthorizerConfig         *DomainAuthorizerConfig        `json:"authorizerConfig,omitempty"`
	ClientCertificateConfig  *DomainClientCertificateConfig `json:"clientCertificateConfig,omitempty"`
	ServerCertificateConfig  *DomainServerCertificateConfig `json:"serverCertificateConfig,omitempty"`
	TLSConfig                *DomainTLSConfig               `json:"tlsConfig,omitempty"`
	DomainConfigurationName  string                         `json:"domainConfigurationName"`
	DomainName               string                         `json:"domainName,omitempty"`
	ServiceType              string                         `json:"serviceType,omitempty"`
	ApplicationProtocol      string                         `json:"applicationProtocol,omitempty"`
	AuthenticationType       string                         `json:"authenticationType,omitempty"`
	ValidationCertificateARN string                         `json:"validationCertificateArn,omitempty"`
	ServerCertificateARNs    []string                       `json:"serverCertificateArns,omitempty"`
	// []types.Tag on the wire, not a map (serializers.go:2450, aws-sdk-go-v2/service/iot@v1.77.4).
	Tags []tags.KV `json:"tags,omitempty"`
}

// UpdateDomainConfigurationInput holds input for UpdateDomainConfiguration.
type UpdateDomainConfigurationInput struct {
	AuthorizerConfig          *DomainAuthorizerConfig        `json:"authorizerConfig"`
	ClientCertificateConfig   *DomainClientCertificateConfig `json:"clientCertificateConfig"`
	ServerCertificateConfig   *DomainServerCertificateConfig `json:"serverCertificateConfig"`
	TLSConfig                 *DomainTLSConfig               `json:"tlsConfig"`
	DomainConfigurationStatus string                         `json:"domainConfigurationStatus"`
	ApplicationProtocol       string                         `json:"applicationProtocol"`
	AuthenticationType        string                         `json:"authenticationType"`
	RemoveAuthorizerConfig    bool                           `json:"removeAuthorizerConfig"`
}

func (b *InMemoryBackend) CreateDomainConfiguration(
	input *CreateDomainConfigurationInput,
) (*DomainConfiguration, error) {
	if len(input.ServerCertificateARNs) > 1 {
		return nil, fmt.Errorf("%w: only one serverCertificateArn can be specified", ErrValidation)
	}

	b.mu.Lock("CreateDomainConfiguration")
	defer b.mu.Unlock()

	if b.domainConfigs.Has(input.DomainConfigurationName) {
		return nil, fmt.Errorf(
			"domain configuration %q already exists: %w",
			input.DomainConfigurationName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().Unix())
	dc := &DomainConfiguration{
		DomainConfigurationName:   input.DomainConfigurationName,
		DomainConfigurationARN:    b.domainConfigARN(input.DomainConfigurationName),
		DomainName:                input.DomainName,
		ServiceType:               input.ServiceType,
		DomainConfigurationStatus: "ENABLED",
		ApplicationProtocol:       input.ApplicationProtocol,
		AuthenticationType:        input.AuthenticationType,
		Tags:                      tags.MapFromKV(input.Tags),
		CreationDate:              now,
		LastModifiedDate:          now,
		AuthorizerConfig:          input.AuthorizerConfig,
		ClientCertificateConfig:   input.ClientCertificateConfig,
		ServerCertificateConfig:   input.ServerCertificateConfig,
		TLSConfig:                 input.TLSConfig,
		ValidationCertificateARN:  input.ValidationCertificateARN,
		ServerCertificateARNs:     append([]string(nil), input.ServerCertificateARNs...),
	}
	if dc.ServiceType == "" {
		dc.ServiceType = "DATA"
	}
	b.domainConfigs.Put(dc)
	b.putResourceTagsLocked(dc.DomainConfigurationARN, dc.Tags)

	return cloneDomainConfig(dc), nil
}

func (b *InMemoryBackend) DescribeDomainConfiguration(name string) (*DomainConfiguration, error) {
	b.mu.RLock("DescribeDomainConfiguration")
	defer b.mu.RUnlock()

	dc, ok := b.domainConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}

	out := cloneDomainConfig(dc)
	checker := b.serverCertChecker
	for _, certARN := range out.ServerCertificateARNs {
		sum := DomainServerCertificateSummary{ServerCertificateARN: certARN}
		if checker != nil {
			sum.ServerCertificateStatus, sum.ServerCertificateStatusDetail = checker.ServerCertificateStatus(certARN)
		}
		out.ServerCertificates = append(out.ServerCertificates, sum)
	}

	return out, nil
}

// SetServerCertificateChecker wires the ACM certificate validity lookup.
func (b *InMemoryBackend) SetServerCertificateChecker(c ServerCertificateChecker) {
	b.mu.Lock("SetServerCertificateChecker")
	defer b.mu.Unlock()

	b.serverCertChecker = c
}

func (b *InMemoryBackend) certificateChecker() ServerCertificateChecker {
	b.mu.RLock("certificateChecker")
	defer b.mu.RUnlock()

	return b.serverCertChecker
}

func (b *InMemoryBackend) ListDomainConfigurations() []*DomainConfiguration {
	b.mu.RLock("ListDomainConfigurations")
	defer b.mu.RUnlock()

	out := make([]*DomainConfiguration, 0, b.domainConfigs.Len())
	for _, v := range b.domainConfigs.Snapshot() {
		out = append(out, cloneDomainConfig(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateDomainConfiguration(
	name string, in *UpdateDomainConfigurationInput,
) (*DomainConfiguration, error) {
	if in.RemoveAuthorizerConfig && in.AuthorizerConfig != nil {
		return nil, fmt.Errorf("%w: removeAuthorizerConfig can't be set with authorizerConfig", ErrValidation)
	}

	b.mu.Lock("UpdateDomainConfiguration")
	defer b.mu.Unlock()

	dc, ok := b.domainConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}
	if in.DomainConfigurationStatus != "" {
		dc.DomainConfigurationStatus = in.DomainConfigurationStatus
	}
	if in.ApplicationProtocol != "" {
		dc.ApplicationProtocol = in.ApplicationProtocol
	}
	if in.AuthenticationType != "" {
		dc.AuthenticationType = in.AuthenticationType
	}
	if in.RemoveAuthorizerConfig {
		dc.AuthorizerConfig = nil
	}
	if in.AuthorizerConfig != nil {
		dc.AuthorizerConfig = in.AuthorizerConfig
	}
	if in.ClientCertificateConfig != nil {
		dc.ClientCertificateConfig = in.ClientCertificateConfig
	}
	if in.ServerCertificateConfig != nil {
		dc.ServerCertificateConfig = in.ServerCertificateConfig
	}
	if in.TLSConfig != nil {
		dc.TLSConfig = in.TLSConfig
	}
	dc.LastModifiedDate = float64(time.Now().Unix())

	return cloneDomainConfig(dc), nil
}

func (b *InMemoryBackend) DeleteDomainConfiguration(name string) error {
	b.mu.Lock("DeleteDomainConfiguration")
	defer b.mu.Unlock()

	if !b.domainConfigs.Has(name) {
		return fmt.Errorf("domain configuration %q not found: %w", name, ErrResourceNotFound)
	}
	b.domainConfigs.Delete(name)
	delete(b.resourceTags, b.domainConfigARN(name))

	return nil
}

// ProvisioningHook identifies a pre-provisioning hook Lambda function
// (types.ProvisioningHook, aws-sdk-go-v2/service/iot@v1.77.4).
type ProvisioningHook struct {
	TargetARN      string `json:"targetArn"`
	PayloadVersion string `json:"payloadVersion,omitempty"`
}

// ProvisioningTemplate represents an IoT fleet provisioning template.
type ProvisioningTemplate struct {
	PreProvisioningHook *ProvisioningHook `json:"preProvisioningHook,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
	TemplateName        string            `json:"templateName"`
	TemplateARN         string            `json:"templateArn"`
	Description         string            `json:"description,omitempty"`
	TemplateBody        string            `json:"templateBody,omitempty"`
	ProvisioningRoleARN string            `json:"provisioningRoleArn,omitempty"`
	TemplateType        string            `json:"type,omitempty"`
	Enabled             bool              `json:"enabled"`
	DefaultVersionID    int32             `json:"defaultVersionId,omitempty"`
	CreationDate        float64           `json:"creationDate,omitempty"`
	LastModifiedDate    float64           `json:"lastModifiedDate,omitempty"`
}

// ProvisioningTemplateVersion represents a version of a provisioning template.
type ProvisioningTemplateVersion struct {
	TemplateBody     string  `json:"templateBody,omitempty"`
	CreationDate     float64 `json:"creationDate,omitempty"`
	VersionID        int32   `json:"versionId"`
	IsDefaultVersion bool    `json:"isDefaultVersion"`
}

func cloneProvTemplate(pt *ProvisioningTemplate) *ProvisioningTemplate {
	cp := *pt

	return &cp
}

func (b *InMemoryBackend) provTemplateARN(name string) string {
	return arn.Build("iot", b.region, b.accountID, fmt.Sprintf("provisioningtemplate/%s", name))
}

// CreateProvisioningTemplateInput holds input for CreateProvisioningTemplate.
type CreateProvisioningTemplateInput struct {
	PreProvisioningHook *ProvisioningHook `json:"preProvisioningHook,omitempty"`
	TemplateName        string            `json:"templateName"`
	Description         string            `json:"description,omitempty"`
	TemplateBody        string            `json:"templateBody,omitempty"`
	ProvisioningRoleARN string            `json:"provisioningRoleArn,omitempty"`
	Type                string            `json:"type,omitempty"`
	// []types.Tag on the wire, not a map (serializers.go:4052, aws-sdk-go-v2/service/iot@v1.77.4).
	Tags    []tags.KV `json:"tags,omitempty"`
	Enabled bool      `json:"enabled"`
}

func (b *InMemoryBackend) CreateProvisioningTemplate(
	input *CreateProvisioningTemplateInput,
) (*ProvisioningTemplate, error) {
	b.mu.Lock("CreateProvisioningTemplate")
	defer b.mu.Unlock()

	if b.provTemplates.Has(input.TemplateName) {
		return nil, fmt.Errorf(
			"provisioning template %q already exists: %w",
			input.TemplateName,
			ErrAlreadyExists,
		)
	}
	now := float64(time.Now().Unix())
	pt := &ProvisioningTemplate{
		PreProvisioningHook: input.PreProvisioningHook,
		TemplateName:        input.TemplateName,
		TemplateARN:         b.provTemplateARN(input.TemplateName),
		Description:         input.Description,
		TemplateBody:        input.TemplateBody,
		ProvisioningRoleARN: input.ProvisioningRoleARN,
		TemplateType:        input.Type,
		Enabled:             input.Enabled,
		DefaultVersionID:    1,
		Tags:                tags.MapFromKV(input.Tags),
		CreationDate:        now,
		LastModifiedDate:    now,
	}
	b.provTemplates.Put(pt)
	b.putResourceTagsLocked(pt.TemplateARN, pt.Tags)
	// Create initial version.
	b.provTemplateVersions[input.TemplateName] = []*ProvisioningTemplateVersion{
		{VersionID: 1, TemplateBody: input.TemplateBody, CreationDate: now, IsDefaultVersion: true},
	}

	return cloneProvTemplate(pt), nil
}

func (b *InMemoryBackend) DescribeProvisioningTemplate(name string) (*ProvisioningTemplate, error) {
	b.mu.RLock("DescribeProvisioningTemplate")
	defer b.mu.RUnlock()

	pt, ok := b.provTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}

	return cloneProvTemplate(pt), nil
}

func (b *InMemoryBackend) ListProvisioningTemplates() []*ProvisioningTemplate {
	b.mu.RLock("ListProvisioningTemplates")
	defer b.mu.RUnlock()

	out := make([]*ProvisioningTemplate, 0, b.provTemplates.Len())
	for _, v := range b.provTemplates.Snapshot() {
		out = append(out, cloneProvTemplate(v))
	}

	return out
}

func (b *InMemoryBackend) UpdateProvisioningTemplate(
	name, description string,
	enabled *bool,
	provRoleARN string,
	defaultVersionID *int32,
	preProvisioningHook *ProvisioningHook,
	removePreProvisioningHook bool,
) error {
	b.mu.Lock("UpdateProvisioningTemplate")
	defer b.mu.Unlock()

	pt, ok := b.provTemplates.Get(name)
	if !ok {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	if description != "" {
		pt.Description = description
	}
	if enabled != nil {
		pt.Enabled = *enabled
	}
	if provRoleARN != "" {
		pt.ProvisioningRoleARN = provRoleARN
	}
	if defaultVersionID != nil {
		pt.DefaultVersionID = *defaultVersionID
	}
	if removePreProvisioningHook {
		pt.PreProvisioningHook = nil
	} else if preProvisioningHook != nil {
		pt.PreProvisioningHook = preProvisioningHook
	}
	pt.LastModifiedDate = float64(time.Now().Unix())

	return nil
}

func (b *InMemoryBackend) DeleteProvisioningTemplate(name string) error {
	b.mu.Lock("DeleteProvisioningTemplate")
	defer b.mu.Unlock()

	if !b.provTemplates.Has(name) {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	b.provTemplates.Delete(name)
	delete(b.provTemplateVersions, name)
	delete(b.resourceTags, b.provTemplateARN(name))

	return nil
}

func (b *InMemoryBackend) CreateProvisioningTemplateVersion(
	name, body string,
	setAsDefault bool,
) (*ProvisioningTemplateVersion, error) {
	b.mu.Lock("CreateProvisioningTemplateVersion")
	defer b.mu.Unlock()

	pt, ok := b.provTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	versions := b.provTemplateVersions[name]
	newID := int32(len(versions) + 1) //nolint:gosec // safe: version count is always small
	v := &ProvisioningTemplateVersion{
		VersionID:        newID,
		TemplateBody:     body,
		CreationDate:     float64(time.Now().Unix()),
		IsDefaultVersion: setAsDefault,
	}
	if setAsDefault {
		for _, existing := range versions {
			existing.IsDefaultVersion = false
		}
		pt.DefaultVersionID = newID
	}
	b.provTemplateVersions[name] = append(versions, v)

	return v, nil
}

func (b *InMemoryBackend) ListProvisioningTemplateVersions(
	name string,
) ([]*ProvisioningTemplateVersion, error) {
	b.mu.RLock("ListProvisioningTemplateVersions")
	defer b.mu.RUnlock()

	if !b.provTemplates.Has(name) {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	src := b.provTemplateVersions[name]
	out := make([]*ProvisioningTemplateVersion, len(src))
	copy(out, src)

	return out, nil
}

func (b *InMemoryBackend) DeleteProvisioningTemplateVersion(name string, versionID int32) error {
	b.mu.Lock("DeleteProvisioningTemplateVersion")
	defer b.mu.Unlock()

	if !b.provTemplates.Has(name) {
		return fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}
	versions := b.provTemplateVersions[name]
	for i, v := range versions {
		if v.VersionID == versionID {
			b.provTemplateVersions[name] = append(versions[:i], versions[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("version %d not found: %w", versionID, ErrResourceNotFound)
}

// DescribeProvisioningTemplateVersion returns a specific stored version of a
// provisioning template.
func (b *InMemoryBackend) DescribeProvisioningTemplateVersion(
	name string, versionID int32,
) (*ProvisioningTemplateVersion, error) {
	b.mu.RLock("DescribeProvisioningTemplateVersion")
	defer b.mu.RUnlock()

	if !b.provTemplates.Has(name) {
		return nil, fmt.Errorf("provisioning template %q not found: %w", name, ErrResourceNotFound)
	}

	for _, v := range b.provTemplateVersions[name] {
		if v.VersionID == versionID {
			cp := *v

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("version %d not found: %w", versionID, ErrResourceNotFound)
}
