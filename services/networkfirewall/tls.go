package networkfirewall

import (
	"context"
	"slices"
	"strings"
	"time"
)

// CreateTLSInspectionConfigurationInput is the CreateTLSInspectionConfiguration request.
type CreateTLSInspectionConfigurationInput struct {
	EncryptionConfiguration        *EncryptionConfiguration    `json:"EncryptionConfiguration"`
	TLSInspectionConfiguration     *TLSInspectionConfiguration `json:"TLSInspectionConfiguration"`
	TLSInspectionConfigurationName string                      `json:"TLSInspectionConfigurationName"`
	Description                    string                      `json:"Description"`
	Tags                           []Tag                       `json:"Tags"`
}

// TLSInspectionConfigurationOutput is the Create/Update response.
type TLSInspectionConfigurationOutput struct {
	UpdateToken                        string                             `json:"UpdateToken"`
	TLSInspectionConfigurationResponse TLSInspectionConfigurationResponse `json:"TLSInspectionConfigurationResponse"`
}

// DescribeTLSInspectionConfigurationInput is the Describe request.
type DescribeTLSInspectionConfigurationInput struct {
	TLSInspectionConfigurationArn  string `json:"TLSInspectionConfigurationArn"`
	TLSInspectionConfigurationName string `json:"TLSInspectionConfigurationName"`
}

// DescribeTLSInspectionConfigurationOutput is the Describe response.
type DescribeTLSInspectionConfigurationOutput struct {
	TLSInspectionConfiguration         *TLSInspectionConfiguration        `json:"TLSInspectionConfiguration,omitempty"`
	UpdateToken                        string                             `json:"UpdateToken"`
	TLSInspectionConfigurationResponse TLSInspectionConfigurationResponse `json:"TLSInspectionConfigurationResponse"`
}

// ListTLSInspectionConfigurationsInput is the List request.
type ListTLSInspectionConfigurationsInput struct {
	MaxResults *int32 `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

// ListTLSInspectionConfigurationsOutput is the List response.
type ListTLSInspectionConfigurationsOutput struct {
	NextToken                   string             `json:"NextToken,omitempty"`
	TLSInspectionConfigurations []ResourceMetadata `json:"TLSInspectionConfigurations"`
}

// UpdateTLSInspectionConfigurationInput is the Update request.
type UpdateTLSInspectionConfigurationInput struct {
	EncryptionConfiguration        *EncryptionConfiguration    `json:"EncryptionConfiguration"`
	TLSInspectionConfiguration     *TLSInspectionConfiguration `json:"TLSInspectionConfiguration"`
	UpdateToken                    string                      `json:"UpdateToken"`
	TLSInspectionConfigurationArn  string                      `json:"TLSInspectionConfigurationArn"`
	TLSInspectionConfigurationName string                      `json:"TLSInspectionConfigurationName"`
	Description                    string                      `json:"Description"`
}

// DeleteTLSInspectionConfigurationInput is the Delete request.
type DeleteTLSInspectionConfigurationInput = DescribeTLSInspectionConfigurationInput

// DeleteTLSInspectionConfigurationOutput is the Delete response.
type DeleteTLSInspectionConfigurationOutput struct {
	TLSInspectionConfigurationResponse TLSInspectionConfigurationResponse `json:"TLSInspectionConfigurationResponse"`
}

func validateTLSConfiguration(c *TLSInspectionConfiguration) error {
	if c == nil || len(c.ServerCertificateConfigurations) == 0 {
		return invalid("TLSInspectionConfiguration needs at least one ServerCertificateConfiguration")
	}

	return nil
}

func (b *InMemoryBackend) tlsResponseLocked(r *tlsRecord) TLSInspectionConfigurationResponse {
	var users int32

	for _, p := range b.policies.All() {
		if p.Policy.TLSInspectionConfigurationArn == r.ARN {
			users++
		}
	}

	resp := TLSInspectionConfigurationResponse{
		TLSInspectionConfigurationArn:    r.ARN,
		TLSInspectionConfigurationID:     r.ID,
		TLSInspectionConfigurationName:   r.Name,
		Description:                      r.Description,
		TLSInspectionConfigurationStatus: resourceStatusActive,
		NumberOfAssociations:             new(users),
		LastModifiedTime:                 epoch(r.LastModified),
		EncryptionConfiguration:          normalizeEncryption(r.Encryption),
		Tags:                             tagList(r.Tags),
	}

	for _, c := range r.Config.ServerCertificateConfigurations {
		for _, cert := range c.ServerCertificates {
			resp.Certificates = append(resp.Certificates,
				TLSCertificateData{CertificateArn: cert.ResourceArn, Status: resourceStatusActive})
		}

		if c.CertificateAuthorityArn != "" && resp.CertificateAuthority == nil {
			resp.CertificateAuthority = &TLSCertificateData{
				CertificateArn: c.CertificateAuthorityArn, Status: resourceStatusActive,
			}
		}
	}

	return resp
}

func (b *InMemoryBackend) findTLSLocked(arnStr, name string) (*tlsRecord, error) {
	if err := targetNeeded(arnStr, name); err != nil {
		return nil, err
	}

	key := arnStr
	if key == "" {
		key = b.arnFor(kindTLS, name)
	}

	r, ok := b.tlsConfigs.Get(key)
	if !ok {
		return nil, notFound("TLS inspection configuration", strings.TrimPrefix(key, "arn:"))
	}

	return r, nil
}

// CreateTLSInspectionConfiguration creates a TLS inspection configuration.
func (b *InMemoryBackend) CreateTLSInspectionConfiguration(
	_ context.Context, in *CreateTLSInspectionConfigurationInput,
) (*TLSInspectionConfigurationOutput, error) {
	if err := validateName("TLSInspectionConfigurationName", in.TLSInspectionConfigurationName); err != nil {
		return nil, err
	}

	for _, check := range []error{
		validateTLSConfiguration(in.TLSInspectionConfiguration), validateDescription(in.Description),
		validateEncryption(in.EncryptionConfiguration), validateTags(tagMap(in.Tags)),
	} {
		if check != nil {
			return nil, check
		}
	}

	b.mu.Lock("CreateTLSInspectionConfiguration")
	defer b.mu.Unlock()

	cfgARN := b.arnFor(kindTLS, in.TLSInspectionConfigurationName)
	if b.tlsConfigs.Has(cfgARN) {
		return nil, invalid("A TLS inspection configuration named %s already exists", in.TLSInspectionConfigurationName)
	}

	r := &tlsRecord{
		Name:         in.TLSInspectionConfigurationName,
		ID:           newResourceID(),
		ARN:          cfgARN,
		Description:  in.Description,
		UpdateToken:  newToken(),
		Config:       *in.TLSInspectionConfiguration,
		Encryption:   normalizeEncryption(in.EncryptionConfiguration),
		Tags:         tagMap(in.Tags),
		LastModified: time.Now(),
	}

	b.tlsConfigs.Put(r)

	return &TLSInspectionConfigurationOutput{
		UpdateToken:                        r.UpdateToken,
		TLSInspectionConfigurationResponse: b.tlsResponseLocked(r),
	}, nil
}

// DescribeTLSInspectionConfiguration returns a TLS configuration and its body.
func (b *InMemoryBackend) DescribeTLSInspectionConfiguration(
	_ context.Context, in *DescribeTLSInspectionConfigurationInput,
) (*DescribeTLSInspectionConfigurationOutput, error) {
	b.mu.RLock("DescribeTLSInspectionConfiguration")
	defer b.mu.RUnlock()

	r, err := b.findTLSLocked(in.TLSInspectionConfigurationArn, in.TLSInspectionConfigurationName)
	if err != nil {
		return nil, err
	}

	body := r.Config

	return &DescribeTLSInspectionConfigurationOutput{
		TLSInspectionConfiguration: &body, UpdateToken: r.UpdateToken,
		TLSInspectionConfigurationResponse: b.tlsResponseLocked(r),
	}, nil
}

// ListTLSInspectionConfigurations lists TLS configurations by name.
func (b *InMemoryBackend) ListTLSInspectionConfigurations(
	_ context.Context, in *ListTLSInspectionConfigurationsInput,
) (*ListTLSInspectionConfigurationsOutput, error) {
	b.mu.RLock("ListTLSInspectionConfigurations")
	defer b.mu.RUnlock()

	all := make([]ResourceMetadata, 0, b.tlsConfigs.Len())
	for _, r := range b.tlsConfigs.All() {
		all = append(all, ResourceMetadata{Arn: r.ARN, Name: r.Name})
	}

	slices.SortFunc(all, func(a, c ResourceMetadata) int { return strings.Compare(a.Name, c.Name) })

	pg, err := paginate(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListTLSInspectionConfigurationsOutput{TLSInspectionConfigurations: pg.Data, NextToken: pg.Next}, nil
}

// UpdateTLSInspectionConfiguration replaces a TLS configuration body, guarded by the update token.
func (b *InMemoryBackend) UpdateTLSInspectionConfiguration(
	_ context.Context, in *UpdateTLSInspectionConfigurationInput,
) (*TLSInspectionConfigurationOutput, error) {
	for _, check := range []error{
		validateTLSConfiguration(in.TLSInspectionConfiguration), validateDescription(in.Description),
		validateEncryption(in.EncryptionConfiguration),
	} {
		if check != nil {
			return nil, check
		}
	}

	b.mu.Lock("UpdateTLSInspectionConfiguration")
	defer b.mu.Unlock()

	r, err := b.findTLSLocked(in.TLSInspectionConfigurationArn, in.TLSInspectionConfigurationName)
	if err != nil {
		return nil, err
	}

	if err = requireToken(r.UpdateToken, in.UpdateToken); err != nil {
		return nil, err
	}

	r.Description = in.Description
	r.Config = *in.TLSInspectionConfiguration
	r.Encryption = normalizeEncryption(in.EncryptionConfiguration)
	r.UpdateToken = newToken()
	r.LastModified = time.Now()

	return &TLSInspectionConfigurationOutput{
		UpdateToken:                        r.UpdateToken,
		TLSInspectionConfigurationResponse: b.tlsResponseLocked(r),
	}, nil
}

// DeleteTLSInspectionConfiguration deletes a configuration that no policy references.
func (b *InMemoryBackend) DeleteTLSInspectionConfiguration(
	_ context.Context, in *DeleteTLSInspectionConfigurationInput,
) (*DeleteTLSInspectionConfigurationOutput, error) {
	b.mu.Lock("DeleteTLSInspectionConfiguration")
	defer b.mu.Unlock()

	r, err := b.findTLSLocked(in.TLSInspectionConfigurationArn, in.TLSInspectionConfigurationName)
	if err != nil {
		return nil, err
	}

	for _, p := range b.policies.All() {
		if p.Policy.TLSInspectionConfigurationArn == r.ARN {
			return nil, invalidOp("The TLS inspection configuration %s is in use by firewall policy %s", r.Name, p.Name)
		}
	}

	resp := b.tlsResponseLocked(r)
	resp.TLSInspectionConfigurationStatus = statusDeleting

	b.tlsConfigs.Delete(r.ARN)

	return &DeleteTLSInspectionConfigurationOutput{TLSInspectionConfigurationResponse: resp}, nil
}
