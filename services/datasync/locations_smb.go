package datasync

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// --- SMB ---

func (b *InMemoryBackend) CreateLocationSmb(
	serverHostname, subdirectory, domain, user, password, authenticationType string,
	mountOptions *MountOptions,
	agentArns []string,
	tags map[string]string,
	smbKerberos SmbKerberosConfig,
	secretConfig SecretConfig,
) (*Location, error) {
	b.mu.Lock("CreateLocationSmb")
	defer b.mu.Unlock()

	if err := b.validateAgentArns(agentArns); err != nil {
		return nil, err
	}

	id := newID()
	locationArn := b.locationARN(id)
	now := time.Now().UTC()

	sub := strings.TrimPrefix(subdirectory, "/")
	locationURI := fmt.Sprintf("smb://%s/%s", serverHostname, sub)

	locationTags := make(map[string]string)
	maps.Copy(locationTags, tags)

	if authenticationType == "" {
		authenticationType = smbAuthTypeNTLM
	}

	cfg := &storedSmbConfig{
		ServerHostname:     serverHostname,
		Domain:             domain,
		User:               user,
		Password:           password,
		AuthenticationType: authenticationType,
		AgentArns:          agentArns,
		KerberosPrincipal:  smbKerberos.KerberosPrincipal,
		KerberosKeytab:     smbKerberos.KerberosKeytab,
		KerberosKrb5Conf:   smbKerberos.KerberosKrb5Conf,
		DNSIPAddresses:     smbKerberos.DNSIPAddresses,
		CmkSecretConfig:    toStoredCmkSecretConfig(secretConfig.Cmk),
		CustomSecretConfig: toStoredCustomSecretConfig(secretConfig.Custom),
	}

	if mountOptions != nil {
		cfg.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	l := &storedLocation{
		LocationArn:  locationArn,
		LocationURI:  locationURI,
		Subdirectory: subdirectory,
		LocationType: locationTypeSMB,
		CreationTime: now,
		Tags:         locationTags,
		Smb:          cfg,
	}
	b.locations.Put(l)

	if len(locationTags) > 0 {
		b.tags[locationArn] = make(map[string]string)
		maps.Copy(b.tags[locationArn], locationTags)
	}

	cp := l.toLocation()

	return &cp, nil
}

func (b *InMemoryBackend) DescribeLocationSmb(locationArn string) (*LocationSmb, error) {
	b.mu.RLock("DescribeLocationSmb")
	defer b.mu.RUnlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeSMB {
		return nil, ErrNotFound
	}

	out := &LocationSmb{
		LocationArn:  l.LocationArn,
		LocationURI:  l.LocationURI,
		Subdirectory: l.Subdirectory,
		CreationTime: l.CreationTime,
	}

	if l.Smb != nil {
		out.ServerHostname = l.Smb.ServerHostname
		out.Domain = l.Smb.Domain
		out.User = l.Smb.User
		out.AuthenticationType = l.Smb.AuthenticationType
		out.AgentArns = l.Smb.AgentArns
		out.KerberosPrincipal = l.Smb.KerberosPrincipal
		out.DNSIPAddresses = l.Smb.DNSIPAddresses
		out.CmkSecretConfig = fromStoredCmkSecretConfig(l.Smb.CmkSecretConfig)
		out.CustomSecretConfig = fromStoredCustomSecretConfig(l.Smb.CustomSecretConfig)

		if l.Smb.MountOptions != nil {
			out.MountOptions = &MountOptions{Version: l.Smb.MountOptions.Version}
		}
	}

	return out, nil
}

func (b *InMemoryBackend) UpdateLocationSmb(
	locationArn, serverHostname, subdirectory, domain, user, password, authenticationType string,
	mountOptions *MountOptions,
	agentArns []string,
	smbKerberos SmbKerberosConfig,
	secretConfig SecretConfig,
) error {
	b.mu.Lock("UpdateLocationSmb")
	defer b.mu.Unlock()

	l, ok := b.locations.Get(locationArn)
	if !ok || l.LocationType != locationTypeSMB {
		return ErrNotFound
	}

	if err := b.validateAgentArns(agentArns); err != nil {
		return err
	}

	if l.Smb == nil {
		l.Smb = &storedSmbConfig{}
	}

	if serverHostname != "" {
		l.Smb.ServerHostname = serverHostname
	}

	if subdirectory != "" {
		l.Subdirectory = subdirectory
	}

	if serverHostname != "" || subdirectory != "" {
		sub := strings.TrimPrefix(l.Subdirectory, "/")
		l.LocationURI = fmt.Sprintf("smb://%s/%s", l.Smb.ServerHostname, sub)
	}

	if domain != "" {
		l.Smb.Domain = domain
	}

	if user != "" {
		l.Smb.User = user
	}

	if password != "" {
		l.Smb.Password = password
	}

	if authenticationType != "" {
		l.Smb.AuthenticationType = authenticationType
	}

	if mountOptions != nil {
		l.Smb.MountOptions = &storedMountOptions{Version: mountOptions.Version}
	}

	if agentArns != nil {
		l.Smb.AgentArns = agentArns
	}

	updateSmbKerberos(l.Smb, smbKerberos)
	updateSmbSecretConfig(l.Smb, secretConfig)

	return nil
}

func updateSmbKerberos(cfg *storedSmbConfig, smbKerberos SmbKerberosConfig) {
	if smbKerberos.KerberosPrincipal != "" {
		cfg.KerberosPrincipal = smbKerberos.KerberosPrincipal
	}

	if smbKerberos.KerberosKeytab != "" {
		cfg.KerberosKeytab = smbKerberos.KerberosKeytab
	}

	if smbKerberos.KerberosKrb5Conf != "" {
		cfg.KerberosKrb5Conf = smbKerberos.KerberosKrb5Conf
	}

	if smbKerberos.DNSIPAddresses != nil {
		cfg.DNSIPAddresses = smbKerberos.DNSIPAddresses
	}
}

func updateSmbSecretConfig(cfg *storedSmbConfig, secretConfig SecretConfig) {
	if secretConfig.Cmk != nil {
		cfg.CmkSecretConfig = toStoredCmkSecretConfig(secretConfig.Cmk)
	}

	if secretConfig.Custom != nil {
		cfg.CustomSecretConfig = toStoredCustomSecretConfig(secretConfig.Custom)
	}
}
