package datasync

import (
	"context"
	"fmt"
)

// --- SMB location ---

// validateSmbAuthenticationType rejects any AuthenticationType other than
// the two AWS documents (empty means "not supplied", defaulting to NTLM):
// SmbAuthenticationType enum is exactly ["NTLM", "KERBEROS"] (botocore
// datasync 2018-11-09 model).
func validateSmbAuthenticationType(v string) error {
	if v == "" || v == smbAuthTypeNTLM || v == smbAuthTypeKerberos {
		return nil
	}

	return fmt.Errorf("%w: AuthenticationType must be NTLM or KERBEROS, got %q", errInvalidRequest, v)
}

type createLocationSmbInput struct {
	MountOptions       *mountOptionsInput      `json:"MountOptions"`
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig"`
	ServerHostname     string                  `json:"ServerHostname"`
	Subdirectory       string                  `json:"Subdirectory,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
	User               string                  `json:"User"`
	Password           string                  `json:"Password"`
	AuthenticationType string                  `json:"AuthenticationType,omitempty"`
	KerberosPrincipal  string                  `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string                  `json:"KerberosKeytab,omitempty"`
	KerberosKrb5Conf   string                  `json:"KerberosKrb5Conf,omitempty"`
	AgentArns          []string                `json:"AgentArns"`
	Tags               []tagInput              `json:"Tags"`
	DNSIPAddresses     []string                `json:"DnsIpAddresses"`
}

type createLocationSmbOutput struct {
	LocationArn string `json:"LocationArn"`
}

func (h *Handler) handleCreateLocationSmb(
	_ context.Context,
	in *createLocationSmbInput,
) (*createLocationSmbOutput, error) {
	if in.ServerHostname == "" {
		return nil, fmt.Errorf("%w: ServerHostname is required", errInvalidRequest)
	}

	if in.Subdirectory == "" {
		return nil, fmt.Errorf("%w: Subdirectory is required", errInvalidRequest)
	}

	if len(in.AgentArns) == 0 {
		return nil, fmt.Errorf("%w: AgentArns is required", errInvalidRequest)
	}

	if err := validateSmbAuthenticationType(in.AuthenticationType); err != nil {
		return nil, err
	}

	if err := validateSecretConfig(in.CmkSecretConfig, in.CustomSecretConfig); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	smbKerberos := SmbKerberosConfig{
		KerberosPrincipal: in.KerberosPrincipal,
		KerberosKeytab:    in.KerberosKeytab,
		KerberosKrb5Conf:  in.KerberosKrb5Conf,
		DNSIPAddresses:    in.DNSIPAddresses,
	}
	secretConfig := SecretConfig{
		Cmk:    cmkSecretConfigFromWire(in.CmkSecretConfig),
		Custom: customSecretConfigFromWire(in.CustomSecretConfig),
	}

	l, err := h.Backend.CreateLocationSmb(
		in.ServerHostname, in.Subdirectory, in.Domain, in.User, in.Password, in.AuthenticationType,
		mo, in.AgentArns, tags, smbKerberos, secretConfig,
	)
	if err != nil {
		return nil, err
	}

	return &createLocationSmbOutput{LocationArn: l.LocationArn}, nil
}

type describeLocationSmbInput struct {
	LocationArn string `json:"LocationArn"`
}

// describeLocationSmbOutput intentionally has no ServerHostname or
// Subdirectory field: the real DescribeLocationSmbOutput has neither
// (confirmed against aws-sdk-go-v2 v1.61.4: host/path are folded into
// LocationUri only). It does have AuthenticationType, unlike gopherstack's
// prior shape. KerberosKeytab/KerberosKrb5Conf stay write-only (absent from
// the real response too); CmkSecretConfig/CustomSecretConfig are echoed
// below, and ManagedSecretConfig stays absent (documented ReadOnly/
// AWS-populated field gopherstack can't honestly synthesize -- see
// PARITY.md).
type describeLocationSmbOutput struct {
	MountOptions       *mountOptionsOutput     `json:"MountOptions,omitempty"`
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig,omitempty"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig,omitempty"`
	LocationArn        string                  `json:"LocationArn"`
	LocationURI        string                  `json:"LocationUri"`
	Domain             string                  `json:"Domain,omitempty"`
	User               string                  `json:"User,omitempty"`
	AuthenticationType string                  `json:"AuthenticationType,omitempty"`
	KerberosPrincipal  string                  `json:"KerberosPrincipal,omitempty"`
	AgentArns          []string                `json:"AgentArns,omitempty"`
	DNSIPAddresses     []string                `json:"DnsIpAddresses,omitempty"`
	CreationTime       int64                   `json:"CreationTime"`
}

func (h *Handler) handleDescribeLocationSmb(
	_ context.Context,
	in *describeLocationSmbInput,
) (*describeLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	l, err := h.Backend.DescribeLocationSmb(in.LocationArn)
	if err != nil {
		return nil, err
	}

	out := &describeLocationSmbOutput{
		LocationArn:        l.LocationArn,
		LocationURI:        l.LocationURI,
		Domain:             l.Domain,
		User:               l.User,
		AuthenticationType: l.AuthenticationType,
		AgentArns:          l.AgentArns,
		CreationTime:       l.CreationTime.Unix(),
		KerberosPrincipal:  l.KerberosPrincipal,
		DNSIPAddresses:     l.DNSIPAddresses,
		CmkSecretConfig:    cmkSecretConfigToWire(l.CmkSecretConfig),
		CustomSecretConfig: customSecretConfigToWire(l.CustomSecretConfig),
	}

	if l.MountOptions != nil {
		out.MountOptions = &mountOptionsOutput{Version: l.MountOptions.Version}
	}

	return out, nil
}

type updateLocationSmbInput struct {
	MountOptions       *mountOptionsInput      `json:"MountOptions"`
	CmkSecretConfig    *cmkSecretConfigWire    `json:"CmkSecretConfig"`
	CustomSecretConfig *customSecretConfigWire `json:"CustomSecretConfig"`
	LocationArn        string                  `json:"LocationArn"`
	ServerHostname     string                  `json:"ServerHostname,omitempty"`
	Subdirectory       string                  `json:"Subdirectory,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
	User               string                  `json:"User,omitempty"`
	Password           string                  `json:"Password,omitempty"`
	AuthenticationType string                  `json:"AuthenticationType,omitempty"`
	KerberosPrincipal  string                  `json:"KerberosPrincipal,omitempty"`
	KerberosKeytab     string                  `json:"KerberosKeytab,omitempty"`
	KerberosKrb5Conf   string                  `json:"KerberosKrb5Conf,omitempty"`
	AgentArns          []string                `json:"AgentArns"`
	DNSIPAddresses     []string                `json:"DnsIpAddresses"`
}

type updateLocationSmbOutput struct{}

func (h *Handler) handleUpdateLocationSmb(
	_ context.Context,
	in *updateLocationSmbInput,
) (*updateLocationSmbOutput, error) {
	if in.LocationArn == "" {
		return nil, fmt.Errorf("%w: LocationArn is required", errInvalidRequest)
	}

	if err := validateSmbAuthenticationType(in.AuthenticationType); err != nil {
		return nil, err
	}

	if err := validateSecretConfig(in.CmkSecretConfig, in.CustomSecretConfig); err != nil {
		return nil, err
	}

	var mo *MountOptions
	if in.MountOptions != nil {
		mo = &MountOptions{Version: in.MountOptions.Version}
	}

	smbKerberos := SmbKerberosConfig{
		KerberosPrincipal: in.KerberosPrincipal,
		KerberosKeytab:    in.KerberosKeytab,
		KerberosKrb5Conf:  in.KerberosKrb5Conf,
		DNSIPAddresses:    in.DNSIPAddresses,
	}
	secretConfig := SecretConfig{
		Cmk:    cmkSecretConfigFromWire(in.CmkSecretConfig),
		Custom: customSecretConfigFromWire(in.CustomSecretConfig),
	}

	if err := h.Backend.UpdateLocationSmb(
		in.LocationArn, in.ServerHostname, in.Subdirectory, in.Domain, in.User, in.Password, in.AuthenticationType,
		mo, in.AgentArns, smbKerberos, secretConfig,
	); err != nil {
		return nil, err
	}

	return &updateLocationSmbOutput{}, nil
}
