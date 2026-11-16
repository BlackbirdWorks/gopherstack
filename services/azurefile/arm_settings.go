package azurefile

// CORSSetting is one CORS rule as the management plane sees it (list values comma separated).
type CORSSetting struct {
	AllowedOrigins  string
	AllowedMethods  string
	AllowedHeaders  string
	ExposedHeaders  string
	MaxAgeInSeconds int32
}

// SMBSetting is the SMB protocol settings of the file service (list values semicolon separated).
type SMBSetting struct {
	Versions                 string
	AuthenticationMethods    string
	KerberosTicketEncryption string
	ChannelEncryption        string
	MultichannelEnabled      bool
}

// SetCORS replaces the account's CORS rules; invalid rules are refused.
func (b *InMemoryBackend) SetCORS(account string, rules []CORSSetting) error {
	wrap := &corsWrap{Rules: make([]corsRule, 0, len(rules))}
	for _, r := range rules {
		wrap.Rules = append(wrap.Rules, corsRule(r))
	}

	if aerr := validateCORS(wrap); aerr != nil {
		return &ShareError{Code: aerr.Code, Message: aerr.Message, Status: aerr.Status}
	}

	b.mu.Lock("SetCORS")
	defer b.mu.Unlock()

	b.acct(account).props().CORS = wrap

	return nil
}

// CORS returns the account's CORS rules.
func (b *InMemoryBackend) CORS(account string) []CORSSetting {
	b.mu.Lock("CORS")
	defer b.mu.Unlock()

	rules := b.acct(account).props().CORS.Rules
	out := make([]CORSSetting, 0, len(rules))

	for _, r := range rules {
		out = append(out, CORSSetting(r))
	}

	return out
}

// SetSMB replaces the account's SMB protocol settings, keeping the encryption-in-transit element.
func (b *InMemoryBackend) SetSMB(account string, s SMBSetting) {
	b.mu.Lock("SetSMB")
	defer b.mu.Unlock()

	p := b.acct(account).props()
	if p.Protocol == nil {
		p.Protocol = &protocolSettings{}
	}

	var enc *encryptionInTransit
	if p.Protocol.SMB != nil {
		enc = p.Protocol.SMB.Encryption
	}

	p.Protocol.SMB = &smbSettings{
		Multichannel: &struct {
			Enabled bool `xml:"Enabled"`
		}{Enabled: s.MultichannelEnabled},
		Encryption: enc, Versions: s.Versions, AuthenticationMethods: s.AuthenticationMethods,
		KerberosTicketEncryption: s.KerberosTicketEncryption, ChannelEncryption: s.ChannelEncryption,
	}
}

// SMB returns the account's SMB protocol settings.
func (b *InMemoryBackend) SMB(account string) SMBSetting {
	b.mu.Lock("SMB")
	defer b.mu.Unlock()

	smb := b.acct(account).props().Protocol
	if smb == nil || smb.SMB == nil {
		return SMBSetting{}
	}

	s := smb.SMB

	return SMBSetting{
		Versions: s.Versions, AuthenticationMethods: s.AuthenticationMethods,
		KerberosTicketEncryption: s.KerberosTicketEncryption, ChannelEncryption: s.ChannelEncryption,
		MultichannelEnabled: s.Multichannel != nil && s.Multichannel.Enabled,
	}
}
