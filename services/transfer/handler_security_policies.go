package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// securityPolicyDef holds the static attributes of a named AWS Transfer security policy.
// Field-diffed against aws-sdk-go-v2/service/transfer/types.DescribedSecurityPolicy
// (SecurityPolicyName, Fips, Protocols, SshCiphers, SshHostKeyAlgorithms, SshKexs,
// SshMacs, TlsCiphers, Type) and against the current AWS documentation catalog
// (docs.aws.amazon.com/transfer/latest/userguide/security-policies{,-connectors}.html).
// ContentEncryptionCiphers/HashAlgorithms (AS2) are documented on the real API but are
// not yet modeled as typed fields on the pinned SDK's DescribedSecurityPolicy struct;
// they are still emitted on the wire below since real AWS sends them and unknown JSON
// fields are harmless to typed clients.
type securityPolicyDef struct {
	Type                     string // "SERVER" or "CONNECTOR"
	Protocols                []string
	SSHCiphers               []string
	SSHKexs                  []string
	SSHMacs                  []string
	TLSCiphers               []string // SERVER only
	SSHHostKeyAlgorithms     []string // CONNECTOR only
	ContentEncryptionCiphers []string // SERVER only (AS2)
	HashAlgorithms           []string // SERVER only (AS2)
	Fips                     bool
}

const (
	secPolicyTypeServer    = "SERVER"
	secPolicyTypeConnector = "CONNECTOR"
)

// Security policy SSH/TLS algorithm name constants (avoids repeated string literals).
const (
	sshCipherAES128GCM  = "aes128-gcm@openssh.com"
	sshCipherAES256GCM  = "aes256-gcm@openssh.com"
	sshCipherAES128CTR  = "aes128-ctr"
	sshCipherAES192CTR  = "aes192-ctr"
	sshCipherAES256CTR  = "aes256-ctr"
	sshCipherChaCha20   = "chacha20-poly1305@openssh.com"
	sshKexCurve25519    = "curve25519-sha256"
	sshKexCurve25519Lib = "curve25519-sha256@libssh.org"
	sshKexNistp384      = "ecdh-sha2-nistp384"
	sshKexNistp256      = "ecdh-sha2-nistp256"
	sshKexNistp521      = "ecdh-sha2-nistp521"
	sshKexDH16          = "diffie-hellman-group16-sha512"
	sshKexDH18          = "diffie-hellman-group18-sha512"
	sshKexDHExchange256 = "diffie-hellman-group-exchange-sha256"
	sshKexDH14sha256    = "diffie-hellman-group14-sha256"
	sshKexDH14sha1      = "diffie-hellman-group14-sha1"
	sshMacETMSHA256     = "hmac-sha2-256-etm@openssh.com"
	sshMacETMSHA512     = "hmac-sha2-512-etm@openssh.com"
	sshMacSHA256        = "hmac-sha2-256"
	sshMacSHA512        = "hmac-sha2-512"
	tlsECDHEECDSA128GCM = "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	tlsECDHERSA128GCM   = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	tlsECDHEECDSA128CBC = "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256"
	tlsECDHERSA128CBC   = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256"
	tlsECDHEECDSA256GCM = "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
	tlsECDHERSA256GCM   = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
	tlsECDHEECDSA256CBC = "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384"
	tlsECDHERSA256CBC   = "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384"
	tlsRSA128CBC        = "TLS_RSA_WITH_AES_128_CBC_SHA256"
	tlsRSA256CBC        = "TLS_RSA_WITH_AES_256_CBC_SHA256"
	hostKeyRSASHA256    = "rsa-sha2-256"
	hostKeyRSASHA512    = "rsa-sha2-512"
)

// securityPolicyCatalog returns the ordered catalog of AWS Transfer security
// policies. It is a function (not a var) to avoid package-level mutable state.
// Sourced from the "Security policy details" JSON examples at
// docs.aws.amazon.com/transfer/latest/userguide/security-policies.html (servers) and
// .../security-policies-connectors.html (SFTP connectors), current as of 2026-07.
func securityPolicyCatalog() []securityPolicyEntry {
	catalog := serverSecurityPolicies2025()
	catalog = append(catalog, serverSecurityPolicies2024()...)
	catalog = append(catalog, serverSecurityPoliciesLegacy()...)
	catalog = append(catalog, connectorSecurityPolicies()...)

	return catalog
}

type securityPolicyEntry = struct {
	name string
	def  securityPolicyDef
}

// securityPolicyCommonCiphers holds the TLS/AS2 algorithm lists shared across
// (almost) every SERVER-type policy, factored out to avoid re-typing the same
// literal AWS-doc data in both serverSecurityPoliciesModern and -Legacy.
type securityPolicyCommonCiphers struct {
	tls8                     []string
	tls2018                  []string
	contentCiphersStd        []string
	contentCiphersRestricted []string
	hashAlgosStd             []string
	hashAlgosRestricted      []string
}

func newSecurityPolicyCommonCiphers() securityPolicyCommonCiphers {
	tls8 := []string{
		tlsECDHEECDSA128GCM, tlsECDHERSA128GCM, tlsECDHEECDSA128CBC, tlsECDHERSA128CBC,
		tlsECDHEECDSA256GCM, tlsECDHERSA256GCM, tlsECDHEECDSA256CBC, tlsECDHERSA256CBC,
	}

	return securityPolicyCommonCiphers{
		tls8:                     tls8,
		tls2018:                  append(append([]string(nil), tls8...), tlsRSA128CBC, tlsRSA256CBC),
		contentCiphersStd:        []string{"aes256-cbc", "aes192-cbc", "aes128-cbc", "3des-cbc"},
		contentCiphersRestricted: []string{"aes256-cbc", "aes192-cbc", "aes128-cbc"},
		hashAlgosStd:             []string{"sha256", "sha384", "sha512", "sha1"},
		hashAlgosRestricted:      []string{"sha256", "sha384", "sha512"},
	}
}

// modernSSHCipherSets holds the (few) distinct 5-cipher AES combinations reused
// across the 2024/2025-vintage SERVER policies, factored out so each policy entry
// below is a single line instead of a 5-line slice literal.
type modernSSHCipherSets struct {
	gcmFirst    []string // 2025-03, AS2Restricted-2025-07
	ctrFirst    []string // FIPS-2025-03
	gcm128First []string // SshAuditCompliant-2025-02, 2024-01, FIPS-2024-01
	kex2025     []string
	kexFIPS2025 []string
	kex2024     []string
	kexFIPS2024 []string
}

func newModernSSHCipherSets() modernSSHCipherSets {
	mlkemKexs := []string{"mlkem768x25519-sha256", "mlkem768nistp256-sha256", "mlkem1024nistp384-sha384"}

	return modernSSHCipherSets{
		gcmFirst: []string{
			sshCipherAES256GCM,
			sshCipherAES128GCM,
			sshCipherAES128CTR,
			sshCipherAES256CTR,
			sshCipherAES192CTR,
		},
		ctrFirst: []string{
			sshCipherAES256GCM,
			sshCipherAES128GCM,
			sshCipherAES256CTR,
			sshCipherAES192CTR,
			sshCipherAES128CTR,
		},
		gcm128First: []string{
			sshCipherAES128GCM,
			sshCipherAES256GCM,
			sshCipherAES128CTR,
			sshCipherAES256CTR,
			sshCipherAES192CTR,
		},
		kex2025: append(
			append([]string(nil), mlkemKexs...),
			sshKexNistp256,
			sshKexNistp384,
			sshKexNistp521,
			sshKexCurve25519,
			sshKexCurve25519Lib,
			sshKexDH16,
			sshKexDH18,
			sshKexDHExchange256,
		),
		kexFIPS2025: append(append([]string(nil), mlkemKexs...),
			sshKexNistp256, sshKexNistp384, sshKexNistp521, sshKexDHExchange256, sshKexDH16, sshKexDH18),
		kex2024: []string{
			sshKexNistp256, sshKexNistp384, sshKexNistp521, sshKexCurve25519,
			sshKexCurve25519Lib, sshKexDH18, sshKexDH16, sshKexDHExchange256,
		},
		kexFIPS2024: []string{
			sshKexNistp256,
			sshKexNistp384,
			sshKexNistp521,
			sshKexDH18,
			sshKexDH16,
			sshKexDHExchange256,
		},
	}
}

// serverSecurityPolicies2025 returns the 2025-vintage SERVER policies, including the
// post-quantum mlkem-hybrid KEX policies introduced that year.
func serverSecurityPolicies2025() []securityPolicyEntry {
	sftpFTPS := []string{protocolSFTP, protocolFTPS}
	c := newSecurityPolicyCommonCiphers()
	s := newModernSSHCipherSets()

	return []securityPolicyEntry{
		{"TransferSecurityPolicy-2025-03", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: s.gcmFirst, SSHKexs: s.kex2025, SSHMacs: []string{sshMacETMSHA256, sshMacETMSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-FIPS-2025-03", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: s.ctrFirst, SSHKexs: s.kexFIPS2025, SSHMacs: []string{sshMacETMSHA512, sshMacETMSHA256},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{
			"TransferSecurityPolicy-AS2Restricted-2025-07",
			securityPolicyDef{
				Type:                     secPolicyTypeServer,
				Protocols:                sftpFTPS,
				SSHCiphers:               s.gcmFirst,
				SSHKexs:                  s.kex2025,
				SSHMacs:                  []string{sshMacETMSHA256, sshMacETMSHA512},
				TLSCiphers:               c.tls8,
				ContentEncryptionCiphers: c.contentCiphersRestricted,
				HashAlgorithms:           c.hashAlgosRestricted,
			},
		},
		{"TransferSecurityPolicy-SshAuditCompliant-2025-02", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: s.gcm128First,
			SSHKexs:    []string{sshKexCurve25519, sshKexCurve25519Lib, sshKexDH18, sshKexDH16, sshKexDHExchange256},
			SSHMacs:    []string{sshMacETMSHA256, sshMacETMSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
	}
}

// serverSecurityPolicies2024 returns the 2024-vintage SERVER policies.
func serverSecurityPolicies2024() []securityPolicyEntry {
	sftpFTPS := []string{protocolSFTP, protocolFTPS}
	c := newSecurityPolicyCommonCiphers()
	s := newModernSSHCipherSets()

	return []securityPolicyEntry{
		{"TransferSecurityPolicy-2024-01", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: s.gcm128First, SSHKexs: s.kex2024, SSHMacs: []string{sshMacETMSHA256, sshMacETMSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-FIPS-2024-01", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: s.gcm128First, SSHKexs: s.kexFIPS2024, SSHMacs: []string{sshMacETMSHA256, sshMacETMSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
	}
}

// serverSecurityPoliciesLegacy returns the 2018-2023-vintage SERVER policies.
func serverSecurityPoliciesLegacy() []securityPolicyEntry {
	catalog := serverSecurityPolicies2022to2023()

	return append(catalog, serverSecurityPolicies2018to2020()...)
}

// serverSecurityPolicies2022to2023 returns the 2022/2023-vintage SERVER policies.
func serverSecurityPolicies2022to2023() []securityPolicyEntry {
	sftpFTPS := []string{protocolSFTP, protocolFTPS}
	c := newSecurityPolicyCommonCiphers()
	aes4 := []string{sshCipherAES256GCM, sshCipherAES128GCM, sshCipherAES256CTR, sshCipherAES192CTR}

	return []securityPolicyEntry{
		{"TransferSecurityPolicy-2023-05", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: aes4,
			SSHKexs:    []string{sshKexCurve25519, sshKexCurve25519Lib, sshKexDH16, sshKexDH18, sshKexDHExchange256},
			SSHMacs:    []string{sshMacETMSHA512, sshMacETMSHA256},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-FIPS-2023-05", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: aes4,
			SSHKexs:    []string{sshKexDH16, sshKexDH18, sshKexDHExchange256},
			SSHMacs:    []string{sshMacETMSHA256, sshMacETMSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-2022-03", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: aes4,
			SSHKexs:    []string{sshKexCurve25519, sshKexCurve25519Lib, sshKexDH16, sshKexDH18, sshKexDHExchange256},
			SSHMacs:    []string{sshMacETMSHA512, sshMacETMSHA256, sshMacSHA512, sshMacSHA256},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
	}
}

// serverSecurityPolicies2018to2020 returns the 2018-2020-vintage SERVER policies.
func serverSecurityPolicies2018to2020() []securityPolicyEntry {
	sftpFTPS := []string{protocolSFTP, protocolFTPS}
	c := newSecurityPolicyCommonCiphers()

	return []securityPolicyEntry{
		{"TransferSecurityPolicy-2020-06", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: []string{
				sshCipherChaCha20,
				sshCipherAES128CTR,
				sshCipherAES192CTR,
				sshCipherAES256CTR,
				sshCipherAES128GCM,
				sshCipherAES256GCM,
			},
			SSHKexs: []string{
				sshKexNistp256,
				sshKexNistp384,
				sshKexNistp521,
				sshKexDHExchange256,
				sshKexDH16,
				sshKexDH18,
				sshKexDH14sha256,
			},
			SSHMacs: []string{
				"umac-128-etm@openssh.com",
				sshMacETMSHA256,
				sshMacETMSHA512,
				"umac-128@openssh.com",
				sshMacSHA256,
				sshMacSHA512,
			},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-FIPS-2020-06", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: []string{
				sshCipherAES128CTR,
				sshCipherAES192CTR,
				sshCipherAES256CTR,
				sshCipherAES128GCM,
				sshCipherAES256GCM,
			},
			SSHKexs: []string{
				sshKexNistp256,
				sshKexNistp384,
				sshKexNistp521,
				sshKexDHExchange256,
				sshKexDH16,
				sshKexDH18,
				sshKexDH14sha256,
			},
			SSHMacs:    []string{sshMacETMSHA256, sshMacETMSHA512, sshMacSHA256, sshMacSHA512},
			TLSCiphers: c.tls8, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
		{"TransferSecurityPolicy-2018-11", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: []string{
				sshCipherChaCha20,
				sshCipherAES128CTR,
				sshCipherAES192CTR,
				sshCipherAES256CTR,
				sshCipherAES128GCM,
				sshCipherAES256GCM,
			},
			SSHKexs: []string{
				sshKexCurve25519, sshKexCurve25519Lib, sshKexNistp256, sshKexNistp384, sshKexNistp521,
				sshKexDHExchange256, sshKexDH16, sshKexDH18, sshKexDH14sha256, sshKexDH14sha1,
			},
			SSHMacs: []string{
				"umac-64-etm@openssh.com",
				"umac-128-etm@openssh.com",
				sshMacETMSHA256,
				sshMacETMSHA512,
				"hmac-sha1-etm@openssh.com",
				"umac-64@openssh.com",
				"umac-128@openssh.com",
				sshMacSHA256,
				sshMacSHA512,
				"hmac-sha1",
			},
			TLSCiphers: c.tls2018, ContentEncryptionCiphers: c.contentCiphersStd, HashAlgorithms: c.hashAlgosStd,
		}},
	}
}

// connectorSecurityPolicies returns the CONNECTOR-type (SFTP connector) policies.
func connectorSecurityPolicies() []securityPolicyEntry {
	sftp := []string{protocolSFTP}

	return []securityPolicyEntry{
		{"TransferSFTPConnectorSecurityPolicy-FIPS-2024-10", securityPolicyDef{
			Type: secPolicyTypeConnector, Protocols: sftp, Fips: true,
			SSHCiphers:           []string{sshCipherAES128GCM, sshCipherAES256GCM},
			SSHKexs:              []string{sshKexNistp256, sshKexNistp384, sshKexNistp521},
			SSHMacs:              []string{sshMacSHA512, sshMacSHA256},
			SSHHostKeyAlgorithms: []string{hostKeyRSASHA256, hostKeyRSASHA512, sshKeyTypeECDSAP256},
		}},
		{"TransferSFTPConnectorSecurityPolicy-2024-03", securityPolicyDef{
			Type: secPolicyTypeConnector, Protocols: sftp,
			SSHCiphers: []string{
				sshCipherAES128GCM,
				sshCipherAES192CTR,
				sshCipherAES256CTR,
				sshCipherAES256GCM,
			},
			SSHKexs: []string{
				sshKexCurve25519,
				sshKexCurve25519Lib,
				sshKexDH16,
				sshKexDH18,
				sshKexDHExchange256,
			},
			SSHMacs: []string{sshMacETMSHA512, sshMacETMSHA256, sshMacSHA512, sshMacSHA256},
			SSHHostKeyAlgorithms: []string{
				hostKeyRSASHA256,
				hostKeyRSASHA512,
				sshKeyTypeECDSAP256,
				sshKeyTypeECDSAP384,
				sshKeyTypeECDSAP521,
			},
		}},
		{"TransferSFTPConnectorSecurityPolicy-2023-07", securityPolicyDef{
			Type: secPolicyTypeConnector, Protocols: sftp,
			SSHCiphers: []string{
				sshCipherAES128CTR,
				sshCipherAES128GCM,
				sshCipherAES192CTR,
				sshCipherAES256CTR,
				sshCipherAES256GCM,
			},
			SSHKexs: []string{
				sshKexCurve25519,
				sshKexCurve25519Lib,
				sshKexDH14sha1,
				sshKexDH16,
				sshKexDH18,
				sshKexDHExchange256,
			},
			SSHMacs: []string{
				sshMacETMSHA512,
				sshMacETMSHA256,
				sshMacSHA512,
				sshMacSHA256,
				"hmac-sha1",
				"hmac-sha1-96",
			},
			SSHHostKeyAlgorithms: []string{
				hostKeyRSASHA256,
				hostKeyRSASHA512,
				sshKeyTypeECDSAP256,
				sshKeyTypeECDSAP384,
				sshKeyTypeECDSAP521,
				defaultHostKeyType,
			},
		}},
	}
}

// lookupSecurityPolicy returns the definition for the named policy, or nil if unknown.
func lookupSecurityPolicy(name string) *securityPolicyDef {
	for _, e := range securityPolicyCatalog() {
		if e.name == name {
			d := e.def

			return &d
		}
	}

	return nil
}

// ErrSecurityPolicyNotFound is returned when a named security policy is not found.
var ErrSecurityPolicyNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)

type describeSecurityPolicyInput struct {
	SecurityPolicyName string `json:"SecurityPolicyName"`
}

func (h *Handler) handleDescribeSecurityPolicy(
	_ context.Context,
	in *describeSecurityPolicyInput,
) (*map[string]any, error) {
	if in.SecurityPolicyName == "" {
		return nil, fmt.Errorf("%w: SecurityPolicyName is required", errInvalidRequest)
	}

	pol := lookupSecurityPolicy(in.SecurityPolicyName)
	if pol == nil {
		return nil, fmt.Errorf("%w: security policy %q not found", ErrSecurityPolicyNotFound, in.SecurityPolicyName)
	}

	body := map[string]any{
		keySecurityPolicyName: in.SecurityPolicyName,
		"Fips":                pol.Fips,
		keyStepType:           pol.Type,
		"Protocols":           pol.Protocols,
		"SshCiphers":          pol.SSHCiphers,
		"SshKexs":             pol.SSHKexs,
		"SshMacs":             pol.SSHMacs,
	}

	if len(pol.TLSCiphers) > 0 {
		body["TlsCiphers"] = pol.TLSCiphers
	}

	if len(pol.SSHHostKeyAlgorithms) > 0 {
		body["SshHostKeyAlgorithms"] = pol.SSHHostKeyAlgorithms
	}

	if len(pol.ContentEncryptionCiphers) > 0 {
		body["ContentEncryptionCiphers"] = pol.ContentEncryptionCiphers
	}

	if len(pol.HashAlgorithms) > 0 {
		body["HashAlgorithms"] = pol.HashAlgorithms
	}

	return &map[string]any{"SecurityPolicy": body}, nil
}

type listSecurityPoliciesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listSecurityPoliciesOutput struct {
	NextToken           string   `json:"NextToken,omitempty"`
	SecurityPolicyNames []string `json:"SecurityPolicyNames"`
}

func (h *Handler) handleListSecurityPolicies(
	_ context.Context,
	in *listSecurityPoliciesInput,
) (*listSecurityPoliciesOutput, error) {
	catalog := securityPolicyCatalog()
	names := make([]string, len(catalog))

	for i, p := range catalog {
		names[i] = p.name
	}

	names, nextToken := applyNextTokenItems(names, in.NextToken, in.MaxResults)

	return &listSecurityPoliciesOutput{SecurityPolicyNames: names, NextToken: nextToken}, nil
}
