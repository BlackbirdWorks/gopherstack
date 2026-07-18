package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// securityPolicyDef holds the static attributes of a named AWS Transfer security policy.
type securityPolicyDef struct {
	Type                 string   // "SERVER" or "CONNECTOR"
	Protocols            []string // e.g. ["SFTP"] or ["SFTP","FTPS"]
	SSHCiphers           []string
	SSHKexs              []string
	SSHMacs              []string
	TLSCiphers           []string // non-empty only for SERVER policies
	SSHHostKeyAlgorithms []string // non-empty only for CONNECTOR policies
	Fips                 bool
}

const (
	secPolicyTypeServer    = "SERVER"
	secPolicyTypeConnector = "CONNECTOR"
)

// Security policy SSH/TLS algorithm name constants (avoids repeated string literals).
const (
	sshKexNistp384             = "ecdh-sha2-nistp384"
	sshKexNistp256             = "ecdh-sha2-nistp256"
	sshKexDH16                 = "diffie-hellman-group16-sha512"
	sshMacETMSHA256            = "hmac-sha2-256-etm@openssh.com"
	sshMacETMSHA512            = "hmac-sha2-512-etm@openssh.com"
	tlsCipherAES256GCM         = "TLS_AES_256_GCM_SHA384"
	tlsCipherECDHERSAAES256GCM = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
)

// securityPolicyCatalog returns the ordered catalog of AWS Transfer security
// policies. It is a function (not a var) to avoid package-level mutable state.
func securityPolicyCatalog() []struct {
	name string
	def  securityPolicyDef
} {
	sftp := []string{protocolSFTP}
	sftpFTPS := []string{protocolSFTP, protocolFTPS}
	stdCiphers := []string{"aes128-gcm@openssh.com", "aes256-gcm@openssh.com", "aes256-ctr", "aes192-ctr", "aes128-ctr"}
	fipsCiphers := []string{"aes256-ctr", "aes192-ctr", "aes128-ctr"}
	stdKexs := []string{"curve25519-sha256", sshKexNistp384, sshKexNistp256, sshKexDH16}
	legacyKexs := []string{sshKexNistp384, sshKexNistp256, sshKexDH16, "diffie-hellman-group14-sha256"}
	fipsKexs := []string{sshKexNistp384, sshKexNistp256, sshKexDH16}
	pqKex := "ecdh-sha2-nistp256-kyber-512r3-sha256-d00@openquantumsafe.org"
	pqKexs := []string{pqKex, "curve25519-sha256", sshKexNistp384, sshKexNistp256, sshKexDH16}
	pqFIPSKexs := []string{pqKex, sshKexNistp384, sshKexNistp256, sshKexDH16}
	stdMacs := []string{sshMacETMSHA256, sshMacETMSHA512, "hmac-sha2-256"}
	legacyMacs := []string{sshMacETMSHA256, sshMacETMSHA512, "hmac-sha2-256", "hmac-sha2-512"}
	fipsMacs := []string{sshMacETMSHA256, sshMacETMSHA512}
	stdTLS := []string{
		tlsCipherAES256GCM, "TLS_AES_128_GCM_SHA256",
		tlsCipherECDHERSAAES256GCM, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
	}
	legacyTLS := []string{
		tlsCipherAES256GCM, "TLS_AES_128_GCM_SHA256",
		tlsCipherECDHERSAAES256GCM, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
	}
	fipsTLS := []string{tlsCipherAES256GCM, tlsCipherECDHERSAAES256GCM, "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"}
	fipsLegacyTLS := []string{tlsCipherAES256GCM, tlsCipherECDHERSAAES256GCM}
	connHKAlgs := []string{sshKeyTypeECDSAP384, sshKeyTypeECDSAP256, "rsa-sha2-512", "rsa-sha2-256"}

	type entry = struct {
		name string
		def  securityPolicyDef
	}

	return []entry{
		{"TransferSecurityPolicy-2024-01", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: stdCiphers, SSHKexs: stdKexs, SSHMacs: stdMacs, TLSCiphers: stdTLS,
		}},
		{"TransferSecurityPolicy-2023-05", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: stdCiphers, SSHKexs: stdKexs, SSHMacs: stdMacs, TLSCiphers: stdTLS,
		}},
		{"TransferSecurityPolicy-2022-03", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: stdCiphers, SSHKexs: legacyKexs, SSHMacs: stdMacs, TLSCiphers: legacyTLS,
		}},
		{"TransferSecurityPolicy-2020-06", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS,
			SSHCiphers: stdCiphers, SSHKexs: legacyKexs, SSHMacs: legacyMacs, TLSCiphers: legacyTLS,
		}},
		{"TransferSecurityPolicy-FIPS-2024-01", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: fipsCiphers, SSHKexs: fipsKexs, SSHMacs: stdMacs, TLSCiphers: fipsTLS,
		}},
		{"TransferSecurityPolicy-FIPS-2023-05", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftpFTPS, Fips: true,
			SSHCiphers: fipsCiphers, SSHKexs: fipsKexs, SSHMacs: stdMacs, TLSCiphers: fipsTLS,
		}},
		{"TransferSecurityPolicy-FIPS-2020-06", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftp, Fips: true,
			SSHCiphers: fipsCiphers, SSHKexs: fipsKexs, SSHMacs: fipsMacs, TLSCiphers: fipsLegacyTLS,
		}},
		{"TransferSecurityPolicy-PQ-SSH-2023-04", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftp,
			SSHCiphers: stdCiphers, SSHKexs: pqKexs, SSHMacs: stdMacs,
		}},
		{"TransferSecurityPolicy-PQ-SSH-FIPS-2023-04", securityPolicyDef{
			Type: secPolicyTypeServer, Protocols: sftp, Fips: true,
			SSHCiphers: fipsCiphers, SSHKexs: pqFIPSKexs, SSHMacs: fipsMacs,
		}},
		{"TransferSecurityPolicy-Connector-2023-05", securityPolicyDef{
			Type: secPolicyTypeConnector, Protocols: sftp,
			SSHCiphers: stdCiphers, SSHKexs: stdKexs, SSHMacs: stdMacs, SSHHostKeyAlgorithms: connHKAlgs,
		}},
		{"TransferSecurityPolicy-FIPS-Connector-2023-05", securityPolicyDef{
			Type: secPolicyTypeConnector, Protocols: sftp, Fips: true,
			SSHCiphers: fipsCiphers, SSHKexs: fipsKexs, SSHMacs: fipsMacs, SSHHostKeyAlgorithms: connHKAlgs,
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
