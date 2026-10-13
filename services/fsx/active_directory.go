package fsx

import (
	"fmt"
	"slices"
)

const maxADDNSIPs = 3

// selfManagedADInput covers SelfManagedActiveDirectoryConfiguration and its
// Updates variant: on create DomainName and DnsIps are required, on update
// every member is optional.
type selfManagedADInput struct {
	DomainName                          *string  `json:"DomainName,omitempty"`
	DomainJoinServiceAccountSecret      *string  `json:"DomainJoinServiceAccountSecret,omitempty"`
	FileSystemAdministratorsGroup       *string  `json:"FileSystemAdministratorsGroup,omitempty"`
	OrganizationalUnitDistinguishedName *string  `json:"OrganizationalUnitDistinguishedName,omitempty"`
	UserName                            *string  `json:"UserName,omitempty"`
	Password                            *string  `json:"Password,omitempty"`
	DNSIPs                              []string `json:"DnsIps,omitempty"`
}

// SelfManagedADAttrs mirrors the Password-free output shape.
type SelfManagedADAttrs struct {
	DomainName                          string   `json:"DomainName,omitempty"`
	DomainJoinServiceAccountSecret      string   `json:"DomainJoinServiceAccountSecret,omitempty"`
	FileSystemAdministratorsGroup       string   `json:"FileSystemAdministratorsGroup,omitempty"`
	OrganizationalUnitDistinguishedName string   `json:"OrganizationalUnitDistinguishedName,omitempty"`
	UserName                            string   `json:"UserName,omitempty"`
	DNSIPs                              []string `json:"DnsIps,omitempty"`
}

// SvmActiveDirectoryConfiguration mirrors types.SvmActiveDirectoryConfiguration.
type SvmActiveDirectoryConfiguration struct {
	SelfManagedAD *SelfManagedADAttrs `json:"SelfManagedActiveDirectoryConfiguration,omitempty"`
	NetBiosName   string              `json:"NetBiosName,omitempty"`
}

// svmADInput covers CreateSvmActiveDirectoryConfiguration and its Update variant.
type svmADInput struct {
	SelfManagedAD *selfManagedADInput `json:"SelfManagedActiveDirectoryConfiguration,omitempty"`
	NetBiosName   *string             `json:"NetBiosName,omitempty"`
}

func validateSelfManagedAD(c *selfManagedADInput, create bool) error {
	if c == nil {
		return nil
	}

	if create && (c.DomainName == nil || c.DNSIPs == nil) {
		return fmt.Errorf("%w: SelfManagedActiveDirectoryConfiguration needs DomainName and DnsIps", ErrValidation)
	}

	if len(c.DNSIPs) > maxADDNSIPs {
		return fmt.Errorf("%w: DnsIps takes at most %d addresses", ErrValidation, maxADDNSIPs)
	}

	return nil
}

func validateSvmAD(c *svmADInput, create bool) error {
	if c == nil {
		return nil
	}

	if create && c.NetBiosName == nil {
		return fmt.Errorf("%w: ActiveDirectoryConfiguration.NetBiosName is required", ErrValidation)
	}

	return validateSelfManagedAD(c.SelfManagedAD, create)
}

// mergeSelfManagedAD applies the non-nil members of upd onto cur. The
// password is write-only and never retained.
func mergeSelfManagedAD(
	cur *SelfManagedADAttrs,
	upd *selfManagedADInput,
) *SelfManagedADAttrs {
	if upd == nil {
		return cur
	}

	out := &SelfManagedADAttrs{}
	if cur != nil {
		*out = *cur
		out.DNSIPs = slices.Clone(cur.DNSIPs)
	}

	if upd.DomainName != nil {
		out.DomainName = *upd.DomainName
	}

	if upd.DomainJoinServiceAccountSecret != nil {
		out.DomainJoinServiceAccountSecret = *upd.DomainJoinServiceAccountSecret
	}

	if upd.FileSystemAdministratorsGroup != nil {
		out.FileSystemAdministratorsGroup = *upd.FileSystemAdministratorsGroup
	}

	if upd.OrganizationalUnitDistinguishedName != nil {
		out.OrganizationalUnitDistinguishedName = *upd.OrganizationalUnitDistinguishedName
	}

	if upd.UserName != nil {
		out.UserName = *upd.UserName
	}

	if upd.DNSIPs != nil {
		out.DNSIPs = slices.Clone(upd.DNSIPs)
	}

	return out
}

func mergeSvmAD(cur *SvmActiveDirectoryConfiguration, upd *svmADInput) *SvmActiveDirectoryConfiguration {
	if upd == nil {
		return cur
	}

	out := &SvmActiveDirectoryConfiguration{}
	if cur != nil {
		*out = *cur
	}

	if upd.NetBiosName != nil {
		out.NetBiosName = *upd.NetBiosName
	}

	out.SelfManagedAD = mergeSelfManagedAD(
		out.SelfManagedAD, upd.SelfManagedAD,
	)

	return out
}

func cloneSelfManagedAD(a *SelfManagedADAttrs) *SelfManagedADAttrs {
	if a == nil {
		return nil
	}

	cp := *a
	cp.DNSIPs = slices.Clone(a.DNSIPs)

	return &cp
}

func cloneSvmAD(c *SvmActiveDirectoryConfiguration) *SvmActiveDirectoryConfiguration {
	if c == nil {
		return nil
	}

	cp := *c
	cp.SelfManagedAD = cloneSelfManagedAD(c.SelfManagedAD)

	return &cp
}
