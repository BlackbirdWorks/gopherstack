package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	accountbackend "github.com/blackbirdworks/gopherstack/services/account"
	oambackend "github.com/blackbirdworks/gopherstack/services/oam"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
)

const serviceQuotasPrincipal = "servicequotas.amazonaws.com"

// quotasOrgAdapter exposes the Organizations backend to Service Quotas and OAM.
type quotasOrgAdapter struct {
	org *organizationsbackend.InMemoryBackend
}

// OrganizationOf implements servicequotas.OrgDirectory.
func (a quotasOrgAdapter) OrganizationOf(accountID string) (servicequotasbackend.OrgInfo, bool) {
	org, err := a.org.DescribeOrganization()
	if err != nil {
		return servicequotasbackend.OrgInfo{}, false
	}

	if _, err = a.org.DescribeAccount(accountID); err != nil {
		return servicequotasbackend.OrgInfo{}, false
	}

	info := servicequotasbackend.OrgInfo{
		ManagementAccountID: org.MasterAccountID,
		AllFeatures:         org.FeatureSet == "ALL",
	}

	if enabled, listErr := a.org.ListAWSServiceAccessForOrganization(); listErr == nil {
		for _, sp := range enabled {
			if sp.ServicePrincipal == serviceQuotasPrincipal {
				info.ServiceAccess = true
			}
		}
	}

	return info, true
}

// AccountInfo implements oam.AccountDirectory.
func (a quotasOrgAdapter) AccountInfo(accountID string) (oambackend.AccountInfo, bool) {
	acct, err := a.org.DescribeAccount(accountID)
	if err != nil {
		return oambackend.AccountInfo{}, false
	}

	info := oambackend.AccountInfo{Name: acct.Name, Email: acct.Email}

	if org, orgErr := a.org.DescribeOrganization(); orgErr == nil {
		info.OrgID = org.ID
	}

	return info, true
}

// oamAccountDirectory resolves link-label variables from Organizations, then from the standalone Account service.
type oamAccountDirectory struct {
	org     *quotasOrgAdapter
	account accountbackend.StorageBackend
}

// AccountInfo implements oam.AccountDirectory.
func (d oamAccountDirectory) AccountInfo(accountID string) (oambackend.AccountInfo, bool) {
	if d.org != nil {
		if info, ok := d.org.AccountInfo(accountID); ok {
			return info, true
		}
	}

	if d.account == nil {
		return oambackend.AccountInfo{}, false
	}

	acct, err := d.account.GetAccountInformation()
	if err != nil || acct.AccountID != accountID {
		return oambackend.AccountInfo{}, false
	}

	return oambackend.AccountInfo{Name: acct.AccountName, Email: d.account.GetPrimaryEmail()}, true
}

// wireQuotaAndObservabilityOrganizations gives Service Quotas and OAM their account lookups.
func wireQuotaAndObservabilityOrganizations(byName map[string]service.Registerable) {
	wireQuotaSupport(byName)

	var adapter *quotasOrgAdapter

	if orgH, ok := byName["Organizations"].(*organizationsbackend.Handler); ok && orgH.Backend != nil {
		if orgBk, bok := orgH.Backend.(*organizationsbackend.InMemoryBackend); bok {
			adapter = &quotasOrgAdapter{org: orgBk}

			if h, hok := byName["ServiceQuotas"].(*servicequotasbackend.Handler); hok {
				h.Backend.SetOrgDirectory(*adapter)
			}
		}
	}

	h, hok := byName["OAM"].(*oambackend.Handler)
	if !hok {
		return
	}

	dir := oamAccountDirectory{org: adapter}
	if acctH, aok := byName["Account"].(*accountbackend.Handler); aok {
		dir.account = acctH.Backend
	}

	h.Backend.SetAccountDirectory(dir)
}
