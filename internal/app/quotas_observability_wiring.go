package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

// wireQuotaAndObservabilityOrganizations gives Service Quotas and OAM their Organizations lookups.
func wireQuotaAndObservabilityOrganizations(byName map[string]service.Registerable) {
	orgH, ok := byName["Organizations"].(*organizationsbackend.Handler)
	if !ok || orgH.Backend == nil {
		return
	}

	orgBk, ok := orgH.Backend.(*organizationsbackend.InMemoryBackend)
	if !ok {
		return
	}

	adapter := quotasOrgAdapter{org: orgBk}

	if h, hok := byName["ServiceQuotas"].(*servicequotasbackend.Handler); hok {
		h.Backend.SetOrgDirectory(adapter)
	}

	if h, hok := byName["OAM"].(*oambackend.Handler); hok {
		h.Backend.SetAccountDirectory(adapter)
	}
}
