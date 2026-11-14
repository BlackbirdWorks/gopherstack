package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
)

const (
	quotaCaseServiceCode  = "service-limit-increase"
	quotaCaseCategoryCode = "general-guidance"
	quotaCaseSeverityCode = "low"
	supportCaseResolved   = "resolved"
)

// quotasSupportAdapter files quota increase requests as Support cases.
type quotasSupportAdapter struct {
	support supportbackend.StorageBackend
}

// OpenCase implements servicequotas.SupportDesk and returns the case display ID.
func (a quotasSupportAdapter) OpenCase(subject, body string) (string, error) {
	cs, err := a.support.CreateCaseWithOptions(supportbackend.CreateCaseOptions{
		Subject:           subject,
		CommunicationBody: body,
		ServiceCode:       quotaCaseServiceCode,
		CategoryCode:      quotaCaseCategoryCode,
		SeverityCode:      quotaCaseSeverityCode,
	})
	if err != nil {
		return "", err
	}

	return cs.DisplayID, nil
}

// CaseResolved implements servicequotas.SupportDesk.
func (a quotasSupportAdapter) CaseResolved(displayID string) bool {
	cases, _, err := a.support.DescribeCasesWithOptions(supportbackend.DescribeCasesOptions{
		DisplayID: displayID, IncludeResolvedCases: true,
	})
	if err != nil || len(cases) == 0 {
		return false
	}

	return cases[0].Status == supportCaseResolved
}

// wireQuotaSupport files quota increase requests as Support cases.
func wireQuotaSupport(byName map[string]service.Registerable) {
	sq, ok := byName["ServiceQuotas"].(*servicequotasbackend.Handler)
	if !ok {
		return
	}

	sup, ok := byName["Support"].(*supportbackend.Handler)
	if !ok || sup.Backend == nil {
		return
	}

	sq.Backend.SetSupportDesk(quotasSupportAdapter{support: sup.Backend})
}
