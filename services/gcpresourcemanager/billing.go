package gcpresourcemanager

import "regexp"

var billingAccountRE = regexp.MustCompile(`^billingAccounts/[0-9A-Za-z]{6}-[0-9A-Za-z]{6}-[0-9A-Za-z]{6}$`)

func (p *projectRec) billing() billingInfo {
	return billingInfo{
		Name: kindProjects + "/" + p.ID + "/billingInfo", ProjectID: p.ID,
		BillingAccountName: p.Billing, BillingEnabled: p.Billing != "",
	}
}

// BillingInfo returns the Cloud Billing association of a project (billingAccountName is empty when none).
func (b *InMemoryBackend) BillingInfo(ref string) (billingInfo, error) {
	b.mu.Lock("BillingInfo")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.projectLocked(ref, "billing.resourceAssociations.list")
	if err != nil {
		return billingInfo{}, err
	}

	return p.billing(), nil
}

// SetBillingAccount links a project to a billing account, or unlinks it when account is empty. Billing accounts
// are not modelled, so any well-formed name is accepted.
func (b *InMemoryBackend) SetBillingAccount(ref, account string) (billingInfo, error) {
	if account != "" && !billingAccountRE.MatchString(account) {
		return billingInfo{}, errInvalid("Invalid billing account %q: use billingAccounts/XXXXXX-XXXXXX-XXXXXX.", account)
	}

	b.mu.Lock("SetBillingAccount")
	defer b.mu.Unlock()

	b.purgeLocked()

	p, err := b.activeProjectLocked(ref, "billing.resourceAssociations.create")
	if err != nil {
		return billingInfo{}, err
	}

	p.Billing = account

	return p.billing(), nil
}
