package sesv2

// ---- account ----

// AccountDetails stores account-level SESv2 settings.
type AccountDetails struct {
	VdmAttributes         map[string]any `json:"vdmAttributes,omitempty"`
	MailType              string         `json:"mailType"`
	WebsiteURL            string         `json:"websiteURL"`
	ContactLanguage       string         `json:"contactLanguage"`
	UseCaseName           string         `json:"useCaseName"`
	SuppressionAttributes []string       `json:"suppressionAttributes,omitempty"`
	SendingEnabled        bool           `json:"sendingEnabled"`
	AutoWarmupEnabled     bool           `json:"autoWarmupEnabled,omitempty"`
}

// GetAccount returns the account details.
func (b *InMemoryBackend) GetAccount() (*AccountDetails, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	if b.accountDetails == nil {
		return &AccountDetails{SendingEnabled: true}, nil
	}

	cp := *b.accountDetails

	return &cp, nil
}

// PutAccountDetails stores account details.
func (b *InMemoryBackend) PutAccountDetails(details *AccountDetails) error {
	b.mu.Lock("PutAccountDetails")
	defer b.mu.Unlock()

	cp := *details
	b.accountDetails = &cp

	return nil
}

// PutAccountSendingAttributes sets the sending enabled flag.
func (b *InMemoryBackend) PutAccountSendingAttributes(sendingEnabled bool) error {
	b.mu.Lock("PutAccountSendingAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.SendingEnabled = sendingEnabled

	return nil
}

func (b *InMemoryBackend) PutAccountSuppressionAttributes(suppressedReasons []string) error {
	b.mu.Lock("PutAccountSuppressionAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	reasons := make([]string, len(suppressedReasons))
	copy(reasons, suppressedReasons)
	b.accountDetails.SuppressionAttributes = reasons

	return nil
}

func (b *InMemoryBackend) PutAccountDedicatedIPWarmupAttributes(autoWarmupEnabled bool) error {
	b.mu.Lock("PutAccountDedicatedIPWarmupAttributes")
	defer b.mu.Unlock()

	if b.accountDetails == nil {
		b.accountDetails = &AccountDetails{}
	}

	b.accountDetails.AutoWarmupEnabled = autoWarmupEnabled

	return nil
}

// GetBlacklistReports returns empty blacklist reports.
func (b *InMemoryBackend) GetBlacklistReports() (map[string][]string, error) {
	return map[string][]string{}, nil
}
