package sns

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// isValidSMSAttributeName returns true if the attribute name is recognised by the AWS SNS API.
// Source: https://docs.aws.amazon.com/sns/latest/api/API_SetSMSAttributes.html
func isValidSMSAttributeName(name string) bool {
	switch name {
	case "MonthlySpendLimit",
		"DeliveryStatusIAMRole",
		"DeliveryStatusSuccessSamplingRate",
		"DefaultSenderID",
		"DefaultSMSType",
		"UsageReportS3Bucket":
		return true
	default:
		return false
	}
}

// isValidE164 returns true if the phone number string is a valid E.164 number
// (starts with '+' followed by 1–15 digits).
func isValidE164(phone string) bool {
	if len(phone) < 2 || len(phone) > 16 || phone[0] != '+' {
		return false
	}

	for _, c := range phone[1:] {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// GetSMSSandboxAccountStatus returns whether the account is in SMS sandbox mode.
// Defaults to true (sandbox mode active) matching the AWS default for new accounts.
func (b *InMemoryBackend) GetSMSSandboxAccountStatus() (bool, error) {
	b.mu.RLock("GetSMSSandboxAccountStatus")
	defer b.mu.RUnlock()

	return b.smsSandboxEnabled, nil
}

// SetSMSSandboxMode configures sandbox mode. AWS does not expose an API for this —
// use this method in tests or operator tooling to simulate production mode.
func (b *InMemoryBackend) SetSMSSandboxMode(enabled bool) {
	b.mu.Lock("SetSMSSandboxMode")
	defer b.mu.Unlock()

	b.smsSandboxEnabled = enabled
}

// CreateSMSSandboxPhoneNumber adds a phone number to the SMS sandbox.
// The phone number must be in E.164 format. Numbers start with status "Pending"
// and must be verified via VerifySMSSandboxPhoneNumber before they can receive SMS.
func (b *InMemoryBackend) CreateSMSSandboxPhoneNumber(phoneNumber, languageCode string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	if b.smsSandbox.Has(phoneNumber) {
		return ErrSandboxPhoneAlreadyExists
	}

	b.smsSandbox.Put(&SandboxPhoneNumber{
		PhoneNumber:       phoneNumber,
		LanguageCode:      languageCode,
		Status:            "Pending",
		CreationTimestamp: time.Now().UTC(),
	})

	return nil
}

// DeleteSMSSandboxPhoneNumber removes a phone number from the SMS sandbox.
// The phone number must be in E.164 format.
func (b *InMemoryBackend) DeleteSMSSandboxPhoneNumber(phoneNumber string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	if !b.smsSandbox.Delete(phoneNumber) {
		return ErrPhoneNumberNotFound
	}

	return nil
}

// VerifySMSSandboxPhoneNumber marks a sandbox phone number as Verified.
// In the mock backend, any non-empty one-time password is accepted.
func (b *InMemoryBackend) VerifySMSSandboxPhoneNumber(phoneNumber, oneTimePassword string) error {
	if oneTimePassword == "" {
		return fmt.Errorf("%w: OneTimePassword is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifySMSSandboxPhoneNumber")
	defer b.mu.Unlock()

	entry, exists := b.smsSandbox.Get(phoneNumber)
	if !exists {
		return ErrPhoneNumberNotFound
	}

	entry.Status = "Verified"

	return nil
}

// ListSMSSandboxPhoneNumbers returns a paginated list of SMS sandbox phone numbers,
// a next-page token (empty when the last page is reached), and any error.
// maxResults controls the page size; 0 means the default (100). Values exceeding 100 are clamped.
func (b *InMemoryBackend) ListSMSSandboxPhoneNumbers(
	nextToken string,
	maxResults int,
) ([]SandboxPhoneNumber, string, error) {
	b.mu.RLock("ListSMSSandboxPhoneNumbers")
	defer b.mu.RUnlock()

	all := b.sortedSandboxNumbers()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	size := resolvePageSize(maxResults, defaultListSMSSandboxResults, maxListSMSSandboxResults)
	nums, next := paginate(all, offset, size)

	return nums, next, nil
}

// sortedSandboxNumbers returns sandbox phone numbers sorted by phone number.
// Must be called with at least RLock held.
func (b *InMemoryBackend) sortedSandboxNumbers() []SandboxPhoneNumber {
	nums := make([]SandboxPhoneNumber, 0, b.smsSandbox.Len())
	for _, n := range b.smsSandbox.All() {
		nums = append(nums, *n)
	}

	sort.Slice(nums, func(i, j int) bool {
		return nums[i].PhoneNumber < nums[j].PhoneNumber
	})

	return nums
}

// CheckIfPhoneNumberIsOptedOut returns whether a phone number has opted out of SMS messages.
func (b *InMemoryBackend) CheckIfPhoneNumberIsOptedOut(phoneNumber string) (bool, error) {
	if !isValidE164(phoneNumber) {
		return false, fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.RLock("CheckIfPhoneNumberIsOptedOut")
	defer b.mu.RUnlock()

	return b.optedOutPhoneNumbers[phoneNumber], nil
}

// ListPhoneNumbersOptedOut returns a paginated list of phone numbers opted out of SMS,
// a next-page token (empty when the last page is reached), and any error.
// maxResults controls the page size; 0 means the default (100). Values exceeding 100 are clamped.
func (b *InMemoryBackend) ListPhoneNumbersOptedOut(
	nextToken string,
	maxResults int,
) ([]string, string, error) {
	b.mu.RLock("ListPhoneNumbersOptedOut")
	defer b.mu.RUnlock()

	all := b.sortedOptedOutNumbers()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	size := resolvePageSize(maxResults, defaultListOptedOutResults, maxListOptedOutResults)
	nums, next := paginate(all, offset, size)

	return nums, next, nil
}

// sortedOptedOutNumbers returns opted-out phone numbers sorted lexicographically.
// Must be called with at least RLock held.
func (b *InMemoryBackend) sortedOptedOutNumbers() []string {
	nums := make([]string, 0, len(b.optedOutPhoneNumbers))
	for phone, optedOut := range b.optedOutPhoneNumbers {
		if optedOut {
			nums = append(nums, phone)
		}
	}

	sort.Strings(nums)

	return nums
}

// OptInPhoneNumber removes a phone number from the opt-out list so it can receive SMS messages.
// The phone number must be in E.164 format.
func (b *InMemoryBackend) OptInPhoneNumber(phoneNumber string) error {
	if !isValidE164(phoneNumber) {
		return fmt.Errorf("%w: phone number must be in E.164 format", ErrInvalidParameter)
	}

	b.mu.Lock("OptInPhoneNumber")
	defer b.mu.Unlock()

	delete(b.optedOutPhoneNumbers, phoneNumber)

	return nil
}

// GetSMSAttributes returns the current SMS account attributes, optionally filtered by name.
// If names is empty all attributes are returned.
func (b *InMemoryBackend) GetSMSAttributes(names []string) (map[string]string, error) {
	b.mu.RLock("GetSMSAttributes")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		result := make(map[string]string, len(b.smsAttributes))
		maps.Copy(result, b.smsAttributes)

		return result, nil
	}

	result := make(map[string]string, len(names))
	for _, name := range names {
		result[name] = b.smsAttributes[name]
	}

	return result, nil
}

// SetSMSAttributes stores global SMS account attributes.
// Existing attribute keys are updated; unspecified keys are left unchanged.
// Only known AWS attribute names are accepted; unknown names are rejected with ErrInvalidParameter.
func (b *InMemoryBackend) SetSMSAttributes(attributes map[string]string) error {
	for k := range attributes {
		if !isValidSMSAttributeName(k) {
			return fmt.Errorf("%w: unknown SMS attribute name %q", ErrInvalidParameter, k)
		}
	}

	b.mu.Lock("SetSMSAttributes")
	defer b.mu.Unlock()

	maps.Copy(b.smsAttributes, attributes)

	return nil
}
