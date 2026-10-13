package account

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	alternateEmailPattern = regexp.MustCompile(`^[\s]*[\w+=.#|!&-]+@[\w.-]+\.[\w]+[\s]*$`)
	alternatePhonePattern = regexp.MustCompile(`^[\s0-9()+-]+$`)
	contactPhonePattern   = regexp.MustCompile(`^[+][\s0-9()-]+$`)
	countryCodePattern    = regexp.MustCompile(`^[A-Za-z]{2}$`)
)

func stateRequired(countryCode string) bool {
	switch strings.ToUpper(countryCode) {
	case "US", "CA", "GB", "DE", "JP", "IN", "BR":
		return true
	}

	return false
}

const fieldPhoneNumber = "PhoneNumber"

const (
	maxAccountNameLen = 50
	maxEmailLen       = 254
	maxPhoneLen       = 25
	maxContactPhone   = 20
)

type lengthRule struct {
	field string
	value string
	max   int
}

func checkLengths(rules []lengthRule) error {
	for _, r := range rules {
		if n := utf8.RuneCountInString(r.value); n > r.max {
			return fmt.Errorf("%w: %s must be at most %d characters", errInvalidField, r.field, r.max)
		}
	}

	return nil
}

func validateAccountName(name string) error {
	return checkLengths([]lengthRule{{"AccountName", name, maxAccountNameLen}})
}

func validateAlternateContact(email, name, phone, title string) error {
	if err := checkLengths([]lengthRule{
		{"EmailAddress", email, maxEmailLen},
		{"Name", name, 64},
		{fieldPhoneNumber, phone, maxPhoneLen},
		{"Title", title, maxAccountNameLen},
	}); err != nil {
		return err
	}

	if !alternateEmailPattern.MatchString(email) {
		return fmt.Errorf("%w: EmailAddress is not a valid email address", errInvalidField)
	}

	if !alternatePhonePattern.MatchString(phone) {
		return fmt.Errorf("%w: PhoneNumber must contain only digits, spaces, and ()+-", errInvalidField)
	}

	return nil
}

func validateContactInformation(info *ContactInformation) error {
	if err := checkLengths([]lengthRule{
		{"AddressLine1", info.AddressLine1, 60},
		{"AddressLine2", info.AddressLine2, 60},
		{"AddressLine3", info.AddressLine3, 60},
		{"City", info.City, maxAccountNameLen},
		{"CompanyName", info.CompanyName, maxAccountNameLen},
		{"DistrictOrCounty", info.DistrictOrCounty, maxAccountNameLen},
		{"FullName", info.FullName, maxAccountNameLen},
		{fieldPhoneNumber, info.PhoneNumber, maxContactPhone},
		{"PostalCode", info.PostalCode, 20},
		{"StateOrRegion", info.StateOrRegion, maxAccountNameLen},
		{"WebsiteUrl", info.WebsiteURL, 256},
	}); err != nil {
		return err
	}

	if !countryCodePattern.MatchString(info.CountryCode) {
		return fmt.Errorf("%w: CountryCode must be a two-letter ISO-3166 code", errInvalidField)
	}

	if !contactPhonePattern.MatchString(info.PhoneNumber) {
		return fmt.Errorf("%w: PhoneNumber must start with + and contain only digits, spaces, and ()-", errInvalidField)
	}

	if stateRequired(info.CountryCode) && info.StateOrRegion == "" {
		return fmt.Errorf("%w: StateOrRegion is required for country %s", errInvalidField, info.CountryCode)
	}

	return nil
}
