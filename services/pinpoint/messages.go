package pinpoint

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const (
	minPhoneLength = 10
	otpModulus     = 1000000

	statusCodeOK = 200

	deliveryStatusSuccessful = "SUCCESSFUL"
)

// SendMessages sends messages and tracks send count.
func (b *InMemoryBackend) SendMessages(
	appID string,
	req sendMessagesRequest,
) (*messageResponse, error) {
	b.mu.Lock("SendMessages")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	result := &messageResponse{ApplicationID: appID, Result: make(map[string]messageResult)}

	for addr := range req.MessageRequest.Addresses {
		result.Result[addr] = messageResult{
			DeliveryStatus: deliveryStatusSuccessful,
			MessageID:      uuid.NewString(),
			StatusCode:     statusCodeOK,
		}
		b.sentMessages[appID]++
	}

	return result, nil
}

// SendUsersMessages sends messages to users, returning per-endpoint results keyed by userID.
func (b *InMemoryBackend) SendUsersMessages(
	appID string,
	req sendUsersMessagesRequest,
) (*usersMessageResponse, error) {
	b.mu.Lock("SendUsersMessages")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	result := make(map[string]map[string]messageResult)

	for userID := range req.SendUsersMessageRequest.Users {
		endpointResults := make(map[string]messageResult)

		for _, ep := range b.endpoints.All() {
			if ep.ApplicationID != appID || ep.UserID != userID {
				continue
			}

			endpointID := ep.ID
			endpointResults[endpointID] = messageResult{
				DeliveryStatus: deliveryStatusSuccessful,
				MessageID:      uuid.NewString(),
				StatusCode:     statusCodeOK,
			}
		}

		// If user has no registered endpoints, return a placeholder per-user entry.
		if len(endpointResults) == 0 {
			endpointResults["unknown"] = messageResult{
				DeliveryStatus: "OPT_OUT",
				StatusCode:     http.StatusOK,
			}
		}

		result[userID] = endpointResults
		b.sentMessages[appID]++
	}

	return &usersMessageResponse{ApplicationID: appID, Result: result}, nil
}

// SendOTPMessage sends an OTP message and stores the generated code.
func (b *InMemoryBackend) SendOTPMessage(appID string) (*sendOTPMessageResponse, error) {
	b.mu.Lock("SendOTPMessage")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	// Generate and store a 6-digit OTP code.
	//nolint:gosec // OTP codes are not cryptographically sensitive in mock
	code := fmt.Sprintf("%06d", rand.IntN(otpModulus))
	b.otpCodes[appID] = code

	msgID := uuid.NewString()

	return &sendOTPMessageResponse{
		MessageResponse: messageResponse{
			ApplicationID: appID,
			Result: map[string]messageResult{
				appID: {DeliveryStatus: deliveryStatusSuccessful, MessageID: msgID, StatusCode: statusCodeOK},
			},
		},
	}, nil
}

// VerifyOTPMessage verifies an OTP code for the given app.
// If code is non-empty it must match the stored code exactly.
// If code is empty it falls back to checking whether any OTP was ever sent.
func (b *InMemoryBackend) VerifyOTPMessage(appID, code string) (*verifyOTPMessageResponse, error) {
	b.mu.RLock("VerifyOTPMessage")
	defer b.mu.RUnlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	stored, hasPendingOTP := b.otpCodes[appID]

	var valid bool

	if code != "" {
		valid = hasPendingOTP && stored == code
	} else {
		valid = hasPendingOTP
	}

	return &verifyOTPMessageResponse{Valid: valid}, nil
}

// countryInfo holds basic phone-number country metadata keyed by E164 country-code prefix.
type countryInfo struct {
	ISO2        string
	NumericCode string
	Name        string
	Timezone    string
}

// lookupCountry returns country metadata for the given E164 number.
func lookupCountry(e164 string) countryInfo {
	type prefixEntry struct {
		prefix string
		info   countryInfo
	}

	table := []prefixEntry{
		{"+1", countryInfo{"US", "1", "United States", "America/New_York"}},
		{"+44", countryInfo{"GB", "44", "United Kingdom", "Europe/London"}},
		{"+49", countryInfo{"DE", "49", "Germany", "Europe/Berlin"}},
		{"+33", countryInfo{"FR", "33", "France", "Europe/Paris"}},
		{"+81", countryInfo{"JP", "81", "Japan", "Asia/Tokyo"}},
		{"+86", countryInfo{"CN", "86", "China", "Asia/Shanghai"}},
		{"+91", countryInfo{"IN", "91", "India", "Asia/Kolkata"}},
		{"+55", countryInfo{"BR", "55", "Brazil", "America/Sao_Paulo"}},
		{"+61", countryInfo{"AU", "61", "Australia", "Australia/Sydney"}},
		{"+52", countryInfo{"MX", "52", "Mexico", "America/Mexico_City"}},
	}

	for _, entry := range table {
		if strings.HasPrefix(e164, entry.prefix) {
			return entry.info
		}
	}

	return countryInfo{ISO2: "ZZ", NumericCode: "0", Name: "Unknown", Timezone: "UTC"}
}

// PhoneNumberValidate validates a phone number and returns a cleaned E164 response.
func (b *InMemoryBackend) PhoneNumberValidate(
	phoneNumber string,
) (*phoneNumberValidateResponse, error) {
	// Normalise to E164: strip non-digit chars, prepend + if missing.
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}

		return -1
	}, phoneNumber)

	var e164 string

	switch {
	case strings.HasPrefix(phoneNumber, "+"):
		e164 = "+" + digits
	case len(digits) == minPhoneLength:
		// Assume US number.
		e164 = "+1" + digits
	default:
		e164 = "+" + digits
	}

	country := lookupCountry(e164)

	return &phoneNumberValidateResponse{
		NumberValidateResponse: numberValidateResponse{
			Carrier:                           "Unknown",
			PhoneType:                         "MOBILE",
			PhoneTypeCode:                     0,
			CleansedPhoneNumberE164:           e164,
			CleansedPhoneNumberNationalFormat: e164,
			Country:                           country.Name,
			CountryCodeIso2:                   country.ISO2,
			CountryCodeNumeric:                country.NumericCode,
			OriginalCountryCodeIso2:           country.ISO2,
			OriginalPhoneNumber:               phoneNumber,
			Timezone:                          country.Timezone,
		},
	}, nil
}
