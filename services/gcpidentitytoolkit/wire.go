package gcpidentitytoolkit

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// flexInt decodes a JSON number or a quoted number, the two forms Google clients send for int64 fields.
type flexInt int64

func (f *flexInt) UnmarshalJSON(data []byte) error {
	data = bytes.Trim(data, `"`)
	if len(data) == 0 || string(data) == "null" {
		return nil
	}

	n, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		fl, ferr := strconv.ParseFloat(string(data), 64)
		if ferr != nil {
			return err //nolint:wrapcheck // surfaced as a JSON decode error
		}

		n = int64(fl)
	}

	*f = flexInt(n)

	return nil
}

type providerWire struct {
	ProviderID  string `json:"providerId,omitempty"`
	RawID       string `json:"rawId,omitempty"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	Phone       string `json:"phoneNumber,omitempty"`
	FederatedID string `json:"federatedId,omitempty"`
}

type userWire struct {
	PasswordUpdatedAt *float64       `json:"passwordUpdatedAt,omitempty"`
	CustomAttributes  string         `json:"customAttributes,omitempty"`
	TenantID          string         `json:"tenantId,omitempty"`
	DisplayName       string         `json:"displayName,omitempty"`
	PhotoURL          string         `json:"photoUrl,omitempty"`
	PhoneNumber       string         `json:"phoneNumber,omitempty"`
	PasswordHash      string         `json:"passwordHash,omitempty"`
	Salt              string         `json:"salt,omitempty"`
	LocalID           string         `json:"localId,omitempty"`
	Email             string         `json:"email,omitempty"`
	LastRefreshAt     string         `json:"lastRefreshAt,omitempty"`
	ProviderUserInfo  []providerWire `json:"providerUserInfo,omitempty"`
	ValidSince        flexInt        `json:"validSince,omitempty"`
	CreatedAt         flexInt        `json:"createdAt,omitempty"`
	LastLoginAt       flexInt        `json:"lastLoginAt,omitempty"`
	EmailVerified     bool           `json:"emailVerified,omitempty"`
	Disabled          bool           `json:"disabled,omitempty"`
	CustomAuth        bool           `json:"customAuth,omitempty"`
}

// MarshalJSON writes the int64 fields as strings, as the Identity Toolkit API does.
func (u userWire) MarshalJSON() ([]byte, error) {
	type alias userWire

	aux := struct {
		CreatedAt   string `json:"createdAt,omitempty"`
		LastLoginAt string `json:"lastLoginAt,omitempty"`
		ValidSince  string `json:"validSince,omitempty"`
		alias
	}{alias: alias(u)}

	if u.CreatedAt != 0 {
		aux.CreatedAt = strconv.FormatInt(int64(u.CreatedAt), 10)
	}

	if u.LastLoginAt != 0 {
		aux.LastLoginAt = strconv.FormatInt(int64(u.LastLoginAt), 10)
	}

	if u.ValidSince != 0 {
		aux.ValidSince = strconv.FormatInt(int64(u.ValidSince), 10)
	}

	return json.Marshal(aux)
}

// authRequest is the union of the request fields the account operations read.
type authRequest struct {
	DisplayName       *string        `json:"displayName"`
	DisableUser       *bool          `json:"disableUser"`
	EmailVerified     *bool          `json:"emailVerified"`
	Disabled          *bool          `json:"disabled"`
	LinkProvider      *providerWire  `json:"linkProviderUserInfo"`
	ReturnSecureToken *bool          `json:"returnSecureToken"`
	CustomAttributes  *string        `json:"customAttributes"`
	PhotoURL          *string        `json:"photoUrl"`
	ContinueURI       string         `json:"continueUri"`
	NewPassword       string         `json:"newPassword"`
	ProviderID        string         `json:"providerId"`
	Identifier        string         `json:"identifier"`
	Password          string         `json:"password"`
	IDToken           string         `json:"idToken"`
	TenantID          string         `json:"tenantId"`
	ContinueURL       string         `json:"continueUrl"`
	NewEmail          string         `json:"newEmail"`
	RequestType       string         `json:"requestType"`
	OobCode           string         `json:"oobCode"`
	Token             string         `json:"token"`
	DeleteAttribute   []string       `json:"deleteAttribute"`
	LocalID           flexStrings    `json:"localId"`
	PhoneNumber       flexStrings    `json:"phoneNumber"`
	FederatedUserID   []providerWire `json:"federatedUserId"`
	Email             flexStrings    `json:"email"`
	Provider          []string       `json:"provider"`
	DeleteProvider    []string       `json:"deleteProvider"`
	ValidSince        flexInt        `json:"validSince"`
	CreatedAt         flexInt        `json:"createdAt"`
	LastLoginAt       flexInt        `json:"lastLoginAt"`
	ValidDuration     flexInt        `json:"validDuration"`
	ReturnOobLink     bool           `json:"returnOobLink"`
}

// flexStrings decodes a string or an array of strings.
type flexStrings []string

func (f *flexStrings) UnmarshalJSON(data []byte) error {
	var one string
	if json.Unmarshal(data, &one) == nil {
		*f = flexStrings{one}

		return nil
	}

	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err //nolint:wrapcheck // surfaced as a JSON decode error
	}

	*f = many

	return nil
}

func (f flexStrings) first() string {
	if len(f) == 0 {
		return ""
	}

	return f[0]
}

type tokenResponse struct {
	Kind         string `json:"kind,omitempty"`
	LocalID      string `json:"localId,omitempty"`
	Email        string `json:"email,omitempty"`
	DisplayName  string `json:"displayName,omitempty"`
	IDToken      string `json:"idToken,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	ExpiresIn    string `json:"expiresIn,omitempty"`
	Registered   bool   `json:"registered,omitempty"`
	IsNewUser    bool   `json:"isNewUser,omitempty"`
}

type setAccountResponse struct {
	Kind             string         `json:"kind"`
	LocalID          string         `json:"localId,omitempty"`
	Email            string         `json:"email,omitempty"`
	DisplayName      string         `json:"displayName,omitempty"`
	PhotoURL         string         `json:"photoUrl,omitempty"`
	NewEmail         string         `json:"newEmail,omitempty"`
	PasswordHash     string         `json:"passwordHash,omitempty"`
	IDToken          string         `json:"idToken,omitempty"`
	RefreshToken     string         `json:"refreshToken,omitempty"`
	ExpiresIn        string         `json:"expiresIn,omitempty"`
	ProviderUserInfo []providerWire `json:"providerUserInfo,omitempty"`
	EmailVerified    bool           `json:"emailVerified,omitempty"`
}

type oobResponse struct {
	Kind    string `json:"kind"`
	Email   string `json:"email,omitempty"`
	OobCode string `json:"oobCode,omitempty"`
	OobLink string `json:"oobLink,omitempty"`
}
