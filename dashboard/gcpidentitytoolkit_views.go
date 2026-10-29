package dashboard

import (
	"encoding/json"
	"strconv"
	"strings"
)

type idpProvider struct {
	ProviderID  string `json:"providerId,omitempty"`
	RawID       string `json:"rawId,omitempty"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
}

// idpUser is the allow-listed view of an account; password hashes and salts have no field here.
type idpUser struct {
	PasswordUpdatedAt *float64      `json:"passwordUpdatedAt,omitempty"`
	LocalID           string        `json:"localId"`
	Email             string        `json:"email,omitempty"`
	DisplayName       string        `json:"displayName,omitempty"`
	PhotoURL          string        `json:"photoUrl,omitempty"`
	PhoneNumber       string        `json:"phoneNumber,omitempty"`
	CustomAttributes  string        `json:"customAttributes,omitempty"`
	TenantID          string        `json:"tenantId,omitempty"`
	CreatedAt         string        `json:"createdAt,omitempty"`
	LastLoginAt       string        `json:"lastLoginAt,omitempty"`
	LastRefreshAt     string        `json:"lastRefreshAt,omitempty"`
	ValidSince        string        `json:"validSince,omitempty"`
	Providers         []idpProvider `json:"providers"`
	EmailVerified     bool          `json:"emailVerified"`
	Disabled          bool          `json:"disabled"`
	HasPassword       bool          `json:"hasPassword"`
}

type idpUserWire struct {
	ProviderUserInfo []idpProvider `json:"providerUserInfo"`
	PasswordHash     string        `json:"passwordHash"`
	idpUser
}

func idpUsersView(raw []byte) (any, error) {
	var in struct {
		RecordsCount string        `json:"recordsCount"`
		Users        []idpUserWire `json:"userInfo"`
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	users := make([]idpUser, 0, len(in.Users))

	for _, w := range in.Users {
		u := w.idpUser
		u.HasPassword = w.PasswordHash != ""
		u.Providers = w.ProviderUserInfo

		if u.Providers == nil {
			u.Providers = []idpProvider{}
		}

		users = append(users, u)
	}

	total, _ := strconv.Atoi(in.RecordsCount)

	return map[string]any{gcpIDPUsers: users, "total": max(total, len(users))}, nil
}

func idpLocalIDView(raw []byte) (any, error) {
	var in struct {
		LocalID string `json:"localId"`
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	return map[string]string{gcpIDPLocalID: in.LocalID}, nil
}

func idpEmptyView([]byte) (any, error) { return map[string]string{}, nil }

func idpOOBView(raw []byte) (any, error) {
	var in struct {
		Codes []struct {
			Email       string `json:"email"`
			OobCode     string `json:"oobCode"`
			OobLink     string `json:"oobLink"`
			RequestType string `json:"requestType"`
		} `json:"oobCodes"`
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	type code struct {
		Email       string `json:"email"`
		OobCode     string `json:"oobCode"`
		OobLink     string `json:"oobLink"`
		RequestType string `json:"requestType"`
	}

	out := make([]code, 0, len(in.Codes))
	for _, c := range in.Codes {
		out = append(out, code(c))
	}

	return map[string]any{"oobCodes": out}, nil
}

func idpTenantsView(raw []byte) (any, error) {
	var in struct {
		NextPageToken string `json:"nextPageToken"`
		Tenants       []struct {
			Name                  string `json:"name"`
			DisplayName           string `json:"displayName"`
			AllowPasswordSignup   bool   `json:"allowPasswordSignup"`
			EnableEmailLinkSignin bool   `json:"enableEmailLinkSignin"`
			EnableAnonymousUser   bool   `json:"enableAnonymousUser"`
		} `json:"tenants"`
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	type tenant struct {
		ID                    string `json:"id"`
		Name                  string `json:"name"`
		DisplayName           string `json:"displayName"`
		AllowPasswordSignup   bool   `json:"allowPasswordSignup"`
		EnableEmailLinkSignin bool   `json:"enableEmailLinkSignin"`
		EnableAnonymousUser   bool   `json:"enableAnonymousUser"`
	}

	out := make([]tenant, 0, len(in.Tenants))

	for _, t := range in.Tenants {
		out = append(out, tenant{
			ID: t.Name[strings.LastIndex(t.Name, "/")+1:], Name: t.Name, DisplayName: t.DisplayName,
			AllowPasswordSignup: t.AllowPasswordSignup, EnableEmailLinkSignin: t.EnableEmailLinkSignin,
			EnableAnonymousUser: t.EnableAnonymousUser,
		})
	}

	return map[string]any{"tenants": out, "nextPageToken": in.NextPageToken}, nil
}

// idpConfigView keeps only the sign-in toggles; the API key and hash parameters stay server side.
func idpConfigView(raw []byte) (any, error) {
	var in struct {
		Name    string   `json:"name"`
		Domains []string `json:"authorizedDomains"`
		SignIn  struct {
			Email struct {
				Enabled bool `json:"enabled"`
			} `json:"email"`
			Anonymous struct {
				Enabled bool `json:"enabled"`
			} `json:"anonymous"`
		} `json:"signIn"`
	}

	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err //nolint:wrapcheck // surfaced as a 502 by the caller
	}

	if in.Domains == nil {
		in.Domains = []string{}
	}

	return struct {
		Name             string   `json:"name"`
		Domains          []string `json:"authorizedDomains"`
		EmailEnabled     bool     `json:"emailEnabled"`
		AnonymousEnabled bool     `json:"anonymousEnabled"`
	}{in.Name, in.Domains, in.SignIn.Email.Enabled, in.SignIn.Anonymous.Enabled}, nil
}

func joinComma(s []string) string { return strings.Join(s, ",") }

// boundedInt parses an optional integer query value within [lo, hi]; empty yields def.
func boundedInt(raw string, def, lo, hi int) (int, bool) {
	if raw == "" {
		return def, true
	}

	n, err := strconv.Atoi(raw)

	return n, err == nil && n >= lo && n <= hi
}
