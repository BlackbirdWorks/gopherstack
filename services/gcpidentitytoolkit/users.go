package gcpidentitytoolkit

import (
	"encoding/base64"
	"encoding/json"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	minPasswordLen = 6
	expiresInSecs  = "3600"
)

var e164 = regexp.MustCompile(`^\+[0-9]{4,15}$`)

func validEmail(email string) bool {
	if email == "" || strings.ContainsAny(email, " \t\r\n") {
		return false
	}

	addr, err := mail.ParseAddress(email)

	return err == nil && addr.Address == email && strings.Count(email, "@") == 1
}

func hashB64(b []byte) string { return base64.URLEncoding.EncodeToString(b) }

// checkPassword enforces the default minimum length and, when enforced, the project's password policy.
func checkPassword(sc *scope, pw string) error {
	if len(pw) < minPasswordLen {
		return idtk("WEAK_PASSWORD", "Password should be at least 6 characters")
	}

	pol := sc.policy
	if pol == nil || pol.PasswordPolicyEnforcementState != "ENFORCE" || len(pol.PasswordPolicyVersions) == 0 {
		return nil
	}

	return checkStrength(pol.PasswordPolicyVersions[len(pol.PasswordPolicyVersions)-1].CustomStrengthOptions, pw)
}

func checkStrength(o *idtkv2.GoogleCloudIdentitytoolkitAdminV2CustomStrengthOptions, pw string) error {
	if o == nil {
		return nil
	}

	var missing []string

	if o.MinPasswordLength > 0 && int64(len(pw)) < o.MinPasswordLength {
		missing = append(
			missing,
			"Password must contain at least "+strconv.FormatInt(o.MinPasswordLength, 10)+" characters",
		)
	}

	if o.MaxPasswordLength > 0 && int64(len(pw)) > o.MaxPasswordLength {
		missing = append(
			missing,
			"Password must contain at most "+strconv.FormatInt(o.MaxPasswordLength, 10)+" characters",
		)
	}

	var lower, upper, digit, other bool

	for _, r := range pw {
		switch {
		case unicode.IsLower(r):
			lower = true
		case unicode.IsUpper(r):
			upper = true
		case unicode.IsDigit(r):
			digit = true
		default:
			other = true
		}
	}

	for _, c := range []struct {
		msg      string
		need, ok bool
	}{
		{"Password must contain a lower case character", o.ContainsLowercaseCharacter, lower},
		{"Password must contain an upper case character", o.ContainsUppercaseCharacter, upper},
		{"Password must contain a numeric character", o.ContainsNumericCharacter, digit},
		{"Password must contain a non-alphanumeric character", o.ContainsNonAlphanumericCharacter, other},
	} {
		if c.need && !c.ok {
			missing = append(missing, c.msg)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return idtk(
		"PASSWORD_DOES_NOT_MEET_REQUIREMENTS",
		"Missing password requirements: ["+strings.Join(missing, ", ")+"]",
	)
}

// setPasswordLocked hashes pw with the project's native scheme.
func (b *InMemoryBackend) setPasswordLocked(sc *scope, u *userRec, pw string) error {
	salt := newSalt()

	h, err := sc.proj.Hash.compute(pw, salt)
	if err != nil {
		return err
	}

	u.PasswordHash, u.Salt, u.Hash = h, salt, nil
	u.PasswordAt = b.nowMillis()

	return nil
}

// syncProvidersLocked keeps the derived password and phone provider entries in step with the account.
func syncProviders(u *userRec) {
	kept := u.Providers[:0:0]

	for _, p := range u.Providers {
		if p.ProviderID != passwordProv && p.ProviderID != phoneProv {
			kept = append(kept, p)
		}
	}

	if len(u.PasswordHash) > 0 && u.Email != "" {
		kept = append([]providerRec{{
			ProviderID: passwordProv, RawID: u.Email, Email: u.Email, FederatedID: u.Email,
			DisplayName: u.DisplayName, PhotoURL: u.PhotoURL,
		}}, kept...)
	}

	if u.Phone != "" {
		kept = append(kept, providerRec{ProviderID: phoneProv, RawID: u.Phone, Phone: u.Phone})
	}

	u.Providers = kept
}

type newUser struct {
	hash          *hashSpec
	uid           string
	email         string
	displayName   string
	photoURL      string
	phone         string
	password      string
	customAttrs   string
	providers     []providerRec
	passwordHash  []byte
	salt          []byte
	createdAt     int64
	lastLoginAt   int64
	emailVerified bool
	disabled      bool
	anonymous     bool
}

func (b *InMemoryBackend) createUserLocked(sc *scope, n newUser, overwrite bool) (*userRec, error) {
	uid := n.uid
	if uid == "" {
		uid = newUID()
		for sc.users[uid] != nil {
			uid = newUID()
		}
	}

	n.email = strs.Fold(n.email)

	if err := validateNewUserLocked(sc, uid, n, overwrite); err != nil {
		return nil, err
	}

	now := b.nowMillis()
	u := &userRec{
		UID: uid, Email: n.email, DisplayName: n.displayName, PhotoURL: n.photoURL, Phone: n.phone,
		EmailVerified: n.emailVerified, Disabled: n.disabled, Anonymous: n.anonymous, CustomAttrs: n.customAttrs,
		CreatedAt: now, LastLoginAt: n.lastLoginAt, Providers: n.providers,
		PasswordHash: n.passwordHash, Salt: n.salt, Hash: n.hash,
	}

	if n.createdAt > 0 {
		u.CreatedAt = n.createdAt
	}

	if n.password != "" {
		if err := b.setPasswordLocked(sc, u, n.password); err != nil {
			return nil, err
		}
	}

	syncProviders(u)
	sc.users[uid] = u

	return u, nil
}

func validateNewUserLocked(sc *scope, uid string, n newUser, overwrite bool) error {
	if len(uid) > maxUIDLen {
		return idtk("INVALID_LOCAL_ID", "Invalid localId")
	}

	if sc.users[uid] != nil && !overwrite {
		return idtk("DUPLICATE_LOCAL_ID")
	}

	if n.email != "" {
		if !validEmail(n.email) {
			return idtk("INVALID_EMAIL")
		}

		if o := sc.byEmail(n.email); o != nil && o.UID != uid {
			return idtk("EMAIL_EXISTS")
		}
	}

	if n.phone != "" {
		if !e164.MatchString(n.phone) {
			return idtk("INVALID_PHONE_NUMBER", "Invalid format.")
		}

		if o := sc.byPhone(n.phone); o != nil && o.UID != uid {
			return idtk("PHONE_NUMBER_EXISTS")
		}
	}

	if n.password != "" {
		return checkPassword(sc, n.password)
	}

	return nil
}

func (b *InMemoryBackend) toWire(sc *scope, u *userRec, admin bool) userWire {
	w := userWire{
		LocalID: u.UID, Email: u.Email, DisplayName: u.DisplayName, PhotoURL: u.PhotoURL, PhoneNumber: u.Phone,
		EmailVerified: u.EmailVerified, Disabled: u.Disabled, CustomAttributes: u.CustomAttrs,
		CreatedAt: flexInt(u.CreatedAt), LastLoginAt: flexInt(u.LastLoginAt), ValidSince: flexInt(u.ValidSince),
		TenantID: sc.tenant, CustomAuth: false,
	}

	if u.LastRefreshAt > 0 {
		w.LastRefreshAt = time.UnixMilli(u.LastRefreshAt).UTC().Format(timeLayoutRFC)
	}

	if u.PasswordAt > 0 {
		f := float64(u.PasswordAt)
		w.PasswordUpdatedAt = &f
	}

	if len(u.PasswordHash) > 0 {
		if admin {
			w.PasswordHash = hashB64(u.PasswordHash)
			if len(u.Salt) > 0 {
				w.Salt = hashB64(u.Salt)
			}
		} else {
			w.PasswordHash = redactedHash
		}
	}

	for _, p := range u.Providers {
		w.ProviderUserInfo = append(w.ProviderUserInfo, providerWire(p))
	}

	return w
}

func parseClaims(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}

	if len(raw) > maxClaimsBytes {
		return "", idtk("CLAIMS_TOO_LARGE")
	}

	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return "", idtk("INVALID_CLAIMS")
	}

	for k := range m {
		if reservedClaims[k] {
			return "", idtk("FORBIDDEN_CLAIM", k)
		}
	}

	if len(m) == 0 {
		return "", nil
	}

	return raw, nil
}
