package gcpsql

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	complexityDefault   = "COMPLEXITY_DEFAULT"
	maxPasswordHistory  = 100
	historySaltBytes    = 16
	historyIterations   = 4096
	maxPasswordBytes    = 1024
	policyKey           = "passwordValidationPolicy"
	passwordChangedFmt  = time.RFC3339Nano
	secondsSuffix       = "s"
	maxPolicyMinLength  = maxPasswordBytes
	maxPolicyReuseCount = maxPasswordHistory
)

// passwordPolicy is settings.passwordValidationPolicy; fields are documented in the sqladmin v1 discovery document.
type passwordPolicy struct {
	complexity string
	minLength  int64
	reuse      int64
	interval   time.Duration
	enabled    bool
	noUsername bool
}

func policyInt(v any) int64 {
	switch t := v.(type) {
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)

		return n
	}

	return 0
}

func parsePasswordPolicy(settings map[string]any) passwordPolicy {
	m := asMap(settings[policyKey])
	if m == nil {
		return passwordPolicy{}
	}

	on, _ := m["enablePasswordPolicy"].(bool)
	noUser, _ := m["disallowUsernameSubstring"].(bool)
	cx, _ := m["complexity"].(string)
	p := passwordPolicy{
		enabled: on, noUsername: noUser, complexity: cx,
		minLength: policyInt(m["minLength"]), reuse: policyInt(m["reuseInterval"]),
	}

	if s, ok := m["passwordChangeInterval"].(string); ok {
		if n, err := strconv.ParseFloat(strings.TrimSuffix(s, secondsSuffix), 64); err == nil && n > 0 {
			p.interval = time.Duration(n * float64(time.Second))
		}
	}

	return p
}

// validatePasswordPolicy rejects a policy that cannot be enforced.
func validatePasswordPolicy(settings map[string]any) error {
	m := asMap(settings[policyKey])
	if m == nil {
		return nil
	}

	if n := policyInt(m["minLength"]); n < 0 || n > maxPolicyMinLength {
		return errInvalid("passwordValidationPolicy.minLength must be between 0 and %d.", maxPolicyMinLength)
	}

	if n := policyInt(m["reuseInterval"]); n < 0 || n > maxPolicyReuseCount {
		return errInvalid("passwordValidationPolicy.reuseInterval must be between 0 and %d.", maxPolicyReuseCount)
	}

	return nil
}

// historyEntry is "salt$hash" in hex: PBKDF2-HMAC-SHA256 over the password with a random per-entry salt.
func historyEntry(user, pw string) string {
	salt := make([]byte, historySaltBytes)
	_, _ = rand.Read(salt)

	return hex.EncodeToString(salt) + "$" + hex.EncodeToString(historyKey(user, pw, salt))
}

func historyKey(user, pw string, salt []byte) []byte {
	key, err := pbkdf2.Key(sha256.New, user+"\x00"+pw, salt, historyIterations, sha256.Size)
	if err != nil {
		return nil
	}

	return key
}

// historyMatches reports whether any entry was made from pw, comparing in constant time.
func historyMatches(entries []string, user, pw string) bool {
	found := 0

	for _, e := range entries {
		saltHex, sumHex, ok := strings.Cut(e, "$")
		if !ok {
			continue
		}

		salt, serr := hex.DecodeString(saltHex)
		sum, herr := hex.DecodeString(sumHex)

		if serr != nil || herr != nil {
			continue
		}

		found |= subtle.ConstantTimeCompare(sum, historyKey(user, pw, salt))
	}

	return found == 1
}

func complexEnough(pw string) bool {
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

	return lower && upper && digit && other
}

// checkPassword applies the instance password policy to a new password for u.
func checkPassword(p passwordPolicy, u *userRec, pw string, pg bool, now time.Time) error {
	if len(pw) > maxPasswordBytes {
		return errInvalid("The password is longer than %d bytes.", maxPasswordBytes)
	}

	if !p.enabled || pw == "" {
		return nil
	}

	if err := checkPasswordText(p, u.User.Name, pw); err != nil {
		return err
	}

	if p.reuse > 0 && historyMatches(lastN(u.History, p.reuse), u.User.Name, pw) {
		return errInvalid("Password validation failed: the password was used recently and cannot be reused.")
	}

	if pg && p.interval > 0 && u.Changed != "" {
		if at, err := time.Parse(passwordChangedFmt, u.Changed); err == nil && now.Sub(at) < p.interval {
			return errInvalid("Password validation failed: the password was changed less than %s ago.", p.interval)
		}
	}

	return nil
}

func checkPasswordText(p passwordPolicy, user, pw string) error {
	if int64(len([]rune(pw))) < p.minLength {
		return errInvalid("Password validation failed: the password must have at least %d characters.", p.minLength)
	}

	if p.complexity == complexityDefault && !complexEnough(pw) {
		return errInvalid("Password validation failed: the password must combine lowercase, uppercase, numeric " +
			"and non-alphanumeric characters.")
	}

	if p.noUsername && user != "" && strings.Contains(strings.ToLower(pw), strings.ToLower(user)) {
		return errInvalid("Password validation failed: the password must not contain the user name.")
	}

	return nil
}

func lastN(list []string, n int64) []string {
	if int64(len(list)) <= n {
		return list
	}

	return list[int64(len(list))-n:]
}

// recordPassword remembers pw for the reuse check and stamps the change time.
func recordPassword(u *userRec, pw string, now time.Time) {
	if pw == "" {
		return
	}

	u.History = append(u.History, historyEntry(u.User.Name, pw))
	if len(u.History) > maxPasswordHistory {
		u.History = u.History[len(u.History)-maxPasswordHistory:]
	}

	u.Changed = now.UTC().Format(passwordChangedFmt)
}
