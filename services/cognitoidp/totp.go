package cognitoidp

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // RFC 6238 TOTP mandates HMAC-SHA1; this is not used for anything else.
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// TOTP (RFC 6238) parameters matching AWS Cognito's software-token MFA implementation:
// 30-second time step, 6-digit codes, HMAC-SHA1 (the algorithm authenticator apps such as
// Google Authenticator and Authy assume when no otpauth "algorithm" parameter is given).
const (
	totpStepSeconds = 30
	totpDigits      = 6
	// totpSkewSteps allows the previous/next time step to account for clock drift between
	// the emulator and whatever generated the code, matching real Cognito's leeway.
	totpSkewSteps = 1
	// totpDigitsPow is 10^totpDigits, used to truncate the HOTP value to totpDigits.
	totpDigitsPow = 1000000

	// RFC 4226 §5.3 dynamic-truncation constants: hotpOffsetMask picks the low nibble of
	// the last HMAC byte as the truncation offset; hotpByteMask masks a single byte;
	// hotpTopByteMask additionally clears the HOTP result's sign bit (byte 0 of the 31-bit
	// truncated value must be non-negative when interpreted as a big-endian int32).
	hotpOffsetMask  = 0x0f
	hotpByteMask    = 0xff
	hotpTopByteMask = 0x7f
)

// GenerateTOTPCode computes the RFC 6238 TOTP code for the given base32-encoded secret at
// time t. It is exported so integration tests (and any AWS-SDK-driven caller that needs to
// simulate a real authenticator app) can compute the expected code for a secret returned by
// AssociateSoftwareToken, the same way a real TOTP app would.
func GenerateTOTPCode(secretBase32 string, t time.Time) (string, error) {
	key, err := decodeTOTPSecret(secretBase32)
	if err != nil {
		return "", err
	}

	return hotpCode(key, totpCounter(t)), nil
}

// totpCounter converts a wall-clock time to the RFC 6238 time-step counter.
func totpCounter(t time.Time) uint64 {
	unix := max(t.Unix(), 0)

	return uint64(unix) / totpStepSeconds
}

// decodeTOTPSecret decodes a base32 TOTP secret, tolerating both padded and unpadded
// encodings and the presence/absence of surrounding whitespace, since real authenticator
// apps and SDKs vary in how they store/transcribe otpauth secrets.
func decodeTOTPSecret(secretBase32 string) ([]byte, error) {
	clean := strings.ToUpper(strings.TrimSpace(secretBase32))
	clean = strings.ReplaceAll(clean, " ", "")

	if key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(clean); err == nil {
		return key, nil
	}

	key, err := base32.StdEncoding.DecodeString(clean)
	if err != nil {
		return nil, fmt.Errorf("decoding TOTP secret: %w", err)
	}

	return key, nil
}

// hotpCode computes the RFC 4226 HOTP value for the given key and counter, truncated to
// totpDigits decimal digits (zero-padded).
func hotpCode(key []byte, counter uint64) string {
	var counterBytes [8]byte
	binary.BigEndian.PutUint64(counterBytes[:], counter)

	mac := hmac.New(sha1.New, key)
	mac.Write(counterBytes[:])
	sum := mac.Sum(nil)

	// Dynamic truncation per RFC 4226 §5.3.
	offset := sum[len(sum)-1] & hotpOffsetMask
	binCode := (uint32(sum[offset])&hotpTopByteMask)<<24 |
		(uint32(sum[offset+1])&hotpByteMask)<<16 |
		(uint32(sum[offset+2])&hotpByteMask)<<8 |
		(uint32(sum[offset+3]) & hotpByteMask)

	code := binCode % totpDigitsPow

	return fmt.Sprintf("%0*d", totpDigits, code)
}

// verifyTOTPCode reports whether code is a valid TOTP for secretBase32 at time t, allowing
// +/- totpSkewSteps time steps of clock drift (matching real Cognito's tolerance for
// software-token MFA). An empty or undecodable secret never validates.
func verifyTOTPCode(secretBase32, code string, t time.Time) bool {
	key, err := decodeTOTPSecret(secretBase32)
	if err != nil || len(key) == 0 {
		return false
	}

	counter := totpCounter(t)

	for skew := -totpSkewSteps; skew <= totpSkewSteps; skew++ {
		c := counter
		if skew < 0 {
			step := uint64(-skew)
			if c < step {
				continue
			}

			c -= step
		} else {
			c += uint64(skew)
		}

		if hmac.Equal([]byte(hotpCode(key, c)), []byte(code)) {
			return true
		}
	}

	return false
}
