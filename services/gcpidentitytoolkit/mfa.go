package gcpidentitytoolkit

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // RFC 6238 TOTP default HMAC-SHA1, the algorithm the API reports as hashingAlgorithm
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

const (
	mfaDisabled      = "DISABLED"
	mfaMandatory     = "MANDATORY"
	mfaPhoneSMS      = "PHONE_SMS"
	mfaEnabledState  = "ENABLED"
	kindTotp         = "totp"
	kindPhone        = "phone"
	reqMfaTotp       = "MFA_TOTP_SESSION"
	reqMfaPhone      = "MFA_PHONE_SESSION"
	reqMfaPending    = "MFA_PENDING"
	totpSecretBytes  = 20
	totpPeriod       = 30
	totpDigits       = 6
	totpAdjacent     = 5
	maxEnrollments   = 5
	mfaSessionTTL    = 10 * time.Minute
	mfaPendingTTL    = 10 * time.Minute
	enrollIDBytes    = 14
	totpMod          = 1_000_000
	truncMask        = 0x7fffffff
	truncOffsetMask  = 0x0f
	maskedPhoneKeep  = 4
	hotpTruncateSize = 4
)

func mfaConfigured(sc *scope) bool {
	return sc.mfa != nil && sc.mfa.State != "" && sc.mfa.State != mfaDisabled
}

func (sc *scope) totpConfig() *idtkv2.GoogleCloudIdentitytoolkitAdminV2TotpMfaProviderConfig {
	if sc.mfa == nil {
		return nil
	}

	for _, pc := range sc.mfa.ProviderConfigs {
		if pc.State == mfaEnabledState && pc.TotpProviderConfig != nil {
			return pc.TotpProviderConfig
		}
	}

	return nil
}

func (sc *scope) smsEnabled() bool {
	return mfaConfigured(sc) && slices.Contains(sc.mfa.EnabledProviders, mfaPhoneSMS)
}

func hotp(secret []byte, counter uint64) string {
	mac := hmac.New(sha1.New, secret)

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], counter)
	mac.Write(buf[:])

	sum := mac.Sum(nil)
	off := sum[len(sum)-1] & truncOffsetMask
	v := binary.BigEndian.Uint32(sum[off:off+hotpTruncateSize]) & truncMask

	return fmt.Sprintf("%0*d", totpDigits, v%totpMod)
}

// totpValid accepts the code of the current period and of adjacent periods either side.
func totpValid(secret []byte, code string, now time.Time, adjacent int64) bool {
	step := now.Unix() / totpPeriod

	for d := -adjacent; d <= adjacent; d++ {
		c := step + d
		if c < 0 {
			continue
		}

		if subtle.ConstantTimeCompare([]byte(hotp(secret, uint64(c))), []byte(code)) == 1 {
			return true
		}
	}

	return false
}

func (sc *scope) adjacentIntervals() int64 {
	if cfg := sc.totpConfig(); cfg != nil && cfg.AdjacentIntervals > 0 {
		return cfg.AdjacentIntervals
	}

	return totpAdjacent
}

func maskPhone(p string) string {
	if len(p) <= maskedPhoneKeep+1 {
		return p
	}

	return "+" + strings.Repeat("*", len(p)-1-maskedPhoneKeep) + p[len(p)-maskedPhoneKeep:]
}

func newEnrollmentID() string { return randomToken(enrollIDBytes) }

type mfaWire struct {
	TotpInfo              *struct{} `json:"totpInfo,omitempty"`
	DisplayName           string    `json:"displayName,omitempty"`
	EnrolledAt            string    `json:"enrolledAt,omitempty"`
	MfaEnrollmentID       string    `json:"mfaEnrollmentId,omitempty"`
	PhoneInfo             string    `json:"phoneInfo,omitempty"`
	UnobfuscatedPhoneInfo string    `json:"unobfuscatedPhoneInfo,omitempty"`
}

func mfaWires(recs []mfaRec, masked bool) []mfaWire {
	out := make([]mfaWire, 0, len(recs))

	for _, m := range recs {
		w := mfaWire{
			DisplayName: m.DisplayName, MfaEnrollmentID: m.ID,
			EnrolledAt: time.UnixMilli(m.EnrolledAt).UTC().Format(time.RFC3339Nano),
		}

		if m.Kind == kindTotp {
			w.TotpInfo = &struct{}{}
		} else {
			w.PhoneInfo = m.Phone
			if masked {
				w.PhoneInfo, w.UnobfuscatedPhoneInfo = maskPhone(m.Phone), m.Phone
			}
		}

		out = append(out, w)
	}

	return out
}

func newTotpSecret() []byte {
	raw := make([]byte, totpSecretBytes)
	_, _ = rand.Read(raw)

	return raw
}

func b32(raw []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
}

func decodeB32(s string) ([]byte, error) {
	return base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(s)
}

// enrollmentsFromWire builds the SMS enrollments an Admin SDK create or update carries; entries without a valid
// phoneInfo are dropped.
func (b *InMemoryBackend) enrollmentsFromWire(ws []mfaWire) []mfaRec {
	var out []mfaRec

	for _, w := range ws {
		if !e164.MatchString(w.PhoneInfo) {
			continue
		}

		id := w.MfaEnrollmentID
		if id == "" {
			id = newEnrollmentID()
		}

		out = append(out, mfaRec{
			ID: id, DisplayName: w.DisplayName, Kind: kindPhone, Phone: w.PhoneInfo, EnrolledAt: b.nowMillis(),
		})
	}

	return out
}
