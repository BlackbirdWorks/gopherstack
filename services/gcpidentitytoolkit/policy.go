package gcpidentitytoolkit

import (
	"slices"
	"strings"
	"time"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

const (
	quotaWindow     = time.Hour
	maxQuotaClients = 10000
	enforceState    = "ENFORCE"
)

type smsRegionCfg = idtkv2.GoogleCloudIdentitytoolkitAdminV2SmsRegionConfig

func (sc *scope) smsRegions() *smsRegionCfg {
	if sc.tenant == rootTenantKey {
		return sc.proj.Config.SmsRegionConfig
	}

	return sc.proj.Tenants[sc.tenant].SmsRegionConfig
}

func regionAllowed(cfg *smsRegionCfg, region string) bool {
	switch {
	case cfg == nil:
		return true
	case cfg.AllowlistOnly != nil:
		return slices.ContainsFunc(
			cfg.AllowlistOnly.AllowedRegions,
			func(r string) bool { return strings.EqualFold(r, region) },
		)
	case cfg.AllowByDefault != nil:
		return !slices.ContainsFunc(
			cfg.AllowByDefault.DisallowedRegions,
			func(r string) bool { return strings.EqualFold(r, region) },
		)
	}

	return true
}

// smsAllowedLocked applies the project or tenant SMS region policy to a destination number.
func (b *InMemoryBackend) smsAllowedLocked(sc *scope, phone string) error {
	if regionAllowed(sc.smsRegions(), phoneRegion(phone)) {
		return nil
	}

	return idtk("OPERATION_NOT_ALLOWED", "SMS unable to be sent until this region enabled by the app administrator.")
}

func (sc *scope) recaptchaCfg() *idtkv2.GoogleCloudIdentitytoolkitAdminV2RecaptchaConfig {
	if sc.tenant == rootTenantKey {
		return sc.proj.Config.RecaptchaConfig
	}

	return sc.proj.Tenants[sc.tenant].RecaptchaConfig
}

// recaptchaGateLocked rejects an enforced flow that carries no reCAPTCHA token. The assessment itself would be
// made by reCAPTCHA Enterprise, which the emulator cannot reach, so a present token is accepted.
func recaptchaGate(sc *scope, phone bool, r *authRequest) error {
	rc := sc.recaptchaCfg()
	if rc == nil {
		return nil
	}

	state := rc.EmailPasswordEnforcementState
	if phone {
		state = rc.PhoneEnforcementState
	}

	if state == enforceState && r.CaptchaResponse == "" && r.RecaptchaToken == "" {
		return idtk("MISSING_RECAPTCHA_TOKEN")
	}

	return nil
}

// signUpAllowedLocked meters sign-ups per client address against the project's signUpQuotaConfig.
func (b *InMemoryBackend) signUpAllowedLocked(sc *scope, ip string) error {
	q := sc.proj.Config.Quota
	if q == nil || q.SignUpQuotaConfig == nil || q.SignUpQuotaConfig.Quota <= 0 {
		return nil
	}

	cfg, now := q.SignUpQuotaConfig, b.now()

	if !quotaActive(cfg, now) {
		return nil
	}

	if sc.proj.signUps == nil || len(sc.proj.signUps) > maxQuotaClients {
		sc.proj.signUps = map[string][]int64{}
	}

	cutoff := now.Add(-quotaWindow).UnixMilli()
	recent := slices.DeleteFunc(slices.Clone(sc.proj.signUps[ip]), func(t int64) bool { return t < cutoff })

	if int64(len(recent)) >= cfg.Quota {
		sc.proj.signUps[ip] = recent

		return idtk("TOO_MANY_ATTEMPTS_TRY_LATER")
	}

	sc.proj.signUps[ip] = append(recent, now.UnixMilli())

	return nil
}

func quotaActive(cfg *idtkv2.GoogleCloudIdentitytoolkitAdminV2TemporaryQuota, now time.Time) bool {
	if cfg.StartTime != "" {
		start, err := time.Parse(time.RFC3339, cfg.StartTime)
		if err != nil || now.Before(start) {
			return false
		}

		if d, derr := time.ParseDuration(cfg.QuotaDuration); derr == nil && d > 0 && !now.Before(start.Add(d)) {
			return false
		}
	}

	return true
}
