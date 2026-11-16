package azurekeyvault

import (
	"strings"
	"time"
)

const (
	actionAutoRenew    = "AutoRenew"
	percentDenominator = 100
	secondsPerDayInt64 = int64(secondsPerDay)
)

// fireTime returns when the trigger fires for a certificate valid from nbf to exp.
func (t LifetimeTrigger) fireTime(nbf, exp time.Time) (time.Time, bool) {
	switch {
	case t.LifetimePercentage > 0:
		secs := exp.Unix() - nbf.Unix()

		return nbf.Add(time.Duration(secs*int64(t.LifetimePercentage)/percentDenominator) * time.Second), true
	case t.DaysBeforeExpiry > 0:
		return time.Unix(exp.Unix()-int64(t.DaysBeforeExpiry)*secondsPerDayInt64, 0), true
	}

	return time.Time{}, false
}

// renewalDue reports whether an AutoRenew action has fired. Triggers at or
// before the version's notBefore are ignored so a fresh version cannot
// re-trigger itself.
func renewalDue(e *Entity[*CertVersion], now time.Time) bool {
	cur := e.latest()
	if cur == nil || e.CertPolicy == nil || e.CertPolicy.Issuer == nil || !cur.Attrs.enabled() ||
		cur.Attrs.NBF == nil || cur.Attrs.Exp == nil {
		return false
	}

	nbf, exp := time.Unix(*cur.Attrs.NBF, 0), time.Unix(*cur.Attrs.Exp, 0)

	for _, la := range e.CertPolicy.LifetimeActions {
		if !strings.EqualFold(la.Action.ActionType, actionAutoRenew) {
			continue
		}

		if at, ok := la.Trigger.fireTime(nbf, exp); ok && at.After(nbf) && !now.Before(at) {
			return true
		}
	}

	return false
}

// reusedKey returns the current private key when the policy asks to keep it.
func (v *vault) reusedKey(name string, pol *CertPolicy) []byte {
	if kp := pol.KeyProps; kp != nil && kp.ReuseKey != nil && *kp.ReuseKey {
		if k := v.Keys[name]; k != nil && k.latest() != nil {
			return k.latest().Priv
		}
	}

	return nil
}

// renewCert renews a certificate: a new self-signed version for the Self
// issuer, otherwise a new pending CSR awaiting the CA's signed chain.
func (v *vault) renewCert(name string, e *Entity[*CertVersion], now time.Time) error {
	pol := e.CertPolicy
	keyDER := v.reusedKey(name, pol)

	if !strings.EqualFold(pol.Issuer.Name, issuerSelf) {
		cur := e.latest()
		req := attrsAndTags{tags: cur.Tags, attrs: &Attributes{Enabled: cur.Attrs.Enabled}}
		_, err := v.startPendingWithKey(name, pol, keyDER, req, now.Unix())

		return err
	}

	certDER, keyDER, err := selfSignWithKey(pol, keyDER, now)
	if err != nil {
		return err
	}

	cur := e.latest()
	req := attrsAndTags{tags: cur.Tags, attrs: &Attributes{Enabled: cur.Attrs.Enabled}}

	_, err = v.addCert(name, certMaterial{cert: certDER, key: keyDER}, pol, req, now.Unix())

	return err
}

// renewDueCerts renews every certificate whose AutoRenew action fired and returns the count.
func (v *vault) renewDueCerts(now time.Time) int {
	n := 0

	for _, name := range v.Certs.names(false) {
		e := v.Certs[name]
		if !renewalDue(e, now) || !v.canIssue(e.CertPolicy.Issuer.Name) {
			continue
		}

		if _, open := v.Pending[name]; open {
			continue
		}

		if err := v.renewCert(name, e, now); err == nil {
			n++
		}
	}

	return n
}
