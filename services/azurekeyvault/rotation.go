package azurekeyvault

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	actionRotate   = "Rotate"
	actionNotify   = "Notify"
	maxDurationNum = 1_000_000
	isoGroups      = 7
	daysPerWeek    = 7
)

var isoDuration = regexp.MustCompile(
	`^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$`,
)

// shiftISO moves t by sign times the ISO-8601 duration s (years, months, weeks, days, time).
func shiftISO(t time.Time, s string, sign int) (time.Time, error) {
	m := isoDuration.FindStringSubmatch(s)
	if m == nil || strings.Join(m[1:], "") == "" {
		return t, errBadParameter("Invalid ISO-8601 duration %q.", s)
	}

	var n [isoGroups]int

	for i := range n {
		if m[i+1] == "" {
			continue
		}

		v, err := strconv.Atoi(m[i+1])
		if err != nil || v > maxDurationNum {
			return t, errBadParameter("ISO-8601 duration %q is out of range.", s)
		}

		n[i] = v * sign
	}

	t = t.AddDate(n[0], n[1], n[2]*daysPerWeek+n[3])

	return t.Add(time.Duration(n[4])*time.Hour + time.Duration(n[5])*time.Minute + time.Duration(n[6])*time.Second), nil
}

// validate checks action types and every duration in the policy.
func (r *RotationPolicy) validate() error {
	for _, la := range r.LifetimeActions {
		if !strings.EqualFold(la.Action.Type, actionRotate) && !strings.EqualFold(la.Action.Type, actionNotify) {
			return errBadParameter("Unsupported lifetime action type %q.", la.Action.Type)
		}

		for _, d := range []string{la.Trigger.TimeAfterCreate, la.Trigger.TimeBeforeExpiry, r.Attributes.ExpiryTime} {
			if d == "" {
				continue
			}

			if _, err := shiftISO(time.Time{}, d, 1); err != nil {
				return err
			}
		}
	}

	return nil
}

// expiryAt returns the epoch expiry for a version created at now, or nil.
func (r *RotationPolicy) expiryAt(now time.Time) *int64 {
	if r == nil || r.Attributes.ExpiryTime == "" {
		return nil
	}

	t, err := shiftISO(now, r.Attributes.ExpiryTime, 1)
	if err != nil {
		return nil
	}

	exp := t.Unix()

	return &exp
}

// rotateKey appends a fresh version to e, expiring it per the rotation policy.
func (v *vault) rotateKey(e *Entity[*KeyVersion], now int64) (*KeyVersion, error) {
	cur := e.latest()
	attrs := &Attributes{Enabled: cur.Attrs.Enabled, Exp: e.Rotation.expiryAt(time.Unix(now, 0))}

	next, err := v.newKeyVersion(&CreateKeyRequest{
		Kty: cur.Kty, Crv: cur.Crv, KeySize: cur.KeySize, KeyOps: cur.KeyOps, Tags: cur.Tags,
		Attributes: attrs,
	}, now)
	if err != nil {
		return nil, err
	}

	e.Versions = append(e.Versions, next)

	return next, nil
}

// firesAt returns the instants at which the trigger fires for cur.
func (t RotationTrigger) firesAt(cur *KeyVersion, created time.Time) []time.Time {
	var out []time.Time

	if t.TimeAfterCreate != "" {
		if at, err := shiftISO(created, t.TimeAfterCreate, 1); err == nil {
			out = append(out, at)
		}
	}

	if t.TimeBeforeExpiry != "" && cur.Attrs.Exp != nil {
		if at, err := shiftISO(time.Unix(*cur.Attrs.Exp, 0), t.TimeBeforeExpiry, -1); err == nil {
			out = append(out, at)
		}
	}

	return out
}

// rotationDue reports whether a Rotate action has fired for the latest version.
// A trigger at or before the version's creation is ignored so a fresh version
// can never re-trigger itself.
func rotationDue(e *Entity[*KeyVersion], now time.Time) bool {
	cur := e.latest()
	if cur == nil || cur.Managed || e.Rotation == nil || !cur.Attrs.enabled() {
		return false
	}

	created := time.Unix(cur.Attrs.Created0(now.Unix()), 0)

	for _, la := range e.Rotation.LifetimeActions {
		if !strings.EqualFold(la.Action.Type, actionRotate) {
			continue
		}

		for _, at := range la.Trigger.firesAt(cur, created) {
			if at.After(created) && !now.Before(at) {
				return true
			}
		}
	}

	return false
}

// rotateDueKeys rotates every active key whose policy fired and returns the count.
func (v *vault) rotateDueKeys(now time.Time) int {
	n := 0

	for _, name := range v.Keys.names(false) {
		e := v.Keys[name]
		if !rotationDue(e, now) {
			continue
		}

		if _, err := v.rotateKey(e, now.Unix()); err == nil {
			n++
		}
	}

	return n
}
