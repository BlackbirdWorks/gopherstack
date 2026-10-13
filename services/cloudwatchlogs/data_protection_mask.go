package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
)

type unmaskContextKey struct{}

// withUnmask marks ctx as a request that set the Unmask flag.
func withUnmask(ctx context.Context, unmask bool) context.Context {
	if !unmask {
		return ctx
	}

	return context.WithValue(ctx, unmaskContextKey{}, true)
}

func unmaskRequested(ctx context.Context) bool {
	v, _ := ctx.Value(unmaskContextKey{}).(bool)

	return v
}

// maskRule masks every match of re accepted by valid (nil accepts all).
type maskRule struct {
	re    *regexp.Regexp
	valid func(string) bool
	// group, when non-zero, masks only that submatch (keyword context stays visible).
	group int
}

// maskPolicy is one parsed data protection policy; it masks events ingested at or after since.
type maskPolicy struct {
	rules []maskRule
	since int64
}

// dataMasker applies the data protection policies governing one log group.
type dataMasker struct {
	policies []maskPolicy
}

type protectionPolicyDoc struct {
	Configuration struct {
		CustomDataIdentifier []struct {
			Name  string `json:"Name"`
			Regex string `json:"Regex"`
		} `json:"CustomDataIdentifier"`
	} `json:"Configuration"`
	Statement []struct {
		Operation struct {
			Deidentify *struct{} `json:"Deidentify"`
		} `json:"Operation"`
		DataIdentifier []string `json:"DataIdentifier"`
	} `json:"Statement"`
}

const (
	managedIdentifierMarker = "data-identifier/"
	maxDigit                = 9
)

// managedMaskRules is the subset of AWS managed data identifiers this emulator can detect.
func managedMaskRules() map[string]maskRule {
	rules := map[string]maskRule{
		"EmailAddress": {re: regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)},
		"IpAddress": {re: regexp.MustCompile(
			`\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b`)},
		"CreditCardNumber": {re: regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`), valid: luhnValid},
		"CreditCardExpiration": {
			re: regexp.MustCompile(`(?i)\b(?:exp(?:iry|iration)?(?:\s*date)?|valid\s*thru)\W{0,3}` +
				`((?:0[1-9]|1[0-2])\s*[/-]\s*(?:\d{4}|\d{2}))\b`),
			group: 1,
		},
		"CreditCardSecurityCode": {
			re:    regexp.MustCompile(`(?i)\b(?:cvv2?|cvc2?|csc|security\s*code)\W{0,3}(\d{3,4})\b`),
			group: 1,
		},
		"AwsSecretKey": {
			re: regexp.MustCompile(
				`(?i)(?:aws_?secret_?(?:access_?)?key|secret_?access_?key)["']?\s*[:=]\s*["']?([A-Za-z0-9/+=]{40})\b`),
			group: 1,
		},
		"OpenSshPrivateKey": privateKeyRule("OPENSSH PRIVATE KEY"),
		"PkcsPrivateKey":    privateKeyRule(`(?:RSA |EC |ENCRYPTED )?PRIVATE KEY`),
		"PgpPrivateKey":     privateKeyRule("PGP PRIVATE KEY BLOCK"),
		"PuttyPrivateKey": {re: regexp.MustCompile(
			`(?s)PuTTY-User-Key-File-\d+:.*?Private-Lines:\s*\d+\s+(?:[A-Za-z0-9+/=]+\s*)+`)},
		"Ssn-US": {re: regexp.MustCompile(`\b(?:00[1-9]|0[1-9]\d|[1-5]\d\d|6[0-57-9]\d|66[0-5]|6[67]\d|[78][0-8]\d)-` +
			`(?:0[1-9]|[1-9]\d)-(?:000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d{3})\b`)},
	}

	maps.Copy(rules, formatManagedMaskRules())

	rules["OpenSSHPrivateKey"] = rules["OpenSshPrivateKey"]

	return rules
}

func privateKeyRule(label string) maskRule {
	return maskRule{re: regexp.MustCompile(`(?s)-----BEGIN ` + label + `-----.*?-----END ` + label + `-----`)}
}

func luhnValid(s string) bool {
	sum, double, digits := 0, false, 0

	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c < '0' || c > '9' {
			continue
		}

		d := int(c - '0')
		if double {
			if d *= 2; d > maxDigit {
				d -= maxDigit
			}
		}

		sum += d
		double = !double
		digits++
	}

	return digits >= 13 && sum%10 == 0
}

// parseMaskPolicy compiles the masking (Deidentify) statements of doc; unsupported managed identifiers are skipped.
func parseMaskPolicy(doc string, since int64) (maskPolicy, error) {
	var parsed protectionPolicyDoc
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return maskPolicy{}, fmt.Errorf("%w: policyDocument must be valid JSON: %w", ErrValidation, err)
	}

	custom := make(map[string]*regexp.Regexp, len(parsed.Configuration.CustomDataIdentifier))

	for _, c := range parsed.Configuration.CustomDataIdentifier {
		re, err := regexp.Compile(c.Regex)
		if err != nil {
			return maskPolicy{}, fmt.Errorf(
				"%w: invalid custom data identifier regex %q: %w",
				ErrValidation,
				c.Name,
				err,
			)
		}

		custom[c.Name] = re
	}

	managed := managedMaskRules()
	pol := maskPolicy{since: since}

	for _, st := range parsed.Statement {
		if st.Operation.Deidentify == nil {
			continue
		}

		for _, id := range st.DataIdentifier {
			if re, ok := custom[id]; ok {
				pol.rules = append(pol.rules, maskRule{re: re})

				continue
			}

			if _, name, found := strings.Cut(id, managedIdentifierMarker); found {
				if rule, ok := managed[name]; ok {
					pol.rules = append(pol.rules, rule)
				}
			}
		}
	}

	return pol, nil
}

// maskerForGroupLocked builds the masker for a group from its own policy and ALL-scope account policies.
func (b *InMemoryBackend) maskerForGroupLocked(groupName, groupARN string) *dataMasker {
	m := &dataMasker{}

	for _, id := range []string{groupName, groupARN} {
		if e, ok := b.dataProtectionPolicies.Get(id); ok && id != "" {
			if pol, err := parseMaskPolicy(e.PolicyDocument, e.LastUpdatedTime); err == nil && len(pol.rules) > 0 {
				m.policies = append(m.policies, pol)
			}
		}
	}

	for _, ap := range b.accountPolicies.All() {
		if ap.PolicyType != policyTypeDataProtection || ap.Scope != accountPolicyScopeAll {
			continue
		}

		if pol, err := parseMaskPolicy(ap.PolicyDocument, ap.LastUpdatedTime); err == nil && len(pol.rules) > 0 {
			m.policies = append(m.policies, pol)
		}
	}

	if len(m.policies) == 0 {
		return nil
	}

	return m
}

// mask replaces policy-matched sensitive data in msg with asterisks; events older than the policy are untouched.
func (m *dataMasker) mask(msg string, ingestionTime int64) string {
	if m == nil {
		return msg
	}

	for _, pol := range m.policies {
		if ingestionTime < pol.since {
			continue
		}

		for _, r := range pol.rules {
			msg = r.apply(msg)
		}
	}

	return msg
}

func (r maskRule) apply(msg string) string {
	var out strings.Builder

	last := 0

	for _, loc := range r.re.FindAllStringSubmatchIndex(msg, -1) {
		start, end := loc[2*r.group], loc[2*r.group+1]
		if start < 0 || (r.valid != nil && !r.valid(msg[start:end])) {
			continue
		}

		out.WriteString(msg[last:start])
		out.WriteString(strings.Repeat("*", len([]rune(msg[start:end]))))

		last = end
	}

	out.WriteString(msg[last:])

	return out.String()
}

// validateDataProtectionDocument rejects a policy that is not JSON or has an uncompilable custom regex.
func validateDataProtectionDocument(doc string) error {
	if strings.TrimSpace(doc) == "" {
		return nil
	}

	_, err := parseMaskPolicy(doc, 0)

	return err
}

// unmaskedMasker returns the group's masker, or nil when the request asked for unmasked data. Caller holds b.mu.
func (b *InMemoryBackend) unmaskedMasker(ctx context.Context, region, groupName string) *dataMasker {
	if unmaskRequested(ctx) {
		return nil
	}

	g, ok := b.groupGet(region, groupName)
	if !ok {
		return nil
	}

	return b.maskerForGroupLocked(groupName, g.Arn)
}
