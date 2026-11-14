package gcpidentitytoolkit

import (
	"strings"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"
)

const (
	defaultMinPassword = 6
	defaultMaxPassword = 4096
	policySchemaV1     = 1
	nonAlnumChars      = "^$*.[]{}()?\"!@#%&/\\,><':;|_~`"
	recaptchaEmail     = "EMAIL_PASSWORD_PROVIDER"
	recaptchaPhone     = "PHONE_PROVIDER"
	stateOff           = "OFF"
)

// PasswordPolicy implements v2 getPasswordPolicy: the enforced policy of the project or tenant.
func (b *InMemoryBackend) PasswordPolicy(
	project, tenant string,
) (*idtkv2.GoogleCloudIdentitytoolkitV2PasswordPolicy, error) {
	b.mu.Lock("PasswordPolicy")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(project, tenant)
	if err != nil {
		return nil, err
	}

	out := &idtkv2.GoogleCloudIdentitytoolkitV2PasswordPolicy{
		EnforcementState:                 stateOff,
		SchemaVersion:                    policySchemaV1,
		AllowedNonAlphanumericCharacters: strings.Split(nonAlnumChars, ""),
		CustomStrengthOptions: &idtkv2.GoogleCloudIdentitytoolkitV2CustomStrengthOptions{
			MinPasswordLength: defaultMinPassword, MaxPasswordLength: defaultMaxPassword,
		},
	}

	pol := sc.policy
	if pol == nil {
		return out, nil
	}

	if pol.PasswordPolicyEnforcementState != "" {
		out.EnforcementState = pol.PasswordPolicyEnforcementState
	}

	out.ForceUpgradeOnSignin = pol.ForceUpgradeOnSignin

	if n := len(pol.PasswordPolicyVersions); n > 0 {
		v := pol.PasswordPolicyVersions[n-1]
		out.SchemaVersion = v.SchemaVersion

		if o := v.CustomStrengthOptions; o != nil {
			out.CustomStrengthOptions = &idtkv2.GoogleCloudIdentitytoolkitV2CustomStrengthOptions{
				ContainsLowercaseCharacter:       o.ContainsLowercaseCharacter,
				ContainsUppercaseCharacter:       o.ContainsUppercaseCharacter,
				ContainsNumericCharacter:         o.ContainsNumericCharacter,
				ContainsNonAlphanumericCharacter: o.ContainsNonAlphanumericCharacter,
				MinPasswordLength:                o.MinPasswordLength,
				MaxPasswordLength:                o.MaxPasswordLength,
			}
		}
	}

	return out, nil
}

// RecaptchaConfig implements v2 getRecaptchaConfig for the project or tenant.
func (b *InMemoryBackend) RecaptchaConfig(
	project, tenant, clientType string,
) (*idtkv2.GoogleCloudIdentitytoolkitV2RecaptchaConfig, error) {
	b.mu.Lock("RecaptchaConfig")
	defer b.mu.Unlock()

	sc, err := b.scopeLocked(project, tenant)
	if err != nil {
		return nil, err
	}

	rc := sc.proj.Config.RecaptchaConfig
	if sc.tenant != rootTenantKey {
		rc = sc.proj.Tenants[sc.tenant].RecaptchaConfig
	}

	out := &idtkv2.GoogleCloudIdentitytoolkitV2RecaptchaConfig{
		RecaptchaEnforcementState: []*idtkv2.GoogleCloudIdentitytoolkitV2RecaptchaEnforcementState{
			{Provider: recaptchaEmail, EnforcementState: stateOff},
			{Provider: recaptchaPhone, EnforcementState: stateOff},
		},
	}
	if rc == nil {
		return out, nil
	}

	out.RecaptchaEnforcementState[0].EnforcementState = enforcement(rc.EmailPasswordEnforcementState)
	out.RecaptchaEnforcementState[1].EnforcementState = enforcement(rc.PhoneEnforcementState)
	out.UseSmsBotScore, out.UseSmsTollFraudProtection = rc.UseSmsBotScore, rc.UseSmsTollFraudProtection

	if out.RecaptchaEnforcementState[0].EnforcementState != stateOff ||
		out.RecaptchaEnforcementState[1].EnforcementState != stateOff {
		out.RecaptchaKey = recaptchaKeyFor(project, rc, clientType)
	}

	return out, nil
}

func enforcement(s string) string {
	switch s {
	case "AUDIT", "ENFORCE":
		return s
	}

	return stateOff
}

func recaptchaKeyFor(
	project string,
	rc *idtkv2.GoogleCloudIdentitytoolkitAdminV2RecaptchaConfig,
	clientType string,
) string {
	want := strings.TrimPrefix(clientType, "CLIENT_TYPE_")

	for _, k := range rc.RecaptchaKeys {
		if k.Key != "" && (want == "" || k.Type == "" || strings.EqualFold(k.Type, want)) {
			return "projects/" + project + "/keys/" + k.Key
		}
	}

	return ""
}
