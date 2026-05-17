package sts

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// approvedRootTaskPolicies returns the AWS-approved task policy ARNs for AssumeRoot (Gap #7).
func approvedRootTaskPolicies() []string {
	return []string{
		"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		"arn:aws:iam::aws:policy/root-task/IAMCreateRootUserPassword",
		"arn:aws:iam::aws:policy/root-task/IAMDeleteRootUserCredentials",
		"arn:aws:iam::aws:policy/root-task/S3UnlockBucketPolicy",
		"arn:aws:iam::aws:policy/root-task/SQSUnlockQueuePolicy",
	}
}

// sourceIdentityRe matches the AWS-allowed character set for SourceIdentity.
// Allowed: word chars (a-z A-Z 0-9 _), +, =, ,, ., @, -, length 2-64.
var sourceIdentityRe = regexp.MustCompile(`^[\w+=,.@\-]{2,64}$`)

// mfaSerialNumberARNRe matches virtual MFA device ARNs: arn:aws:iam::ACCOUNT:mfa/NAME.
var mfaSerialNumberARNRe = regexp.MustCompile(`^arn:aws:iam::\d{12}:mfa/.+$`)

// mfaHardwareSerialRe matches hardware token serial numbers (GAHT prefix + 8 alphanumeric chars).
var mfaHardwareSerialRe = regexp.MustCompile(`^GAHT\w{8}$`)

// mfaTokenCodeRe matches exactly 6 decimal digits.
var mfaTokenCodeRe = regexp.MustCompile(`^\d{6}$`)

// wellFormedAccessKeyRe matches any well-formed AWS access key regardless of type prefix.
// Format: known 4-char prefix + 16 uppercase alphanumeric characters.
var wellFormedAccessKeyRe = regexp.MustCompile(`^(AKIA|ASIA|AIDA|AROA|AGPA|ANPA|ANVA|APKA|ASCA)[A-Z0-9]{16}$`)

// tagKeyRe matches the allowed character set for session tag keys per AWS specification.
// Allowed: Unicode letters, Unicode numbers, spaces, and the special chars _.:/=+-@.
var tagKeyRe = regexp.MustCompile(`^[\pL\pN\pZ_.:/=+\-@]+$`)

// validateSourceIdentity checks that SourceIdentity matches the AWS character set and length
// constraints: `^[\w+=,.@-]{2,64}$`.
func validateSourceIdentity(identity string) error {
	if identity == "" {
		return nil
	}

	if !sourceIdentityRe.MatchString(identity) {
		return fmt.Errorf("%w: must be 2-64 characters matching [\\w+=,.@-]", ErrInvalidSourceIdentity)
	}

	return nil
}

// validateMFASerialNumber checks that a SerialNumber is either a virtual MFA device ARN
// (arn:aws:iam::ACCOUNT:mfa/NAME) or a hardware token serial (GAHT + 8 chars).
func validateMFASerialNumber(serial string) error {
	if serial == "" {
		return nil
	}

	if mfaSerialNumberARNRe.MatchString(serial) || mfaHardwareSerialRe.MatchString(serial) {
		return nil
	}

	return fmt.Errorf("%w: must be arn:aws:iam::ACCOUNT:mfa/NAME or GAHTXXXXXXXX", ErrInvalidMFASerialNumber)
}

// validateMFATokenCode checks that a TokenCode is exactly 6 decimal digits.
func validateMFATokenCode(code string) error {
	if code == "" {
		return nil
	}

	if !mfaTokenCodeRe.MatchString(code) {
		return fmt.Errorf("%w: must be exactly 6 digits", ErrInvalidMFATokenCode)
	}

	return nil
}

// samlProviderARNRe matches IAM SAML provider ARNs.
// Format: arn:<partition>:iam::<account>:saml-provider/<name>.
var samlProviderARNRe = regexp.MustCompile(`^arn:[a-z0-9\-]+:iam::\d{12}:saml-provider/.+$`)

// validateSAMLProviderArn checks that a PrincipalArn is a valid IAM SAML provider ARN.
func validateSAMLProviderArn(principalArn string) error {
	if !samlProviderARNRe.MatchString(principalArn) {
		return fmt.Errorf("%w: %q must be arn:<partition>:iam::<account>:saml-provider/<name>",
			ErrInvalidPrincipalArn, principalArn)
	}

	return nil
}

// managedPolicyARNRe matches AWS managed (account="aws") and customer-managed (account=12 digits) policy ARNs.
// Format: arn:<partition>:iam::<account-or-aws>:policy/<path/name>.
var managedPolicyARNRe = regexp.MustCompile(`^arn:[a-z0-9\-]+:iam::(?:\d{12}|aws)?:policy/.+$`)

// validatePolicyArns validates a list of managed policy ARNs:
// - count must not exceed MaxPolicyArnsCount
// - each ARN must match arn:<partition>:iam::<account>:policy/<name>.
func validatePolicyArns(arns []string) error {
	if len(arns) > MaxPolicyArnsCount {
		return fmt.Errorf("%w: got %d", ErrTooManyPolicyArns, len(arns))
	}

	for _, a := range arns {
		if a == "" || !managedPolicyARNRe.MatchString(a) {
			return fmt.Errorf("%w: %q is not a valid managed policy ARN", ErrInvalidPolicyArn, a)
		}
	}

	return nil
}

// validateProvidedContexts checks each ProvidedContext entry for length limits.
func validateProvidedContexts(contexts []ProvidedContext) error {
	if len(contexts) > MaxProvidedContextsCount {
		return fmt.Errorf(
			"%w: too many provided contexts, maximum is %d",
			ErrInvalidProvidedContext,
			MaxProvidedContextsCount,
		)
	}

	for i, ctx := range contexts {
		if len(ctx.ContextAssertion) > MaxProvidedContextLen {
			return fmt.Errorf(
				"%w: ProvidedContexts[%d].ContextAssertion exceeds %d characters",
				ErrInvalidProvidedContext, i, MaxProvidedContextLen,
			)
		}

		if len(ctx.ProviderArn) > MaxProvidedContextLen {
			return fmt.Errorf(
				"%w: ProvidedContexts[%d].ProviderArn exceeds %d characters",
				ErrInvalidProvidedContext, i, MaxProvidedContextLen,
			)
		}
	}

	return nil
}

// validateInlinePolicy checks that an inline session policy is valid JSON and has a Statement field.
func validateInlinePolicy(policy string) error {
	if policy == "" {
		return nil
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return fmt.Errorf("%w: %w", ErrMalformedPolicyDocument, err)
	}

	if _, hasStatement := doc["Statement"]; !hasStatement {
		return fmt.Errorf("%w: policy document must contain a Statement field", ErrMalformedPolicyDocument)
	}

	return nil
}

// validateTagConstraints checks each session tag key/value against AWS rules:
// - key: 1-128 chars, tagKeyRe charset, no "aws:" prefix (case-insensitive)
// - value: 0-256 chars
// - case-insensitive key uniqueness.
func validateTagConstraints(tags []Tag) error {
	seen := make(map[string]struct{}, len(tags))

	for i, tag := range tags {
		if len(tag.Key) < MinTagKeyLen || len(tag.Key) > MaxTagKeyLen {
			return fmt.Errorf("%w: tag[%d] key length %d is outside allowed range 1-%d",
				ErrInvalidTagKey, i, len(tag.Key), MaxTagKeyLen)
		}

		if !tagKeyRe.MatchString(tag.Key) {
			return fmt.Errorf("%w: tag[%d] key %q contains invalid characters", ErrInvalidTagKey, i, tag.Key)
		}

		if strings.HasPrefix(strings.ToLower(tag.Key), "aws:") {
			return fmt.Errorf("%w: tag[%d] key %q must not use reserved prefix \"aws:\"",
				ErrInvalidTagKey, i, tag.Key)
		}

		if len(tag.Value) > MaxTagValueLen {
			return fmt.Errorf("%w: tag[%d] value length %d exceeds maximum %d",
				ErrInvalidTagValue, i, len(tag.Value), MaxTagValueLen)
		}

		lower := strings.ToLower(tag.Key)
		if _, dup := seen[lower]; dup {
			return fmt.Errorf("%w: duplicate tag key %q (case-insensitive)", ErrInvalidTagKey, tag.Key)
		}

		seen[lower] = struct{}{}
	}

	return nil
}

// validateApprovedRootTaskPolicy checks that TaskPolicyArn is in the AWS-approved set.
func validateApprovedRootTaskPolicy(taskPolicyArn string) error {
	if slices.Contains(approvedRootTaskPolicies(), taskPolicyArn) {
		return nil
	}

	return fmt.Errorf("%w: TaskPolicyArn %q is not in the approved set", ErrValidation, taskPolicyArn)
}

// calculatePackedPolicySizeWithArns computes PackedPolicySize as the percentage of the 2048-byte
// session-policy budget consumed by the combined inline policy and managed policy ARNs.
// Returns 0 when all inputs are empty, capped at 100.
func calculatePackedPolicySizeWithArns(policy string, policyArns []string) int32 {
	total := len(policy)
	for _, a := range policyArns {
		total += len(a)
	}

	if total == 0 {
		return 0
	}

	// Round up: (total * 100 + budget - 1) / budget.
	pct := (total*int(maxPackedPolicySizePercent) + maxSessionPolicyBytes - 1) / maxSessionPolicyBytes
	//nolint:gosec // pct bounded by computation above
	return min(max(int32(pct), 1), maxPackedPolicySizePercent)
}

// checkPackedPolicyBudget returns ErrPackedPolicyTooLarge when the combined policy size
// exceeds the 2048-byte budget.
func checkPackedPolicyBudget(policy string, policyArns []string) error {
	total := len(policy)
	for _, a := range policyArns {
		total += len(a)
	}

	if total > maxSessionPolicyBytes {
		return fmt.Errorf("%w: combined session policy is %d bytes, exceeds %d-byte limit",
			ErrPackedPolicyTooLarge, total, maxSessionPolicyBytes)
	}

	return nil
}

// validateJWTNotExpired checks the "exp" claim in a JWT payload and returns ErrExpiredToken
// when the token is expired. Returns nil when the claim is absent (mock permissive).
func validateJWTNotExpired(token string) error {
	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return nil
	}

	exp, ok := claims["exp"]
	if !ok {
		return nil
	}

	var expTime time.Time

	switch v := exp.(type) {
	case float64:
		expTime = time.Unix(int64(v), 0).UTC()
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return err
		}

		expTime = time.Unix(n, 0).UTC()
	default:
		return nil
	}

	if time.Now().UTC().After(expTime) {
		return fmt.Errorf("%w: JWT exp claim is %s", ErrExpiredToken, expTime.Format(time.RFC3339))
	}

	return nil
}

// isWellFormedAccessKey reports whether the given key matches the well-formed AWS access key format:
// a known 4-char prefix followed by exactly 16 uppercase alphanumeric characters.
func isWellFormedAccessKey(key string) bool {
	return wellFormedAccessKeyRe.MatchString(key)
}
