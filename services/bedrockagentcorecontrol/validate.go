package bedrockagentcorecontrol

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	minTokenLen    = 33
	maxTokenLen    = 256
	maxDescription = 4096
	maxTagKey      = 128
	maxTagValue    = 256
)

var (
	roleARNPattern = regexp.MustCompile(`^arn:aws[a-z-]*:iam::[0-9]{12}:role/.+$`)
	kmsARNPattern  = regexp.MustCompile(`^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/.+$`)
	ecrURIPattern  = regexp.MustCompile(
		`^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?/([^:@]+)(?:[:@].+)?$`,
	)
	lambdaPattern = regexp.MustCompile(`^arn:aws[a-z-]*:lambda:[a-z0-9-]+:[0-9]{12}:function:.+$`)
	secretPattern = regexp.MustCompile(`^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:.+$`)
)

// validateCommon applies the member constraints shared by every operation.
func validateCommon(in args) error {
	if t := in.str(keyClientToken); t != "" && (len(t) < minTokenLen || len(t) > maxTokenLen) {
		return invalid("clientToken must be between %d and %d characters", minTokenLen, maxTokenLen)
	}

	if d := in.str(keyDescription); utf8.RuneCountInString(d) > maxDescription {
		return invalid("description must be at most %d characters", maxDescription)
	}

	for k, v := range in {
		if s, ok := v.(string); ok && s != "" {
			if err := validateARNMember(k, s); err != nil {
				return err
			}
		}
	}

	if in.has(keyTags) {
		return validateTags(in.obj(keyTags))
	}

	return nil
}

// validateARNMember checks the shape of role and KMS key ARN members by name.
func validateARNMember(key, value string) error {
	switch {
	case key == "roleArn" || strings.HasSuffix(key, "RoleArn"):
		if !roleARNPattern.MatchString(value) {
			return invalid("%s is not a valid IAM role ARN", key)
		}
	case key == "kmsKeyArn" || key == "encryptionKeyArn":
		if !kmsARNPattern.MatchString(value) {
			return invalid("%s is not a valid KMS key ARN", key)
		}
	}

	return nil
}

func validateTags(m map[string]any) error {
	if len(m) > maxTags {
		return invalid("a resource can have at most %d tags", maxTags)
	}

	for k, v := range m {
		val, ok := v.(string)
		if !ok {
			return invalid("tag value for %q must be a string", k)
		}

		if k == "" || utf8.RuneCountInString(k) > maxTagKey {
			return invalid("tag keys must be between 1 and %d characters", maxTagKey)
		}

		if utf8.RuneCountInString(val) > maxTagValue {
			return invalid("tag values must be at most %d characters", maxTagValue)
		}

		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("tag keys starting with aws: are reserved")
		}
	}

	return nil
}

func validateName(k *kindSpec, name string) error {
	if k.namePattern == nil || name == "" {
		return nil
	}

	if !k.namePattern.MatchString(name) {
		return invalid("%s %q does not match the pattern %s", k.NameKey, name, k.namePattern.String())
	}

	return nil
}
