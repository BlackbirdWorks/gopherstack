package cleanrooms

import (
	"fmt"
	"regexp"
)

const (
	maxQueryStringLen   = 500000
	maxParamValueLen    = 1000
	maxBucketNameLen    = 63
	minBucketNameLen    = 3
	maxKeyPrefixLen     = 512
	protectedQueryTypeS = "SQL"
)

var analysisTemplateArnRe = regexp.MustCompile(
	`^arn:aws[-a-z]*:cleanrooms:\w{2}-\w{4,9}-\d:\d{12}:membership/[\d\w-]+/analysistemplate/[\d\w-]+$`,
)

func validationErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

func validateProtectedQueryInput(sqlParams, resultConfig map[string]any) error {
	query, _ := sqlParams["queryString"].(string)
	tmpl, _ := sqlParams["analysisTemplateArn"].(string)

	if query == "" && tmpl == "" {
		return validationErr("sqlParameters must contain a queryString or an analysisTemplateArn")
	}

	if len(query) > maxQueryStringLen {
		return validationErr("queryString must not exceed %d characters", maxQueryStringLen)
	}

	if tmpl != "" && (len(tmpl) > 200 || !analysisTemplateArnRe.MatchString(tmpl)) {
		return validationErr("analysisTemplateArn %q is not a valid analysis template ARN", tmpl)
	}

	if params, ok := sqlParams["parameters"].(map[string]any); ok {
		for k, v := range params {
			if s, _ := v.(string); len(s) > maxParamValueLen {
				return validationErr("parameter %q value must not exceed %d characters", k, maxParamValueLen)
			}
		}
	}

	return validateResultConfiguration(resultConfig)
}

func validateResultConfiguration(resultConfig map[string]any) error {
	out, _ := resultConfig["outputConfiguration"].(map[string]any)
	s3, ok := out["s3"].(map[string]any)

	if !ok {
		return nil
	}

	if format, _ := s3["resultFormat"].(string); format != "CSV" && format != "PARQUET" {
		return validationErr("resultFormat %q must be CSV or PARQUET", format)
	}

	bucket, _ := s3["bucket"].(string)
	if len(bucket) < minBucketNameLen || len(bucket) > maxBucketNameLen {
		return validationErr("bucket must be %d-%d characters", minBucketNameLen, maxBucketNameLen)
	}

	if prefix, _ := s3["keyPrefix"].(string); len(prefix) > maxKeyPrefixLen {
		return validationErr("keyPrefix must not exceed %d characters", maxKeyPrefixLen)
	}

	return nil
}

func validateProtectedType(kind string) error {
	if kind != "" && kind != protectedQueryTypeS {
		return validationErr("type %q must be SQL", kind)
	}

	return nil
}

func validateTargetStatus(status string) error {
	if status != statusCancelled {
		return validationErr("targetStatus %q must be CANCELLED", status)
	}

	return nil
}
