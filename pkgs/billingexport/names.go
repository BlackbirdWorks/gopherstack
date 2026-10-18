package billingexport

import (
	"strings"
	"unicode"
)

var productCodes = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	"Amazon Elastic Compute Cloud - Compute": "AmazonEC2",
	"Amazon Simple Storage Service":          "AmazonS3",
	"Amazon Relational Database Service":     "AmazonRDS",
	"AWS Lambda":                             "AWSLambda",
	"Amazon CloudFront":                      "AmazonCloudFront",
	"Amazon DynamoDB":                        "AmazonDynamoDB",
	"Amazon Elastic Load Balancing":          "AWSELB",
	"AWS Key Management Service":             "awskms",
	"Amazon Route 53":                        "AmazonRoute53",
	"AWS CloudTrail":                         "AWSCloudTrail",
	"Amazon SNS":                             "AmazonSNS",
	"Amazon SQS":                             "AWSQueueService",
}

// ProductCode maps a Cost Explorer service name to its billing product code.
func ProductCode(service string) string {
	if c, ok := productCodes[service]; ok {
		return c
	}

	var b strings.Builder

	for _, r := range service {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}

	return b.String()
}

// SnakeCase converts "lineItem/UsageAccountId" style names to "line_item_usage_account_id".
func SnakeCase(s string) string {
	var b strings.Builder

	prevLower := false

	for _, r := range s {
		switch {
		case r == '/' || r == ' ' || r == ':':
			b.WriteByte('_')

			prevLower = false
		case unicode.IsUpper(r):
			if prevLower {
				b.WriteByte('_')
			}

			b.WriteRune(unicode.ToLower(r))

			prevLower = false
		default:
			b.WriteRune(r)

			prevLower = unicode.IsLower(r) || unicode.IsDigit(r)
		}
	}

	return b.String()
}
