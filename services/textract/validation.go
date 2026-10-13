package textract

import (
	"fmt"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxJobTagLen      = 64
	maxClientTokenLen = 64
	maxAdapterNameLen = 128
)

var (
	jobTagRe      = regexp.MustCompile(`^[a-zA-Z0-9_.\-:]+$`)
	clientTokenRe = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
	snsTopicArnRe = regexp.MustCompile(`^arn:[a-z\d-]+:sns:[a-z\d-]+:\d{12}:.+$`)
	iamRoleArnRe  = regexp.MustCompile(`^arn:[a-z\d-]+:iam::\d{12}:.+$`)
	adapterNameRe = regexp.MustCompile(`^[a-zA-Z0-9\-_]+$`)
	codePrefixRe  = regexp.MustCompile(`^[A-Za-z]+(Exception|Error): `)
)

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}

func validateDocumentPresent(bucket, name string, bytes []byte) error {
	if len(bytes) == 0 && (bucket == "" || name == "") {
		return fmt.Errorf("%w: Document must specify Bytes or S3Object (Bucket and Name)", errInvalidRequest)
	}

	return nil
}

func validateStartOptions(jobTag, token string, nc *NotificationChannel) error {
	if jobTag != "" && (len(jobTag) > maxJobTagLen || !jobTagRe.MatchString(jobTag)) {
		return fmt.Errorf("%w: JobTag must match [a-zA-Z0-9_.\\-:]+ and be at most %d characters",
			errInvalidRequest, maxJobTagLen)
	}

	if token != "" && (len(token) > maxClientTokenLen || !clientTokenRe.MatchString(token)) {
		return fmt.Errorf("%w: ClientRequestToken must match ^[a-zA-Z0-9-_]+$ and be at most %d characters",
			errInvalidRequest, maxClientTokenLen)
	}

	if nc == nil {
		return nil
	}

	if !snsTopicArnRe.MatchString(nc.SNSTopicArn) {
		return fmt.Errorf("%w: NotificationChannel.SNSTopicArn is not a valid SNS topic ARN", errInvalidRequest)
	}

	if !iamRoleArnRe.MatchString(nc.RoleArn) {
		return fmt.Errorf("%w: NotificationChannel.RoleArn is not a valid IAM role ARN", errInvalidRequest)
	}

	return nil
}

func validateNextToken(token string) error {
	if err := page.ValidateToken(token); err != nil {
		return fmt.Errorf("%w: NextToken is invalid", errInvalidRequest)
	}

	return nil
}

func validateAdapterName(name string) error {
	if len(name) > maxAdapterNameLen || !adapterNameRe.MatchString(name) {
		return fmt.Errorf("%w: AdapterName must match [a-zA-Z0-9-_]+ and be at most %d characters",
			errInvalidRequest, maxAdapterNameLen)
	}

	return nil
}
