package polly

import (
	"regexp"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/polly/types"
)

var codePrefixRe = regexp.MustCompile(`^[A-Za-z]+Exception: `)

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}

func isKnownLanguageCode(code string) bool {
	for _, v := range sdktypes.LanguageCode("").Values() {
		if string(v) == code {
			return true
		}
	}

	return false
}
