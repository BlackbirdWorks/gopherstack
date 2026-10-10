package rekognition

import "regexp"

var codePrefixRe = regexp.MustCompile(`^[A-Za-z]+Exception: `)

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}
