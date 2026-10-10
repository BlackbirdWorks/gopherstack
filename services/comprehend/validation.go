package comprehend

import (
	"fmt"
	"regexp"
)

const (
	listMaxResultsMin = 1
	listMaxResultsMax = 500
)

var (
	// Brackets are admitted so the "[fail]" job-name failure marker stays usable.
	jobNameRe    = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-%@\[\]]{1,256}$`)
	codePrefixRe = regexp.MustCompile(`^[A-Za-z]+Exception: `)
)

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}

func validateJobName(name string) error {
	if !jobNameRe.MatchString(name) {
		return fmt.Errorf("%w: JobName must match ^([\\p{L}\\p{Z}\\p{N}_.:/=+\\-%%@]*)$ and be 1-256 characters",
			ErrValidation)
	}

	return nil
}
