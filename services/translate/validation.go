package translate

import (
	"fmt"
	"regexp"
	"slices"
)

const (
	listMaxResultsMin = 1
	listMaxResultsMax = 500
)

var (
	resourceNameRe = regexp.MustCompile(`^([A-Za-z0-9-]_?){1,256}$`)
	// Brackets are admitted so the "[fail]" job-name failure marker stays usable.
	jobNameRe    = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-%@\[\]]{1,256}$`)
	s3URIRe      = regexp.MustCompile(`^s3://[a-z0-9][.\-a-z0-9]{1,61}[a-z0-9](/.*)?$`)
	codePrefixRe = regexp.MustCompile(`^[A-Za-z]+Exception: `)
)

func inputContentTypes() []string {
	return []string{
		"text/plain",
		"text/html",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/x-xliff+xml",
	}
}

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}

func validateResourceName(sentinel error, field, name string) error {
	if !resourceNameRe.MatchString(name) {
		return fmt.Errorf("%w: %s must match ^([A-Za-z0-9-]_?)+$ and be 1-256 characters", sentinel, field)
	}

	return nil
}

func validateJobName(name string) error {
	if name != "" && !jobNameRe.MatchString(name) {
		return fmt.Errorf("%w: JobName contains invalid characters or exceeds 256 characters", ErrValidation)
	}

	return nil
}

func validateS3URI(field, uri string) error {
	if !s3URIRe.MatchString(uri) {
		return fmt.Errorf("%w: %s must be a valid s3:// URI", ErrValidation, field)
	}

	return nil
}

func validateInputContentType(contentType string) error {
	if !slices.Contains(inputContentTypes(), contentType) {
		return fmt.Errorf("%w: InputDataConfig.ContentType %q is not supported", ErrValidation, contentType)
	}

	return nil
}

// validateListMaxResults rejects an explicitly supplied MaxResults outside [1, 500].
func validateListMaxResults(input map[string]any) error {
	raw, present := input["MaxResults"]
	if !present || raw == nil {
		return nil
	}

	if n := maxResultsField(input); n < listMaxResultsMin || n > listMaxResultsMax {
		return fmt.Errorf("%w: MaxResults must be between %d and %d", ErrInvalidParameter,
			listMaxResultsMin, listMaxResultsMax)
	}

	return nil
}
