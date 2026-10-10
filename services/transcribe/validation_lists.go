package transcribe

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/transcribe/types"
)

var resourceNameRe = regexp.MustCompile(`^[0-9a-zA-Z._-]{1,200}$`)

var codePrefixRe = regexp.MustCompile(`^[A-Za-z]+Exception: `)

func validateResourceName(field, name string) error {
	if name == "" {
		return fmt.Errorf("%w: %s is required", ErrValidation, field)
	}

	if !resourceNameRe.MatchString(name) {
		return fmt.Errorf("%w: %s must match ^[0-9a-zA-Z._-]{1,200}$", ErrValidation, field)
	}

	return nil
}

func enumStrings[T ~string](values []T) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = string(v)
	}

	return out
}

func validateListPage(nextToken string, maxResults int32) error {
	if maxResults < 0 || maxResults > transcribeMaxResultsUpperBound {
		return fmt.Errorf("%w: MaxResults must be between 1 and %d", ErrValidation, transcribeMaxResultsUpperBound)
	}

	if nextToken == "" {
		return nil
	}

	if idx, err := strconv.Atoi(nextToken); err != nil || idx < 0 {
		return fmt.Errorf("%w: NextToken is invalid", ErrValidation)
	}

	return nil
}

func validateListFilter(field, value string, allowed []string) error {
	if value == "" || slices.Contains(allowed, value) {
		return nil
	}

	return fmt.Errorf("%w: %s %q must be one of %v", ErrValidation, field, value, allowed)
}

func validateJobListRequest(status, nextToken string, maxResults int32) error {
	allowed := enumStrings(sdktypes.TranscriptionJobStatus("").Values())
	if err := validateListFilter("Status", status, allowed); err != nil {
		return err
	}

	return validateListPage(nextToken, maxResults)
}

func validateCallAnalyticsJobListRequest(status, nextToken string, maxResults int32) error {
	allowed := enumStrings(sdktypes.CallAnalyticsJobStatus("").Values())
	if err := validateListFilter("Status", status, allowed); err != nil {
		return err
	}

	return validateListPage(nextToken, maxResults)
}

func validateScribeJobListRequest(status, nextToken string, maxResults int32) error {
	allowed := enumStrings(sdktypes.MedicalScribeJobStatus("").Values())
	if err := validateListFilter("Status", status, allowed); err != nil {
		return err
	}

	return validateListPage(nextToken, maxResults)
}

func validateVocabularyListRequest(state, nextToken string, maxResults int32) error {
	allowed := enumStrings(sdktypes.VocabularyState("").Values())
	if err := validateListFilter("StateEquals", state, allowed); err != nil {
		return err
	}

	return validateListPage(nextToken, maxResults)
}

func validateModelListRequest(status, nextToken string, maxResults int32) error {
	allowed := enumStrings(sdktypes.ModelStatus("").Values())
	if err := validateListFilter("StatusEquals", status, allowed); err != nil {
		return err
	}

	return validateListPage(nextToken, maxResults)
}

func errMessage(err error) string {
	return codePrefixRe.ReplaceAllString(err.Error(), "")
}
