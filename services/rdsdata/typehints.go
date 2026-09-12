package rdsdata

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// SqlParameter.TypeHint enum values (rdsdata@v1.35.4 types/enums.go's
// TypeHint; also https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html).
const (
	typeHintDate      = "DATE"
	typeHintDecimal   = "DECIMAL"
	typeHintJSON      = "JSON"
	typeHintTime      = "TIME"
	typeHintTimestamp = "TIMESTAMP"
	typeHintUUID      = "UUID"
)

// errBadTypeHintValue is wrapped by ErrValidation so it 400s as
// BadRequestException like every other input-validation failure in this
// package (see errIsValidation in errors.go).
var errBadTypeHintValue = errors.New("value does not match typeHint format")

// timeHintRe/timestampHintRe/decimalHintRe/uuidHintRe enforce the formats
// documented on SqlParameter.TypeHint. DATE uses time.Parse directly since
// "YYYY-MM-DD" has no optional component; the others have an optional
// fractional-seconds/sign suffix that's simpler to express as a regexp than
// as a set of time.Parse layout variants.
var (
	timeHintRe      = regexp.MustCompile(`^([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?$`)
	timestampHintRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} ([01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?$`)
	decimalHintRe   = regexp.MustCompile(`^[+-]?\d+(\.\d+)?$`)
	uuidHintRe      = regexp.MustCompile(
		`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`,
	)
)

// validateTypeHints enforces the documented stringValue format for each
// SqlParameter.TypeHint (see the enum doc comment above): DATE
// "YYYY-MM-DD", DECIMAL a plain decimal number, JSON any valid JSON text,
// TIME "HH:MM:SS[.FFF]", TIMESTAMP "YYYY-MM-DD HH:MM:SS[.FFF]", UUID a
// standard 8-4-4-4-12 hex UUID. The hint only constrains a stringValue
// parameter, per the doc's "the corresponding String parameter value..."
// wording -- a hint on a non-string or null value, or an unrecognized hint
// string, is a no-op, matching this package's existing tolerant handling of
// TypeHint (see models.go's SQLParameter doc comment).
//
// On success the parameter still binds unchanged (see engine.go's
// fieldToValue): the mock SQLite engine has no distinct DATE/DECIMAL/JSON/
// TIME/TIMESTAMP/UUID column types to convert into, so validating the
// format is the only real-AWS-documented behavior this mock can reproduce
// without a live Aurora cluster to observe actual bind/coercion semantics
// against -- see PARITY.md.
func validateTypeHints(params []SQLParameter) error {
	for _, p := range params {
		if p.TypeHint == "" || p.Value.StringValue == nil {
			continue
		}

		if err := validateTypeHintFormat(p.TypeHint, *p.Value.StringValue); err != nil {
			return fmt.Errorf("%w: parameter %q: %w", ErrValidation, p.Name, err)
		}
	}

	return nil
}

func validateTypeHintFormat(hint, value string) error {
	switch hint {
	case typeHintDate:
		if _, err := time.Parse(time.DateOnly, value); err != nil {
			return fmt.Errorf("%w: %s requires format YYYY-MM-DD", errBadTypeHintValue, typeHintDate)
		}
	case typeHintTime:
		if !timeHintRe.MatchString(value) {
			return fmt.Errorf("%w: %s requires format HH:MM:SS[.FFF]", errBadTypeHintValue, typeHintTime)
		}
	case typeHintTimestamp:
		if !timestampHintRe.MatchString(value) {
			return fmt.Errorf(
				"%w: %s requires format YYYY-MM-DD HH:MM:SS[.FFF]", errBadTypeHintValue, typeHintTimestamp,
			)
		}
	case typeHintDecimal:
		if !decimalHintRe.MatchString(value) {
			return fmt.Errorf("%w: %s requires a decimal number", errBadTypeHintValue, typeHintDecimal)
		}
	case typeHintJSON:
		if !json.Valid([]byte(value)) {
			return fmt.Errorf("%w: %s requires valid JSON", errBadTypeHintValue, typeHintJSON)
		}
	case typeHintUUID:
		if !uuidHintRe.MatchString(value) {
			return fmt.Errorf("%w: %s requires a UUID", errBadTypeHintValue, typeHintUUID)
		}
	}

	return nil
}
