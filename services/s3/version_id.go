package s3

import (
	"errors"
	"regexp"
)

// versionIDPattern covers the emulator's 32-hex ids, its decimal-nanosecond
// fallback, and real-AWS-shaped 32-char ids.
var versionIDPattern = regexp.MustCompile(
	`^(?:[A-Za-z0-9._-]{32}|[0-9]{1,20})$`,
)

// versionMissError classifies a versionId that matched no stored version:
// malformed ids are InvalidArgument, well-formed absent ids are NoSuchVersion.
func versionMissError(key, versionID string) error {
	if isWellFormedVersionID(versionID) {
		return &versionError{base: ErrNoSuchVersion, key: key, versionID: versionID}
	}

	return &versionError{base: ErrInvalidVersionID, key: key, versionID: versionID}
}

// versionError carries the key and versionId that the real S3 error body
// echoes back; errors.Is still matches the wrapped sentinel.
type versionError struct {
	base      error
	key       string
	versionID string
}

func (e *versionError) Error() string { return e.base.Error() }
func (e *versionError) Unwrap() error { return e.base }

// applyVersionDetail adds the per-error elements S3 returns for NoSuchVersion
// (Key, VersionId) and malformed ids (ArgumentName, ArgumentValue).
func applyVersionDetail(resp *ErrorResponse, err error) {
	var ve *versionError
	if !errors.As(err, &ve) {
		return
	}

	if errors.Is(ve.base, ErrInvalidVersionID) {
		resp.ArgumentName = "versionId"
		resp.ArgumentValue = ve.versionID

		return
	}

	resp.Key = ve.key
	resp.VersionID = ve.versionID
}

func isWellFormedVersionID(versionID string) bool {
	return versionID == NullVersion || versionIDPattern.MatchString(versionID)
}
