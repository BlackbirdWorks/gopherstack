package s3

import "regexp"

// versionIDPattern covers the emulator's 32-hex ids, its decimal-nanosecond
// fallback, and real-AWS-shaped 32-char ids.
var versionIDPattern = regexp.MustCompile(
	`^(?:[A-Za-z0-9._-]{32}|[0-9]{1,20})$`,
)

// versionMissError classifies a versionId that matched no stored version:
// malformed ids are InvalidArgument, well-formed absent ids are NoSuchVersion.
func versionMissError(versionID string) error {
	if isWellFormedVersionID(versionID) {
		return ErrNoSuchVersion
	}

	return ErrInvalidVersionID
}

func isWellFormedVersionID(versionID string) bool {
	return versionID == NullVersion || versionIDPattern.MatchString(versionID)
}
