package kinesisvideo

import (
	"fmt"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxMediaTypeLen    = 128
	maxDeviceNameLen   = 128
	maxKmsKeyIDLen     = 2048
	maxVersionLen      = 64
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	maxListStreamsMax  = 10000
	maxListChannelsMax = 1000
	minMessageTTL      = 5
	maxMessageTTL      = 120
)

var (
	mediaTypeRE  = regexp.MustCompile(`^[\w\-.+]+/[\w\-.+]+(,[\w\-.+]+/[\w\-.+]+)*$`)
	deviceNameRE = regexp.MustCompile(`^[\w\-]+$`)
	tagTextRE    = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]*$`)
)

func invalid(format string, args ...any) error {
	return awserr.New(fmt.Sprintf(format, args...), awserr.ErrInvalidParameter)
}

func validateMediaType(v string) error {
	if v == "" {
		return nil
	}

	if len(v) > maxMediaTypeLen || !mediaTypeRE.MatchString(v) {
		return invalid("Value '%s' at 'mediaType' failed to satisfy constraint: Member must satisfy regular "+
			"expression pattern: [\\w\\-\\.\\+]+/[\\w\\-\\.\\+]+(,[\\w\\-\\.\\+]+/[\\w\\-\\.\\+]+)*", v)
	}

	return nil
}

func validateDeviceName(v string) error {
	if v == "" {
		return nil
	}

	if len(v) > maxDeviceNameLen || !deviceNameRE.MatchString(v) {
		return invalid("Value '%s' at 'deviceName' failed to satisfy constraint: Member must satisfy regular "+
			"expression pattern: [\\w\\-]+", v)
	}

	return nil
}

func validateKmsKeyID(v string) error {
	if len(v) > maxKmsKeyIDLen {
		return invalid("Value at 'kmsKeyId' failed to satisfy constraint: Member must have length less than or "+
			"equal to %d", maxKmsKeyIDLen)
	}

	return nil
}

func validateCurrentVersion(v string) error {
	if v == "" {
		return nil
	}

	if len(v) > maxVersionLen {
		return invalid("Value '%s' at 'currentVersion' failed to satisfy constraint: Member must have length "+
			"less than or equal to %d", v, maxVersionLen)
	}

	return nil
}

func validateStorageTier(v string) error {
	if v == "" || v == "HOT" || v == "WARM" {
		return nil
	}

	return invalid("Value '%s' at 'defaultStorageTier' failed to satisfy constraint: Member must satisfy enum "+
		"value set: [HOT, WARM]", v)
}

func validateChannelType(v string) error {
	if v == "" || v == channelTypeSingleMaster || v == "FULL_MESH" {
		return nil
	}

	return invalid("Value '%s' at 'channelType' failed to satisfy constraint: Member must satisfy enum "+
		"value set: [SINGLE_MASTER, FULL_MESH]", v)
}

func validateMessageTTL(v int32) error {
	if v != 0 && (v < minMessageTTL || v > maxMessageTTL) {
		return invalid("Value '%d' at 'messageTtlSeconds' failed to satisfy constraint: Member must have value "+
			"between %d and %d", v, minMessageTTL, maxMessageTTL)
	}

	return nil
}

func validateListArgs(nextToken string, maxResults, upper int, operator string) error {
	if maxResults < 0 || maxResults > upper {
		return invalid("Value '%d' at 'maxResults' failed to satisfy constraint: Member must have value between "+
			"1 and %d", maxResults, upper)
	}

	if err := page.ValidateToken(nextToken); err != nil {
		return invalid("Invalid NextToken")
	}

	if operator != "" && operator != comparisonOperatorBeginsWith {
		return invalid("Value '%s' at 'comparisonOperator' failed to satisfy constraint: Member must satisfy "+
			"enum value set: [BEGINS_WITH]", operator)
	}

	return nil
}
