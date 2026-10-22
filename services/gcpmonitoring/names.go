package gcpmonitoring

import (
	"crypto/rand"
	"math/big"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	collMetricDescriptors   = "metricDescriptors"
	collAlertPolicies       = "alertPolicies"
	collChannels            = "notificationChannels"
	collChannelDescriptors  = "notificationChannelDescriptors"
	collUptime              = "uptimeCheckConfigs"
	collGroups              = "groups"
	collConditions          = "conditions"
	collResourceDescriptors = "monitoredResourceDescriptors"
	numericIDDigits         = 19
	nameParts               = 3
	hexSuffixLen            = 6
	decimalBase             = 10
	projectScope            = "projects/"
)

var projectRe = regexp.MustCompile(`^projects/([^/]+)$`)

// projectOf validates "projects/ID" and returns the id.
func projectOf(name string) (string, error) {
	m := projectRe.FindStringSubmatch(name)
	if m == nil {
		return "", errInvalid("Invalid name [%s]: expected projects/[PROJECT_ID_OR_NUMBER].", name)
	}

	return m[1], nil
}

// splitName validates projects/P/collection/ID and returns the project and the id.
func splitName(name, collection string) (string, string, error) {
	rest, ok := strings.CutPrefix(name, projectScope)
	if !ok {
		return "", "", errInvalid("Invalid %s name [%s].", collection, name)
	}

	parts := strings.SplitN(rest, "/", nameParts)
	if len(parts) != nameParts || parts[0] == "" || parts[1] != collection || parts[2] == "" {
		return "", "", errInvalid("Invalid %s name [%s].", collection, name)
	}

	return parts[0], parts[2], nil
}

// numericID returns a random decimal id of the given digit count without a leading zero.
func numericID(digits int) string {
	out := make([]byte, digits)

	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(decimalBase))
		d := int64(1)

		if err == nil {
			d = n.Int64()
		}

		if i == 0 && d == 0 {
			d = 1
		}

		out[i] = byte('0' + d)
	}

	return string(out)
}

func newNumericID() string { return numericID(numericIDDigits) }

func newHexSuffix() string { return awsid.NewN("", hexSuffixLen) }
