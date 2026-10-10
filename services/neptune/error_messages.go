package neptune

import (
	"fmt"
	"regexp"
	"strings"
)

type notFoundRewrite struct {
	re   *regexp.Regexp
	repl string
}

func notFoundRewrites() []notFoundRewrite {
	return []notFoundRewrite{
		{regexp.MustCompile(`^cluster snapshot (.+) not found$`), "DBClusterSnapshot not found: $1"},
		{regexp.MustCompile(`^cluster parameter group (.+) not found$`), "DBParameterGroup not found: $1"},
		{regexp.MustCompile(`^subnet group (.+) not found$`), "DBSubnetGroup not found: $1"},
		{regexp.MustCompile(`^cluster (.+) not found$`), "DBCluster not found: $1"},
		{regexp.MustCompile(`^instance (.+) not found$`), "DBInstance $1 not found."},
	}
}

func wireMessage(code, msg string) string {
	msg = strings.TrimPrefix(msg, code+": ")
	msg = strings.TrimPrefix(msg, strings.TrimSuffix(code, "Fault")+": ")
	for _, r := range notFoundRewrites() {
		if r.re.MatchString(msg) {
			return r.re.ReplaceAllString(msg, r.repl)
		}
	}

	return msg
}

var subnetGroupNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._ -]{1,255}$`)

func validateSubnetGroupName(name string) error {
	if !subnetGroupNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: DBSubnetGroupName %q is not valid: must be 1-255 letters, numbers, periods, underscores, spaces or hyphens",
			ErrInvalidParameter,
			name,
		)
	}

	return nil
}
