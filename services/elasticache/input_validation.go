package elasticache

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxCacheClusterIDLen     = 50
	maxReplicationGroupIDLen = 40
	minAuthTokenLen          = 16
	maxAuthTokenLen          = 128
)

var (
	cacheIDRegex   = regexp.MustCompile(`^[a-zA-Z](-?[a-zA-Z0-9])*$`)
	nodeTypeRegex  = regexp.MustCompile(`^cache\.[a-z0-9-]+\.[a-z0-9]+$`)
	authTokenBadRe = regexp.MustCompile(`[^\x21-\x7e]|[@"/]`)
)

func validateCacheID(field, id string, maxLen int) error {
	if len(id) > maxLen || !cacheIDRegex.MatchString(id) {
		return fmt.Errorf(
			"%w: the parameter %s is not a valid identifier: it must contain 1-%d alphanumeric characters or "+
				"hyphens, begin with a letter, and not end with a hyphen or contain two consecutive hyphens",
			ErrInvalidParameterValue, field, maxLen,
		)
	}

	return nil
}

func validateCacheEngine(engine string) error {
	switch engine {
	case "", engineRedis, engineValkey, engineMemcached:
		return nil
	}

	return fmt.Errorf(
		"%w: invalid Engine %q; must be one of redis, valkey or memcached", ErrInvalidParameterValue, engine,
	)
}

func validateCacheNodeType(nodeType string) error {
	if nodeType == "" || nodeTypeRegex.MatchString(nodeType) {
		return nil
	}

	return fmt.Errorf("%w: invalid CacheNodeType %q", ErrInvalidParameterValue, nodeType)
}

func validateNumCacheNodesRaw(raw string) error {
	if raw == "" {
		return nil
	}

	if n, err := strconv.Atoi(raw); err != nil || n < 1 {
		return fmt.Errorf("%w: NumCacheNodes must be a positive integer, got %q", ErrInvalidParameterValue, raw)
	}

	return nil
}

func validateAuthToken(token string, transitEncryption bool) error {
	if token == "" {
		return nil
	}

	if len(token) < minAuthTokenLen || len(token) > maxAuthTokenLen || authTokenBadRe.MatchString(token) {
		return fmt.Errorf(
			"%w: AuthToken must be %d-%d printable characters and cannot contain '@', '\"' or '/'",
			ErrInvalidParameterValue, minAuthTokenLen, maxAuthTokenLen,
		)
	}

	if !transitEncryption {
		return fmt.Errorf(
			"%w: AuthToken can only be set when TransitEncryptionEnabled is true", ErrInvalidParameterCombination,
		)
	}

	return nil
}

func validateMarkerToken(marker string) error {
	if err := page.ValidateToken(strings.TrimSpace(marker)); err != nil {
		return fmt.Errorf("%w: invalid Marker %q", ErrInvalidParameterValue, marker)
	}

	return nil
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}
