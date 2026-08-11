package detective

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrValidation, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

// validateAccountID returns true if id is exactly 12 ASCII digits.
func validateAccountID(id string) bool {
	if len(id) != accountIDLen {
		return false
	}

	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// validateEmail returns true if email contains an @ with chars on both sides.
func validateEmail(email string) bool {
	idx := strings.Index(email, "@")

	return idx > 0 && idx < len(email)-1
}

// InMemoryBackend implements StorageBackend using in-memory maps.
//
// graphs, members, and investigations are store.Table-backed (see
// store_setup.go); tags, datasources, and orgConfigs remain plain maps since
// their values are not *T; orgAdmins remains a plain order-sensitive slice.
type InMemoryBackend struct {
	members               *store.Table[storedMember]
	mu                    *lockmetrics.RWMutex
	registry              *store.Registry
	graphs                *store.Table[storedGraph]
	membersByGraph        *store.Index[storedMember]
	investigations        *store.Table[storedInvestigation]
	investigationsByGraph *store.Index[storedInvestigation]
	tags                  map[string]map[string]string
	datasources           map[string]map[string]string
	datasourceChangedAt   map[string]map[string]time.Time
	orgConfigs            map[string]bool
	accountID             string
	region                string
	orgAdmins             []*storedOrgAdmin
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                  lockmetrics.New("detective"),
		registry:            store.NewRegistry(),
		accountID:           accountID,
		region:              region,
		tags:                make(map[string]map[string]string),
		datasources:         make(map[string]map[string]string),
		datasourceChangedAt: make(map[string]map[string]time.Time),
		orgAdmins:           nil,
		orgConfigs:          make(map[string]bool),
	}

	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) graphARN(id string) string {
	return arn.Build("detective", b.region, b.accountID, fmt.Sprintf("graph:%s", id))
}

// AccountID returns the account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the region.
func (b *InMemoryBackend) Region() string { return b.region }

// encodePageToken encodes a pagination offset as an opaque base64 token.
func encodePageToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// decodePageToken decodes an opaque base64 pagination token back to an offset.
// Returns 0 and no error when the token is empty.
func decodePageToken(tok string) (int, error) {
	if tok == "" {
		return 0, nil
	}

	b, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid pagination token", ErrValidation)
	}

	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, fmt.Errorf("%w: invalid pagination token", ErrValidation)
	}

	return n, nil
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.datasources = make(map[string]map[string]string)
	b.datasourceChangedAt = make(map[string]map[string]time.Time)
	b.orgAdmins = nil
	b.orgConfigs = make(map[string]bool)
}
