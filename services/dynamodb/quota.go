package dynamodb

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
)

const (
	quotaServiceCode = "dynamodb"
	quotaTables      = "L-F98FE922"

	arnAccountPartIndex = 4
)

// SetQuotaLimiter makes CreateTable honour the Service Quotas table limit; nil disables it.
func (db *InMemoryDB) SetQuotaLimiter(f quotalimit.Func) { db.quotaGate.Set(f) }

// TableCount returns the tables an account owns in a Region.
func (db *InMemoryDB) TableCount(account, region string) int {
	db.mu.RLock("TableCount")
	defer db.mu.RUnlock()

	return db.tableCountLocked(account, region)
}

func (db *InMemoryDB) tableCountLocked(account, region string) int {
	n := 0

	for _, t := range db.tablesByRegion.Get(region) {
		parts := strings.Split(t.TableArn, ":")
		if len(parts) > arnAccountPartIndex && parts[arnAccountPartIndex] == account {
			n++
		}
	}

	return n
}

func (db *InMemoryDB) tableLimit(ctx context.Context, region string) (quotalimit.Limit, string) {
	account := accountFromContext(ctx, db)

	return db.quotaGate.Lookup(account, region, quotaServiceCode, quotaTables), account
}
