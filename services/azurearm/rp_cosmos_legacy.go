package azurearm

import (
	"errors"
	"strings"
)

// legacyDatabaseParts is the segment count of an "account/database" key.
const legacyDatabaseParts = 2

// legacyKey is the snapshot key of a database (container == "") or container offer.
func legacyKey(account, db, container string) string {
	if container == "" {
		return throughputKey(account, db)
	}

	return throughputKey(account, db, container)
}

// migrateLegacyThroughput moves throughput entries restored from older snapshots into the data plane, where
// offers now live. Entries whose database or container is not there yet stay for a later attempt. The lock must be
// held.
func (p *CosmosProvider) migrateLegacyThroughput() {
	for key, tp := range p.throughput {
		db, container, ok := p.legacyTarget(key)
		if !ok {
			continue
		}

		if _, err := p.dataPlane.GetThroughput(db, container); errors.Is(err, ErrCosmosThroughputNotFound) {
			if p.dataPlane.PutThroughput(db, container, tp) != nil {
				continue
			}
		}

		delete(p.throughput, key)
	}
}

// legacyTarget resolves a lower-cased "account/db[/container]" key to the data plane's own names.
func (p *CosmosProvider) legacyTarget(key string) (string, string, bool) {
	parts := strings.Split(key, "/")
	if len(parts) < legacyDatabaseParts {
		return "", "", false
	}

	var db string

	for _, d := range p.dataPlane.ListDatabases() {
		if strings.EqualFold(d.ID, parts[1]) {
			db = d.ID
		}
	}

	if db == "" || len(parts) == legacyDatabaseParts {
		return db, "", db != ""
	}

	containers, err := p.dataPlane.ListContainers(db)
	if err != nil {
		return "", "", false
	}

	for _, c := range containers {
		if strings.EqualFold(c.ID, parts[2]) {
			return db, c.ID, true
		}
	}

	return "", "", false
}
