package app

import (
	_ "embed" // quota classification table
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
)

//go:embed testdata/service_quota_classes.json
var quotaClassesJSON []byte

// quotaClassEntry classifies the catalogued quotas of one service:
// c counted (has a live counter), e count-of-resources without a counter (Reason says why),
// s per-resource size or length limit, r rate or concurrency, o not derivable in an emulator.
type quotaClassEntry struct {
	Quotas map[string]string `json:"quotas"`
	Reason string            `json:"reason"`
}

func loadQuotaClasses(t *testing.T) map[string]quotaClassEntry {
	t.Helper()

	var classes map[string]quotaClassEntry
	require.NoError(t, json.Unmarshal(quotaClassesJSON, &classes, jsonx.V1()))

	return classes
}

func catalogQuotaKeys(t *testing.T) map[string]bool {
	t.Helper()

	b := servicequotasbackend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion)
	keys := map[string]bool{}
	token := ""

	for {
		pg, err := b.ListServices(token, nil)
		require.NoError(t, err)

		for _, svc := range pg.Data {
			for _, q := range svc.Quotas {
				keys[svc.Code+"/"+q.Code] = true
			}
		}

		if pg.Next == "" {
			return keys
		}

		token = pg.Next
	}
}

func TestQuotaClassificationCoversCatalog(t *testing.T) {
	t.Parallel()

	classes := loadQuotaClasses(t)
	catalog := catalogQuotaKeys(t)
	classified := map[string]string{}

	for svc, entry := range classes {
		hasExempt := false

		for code, class := range entry.Quotas {
			require.Contains(t, []string{"c", "e", "s", "r", "o"}, class, "%s/%s", svc, code)
			classified[svc+"/"+code] = class
			hasExempt = hasExempt || class == "e"
		}

		if hasExempt {
			assert.NotEmpty(t, entry.Reason, "%s has count quotas without a counter and no reason", svc)
		}
	}

	for k := range catalog {
		assert.Contains(
			t,
			classified,
			k,
			"catalogued quota %s is not classified: add it to testdata/service_quota_classes.json as c (and wire a counter in "+
				"service_quota_counters.go), e (count, no counter yet), s (size or length) or r (rate or concurrency)",
			k,
		)
	}

	for k := range classified {
		assert.True(t, catalog[k], "classified quota %s is not catalogued", k)
	}
}

func TestQuotaCountersMatchClassification(t *testing.T) {
	t.Parallel()

	classes := loadQuotaClasses(t)
	counters := map[string]bool{}

	for k := range quotaCounters {
		counters[k] = true
	}

	for k := range extraQuotaCounters {
		counters[k] = true
	}

	counted := map[string]bool{}

	for svc, entry := range classes {
		for code, class := range entry.Quotas {
			if class == "c" {
				counted[svc+"/"+code] = true
			}
		}
	}

	for k := range counted {
		assert.True(t, counters[k],
			"%s is classified c but has no counter: add it to serviceQuotaCounters/quotaCounters or reclassify it e", k)
	}

	for k := range counters {
		assert.True(t, counted[k], "%s has a counter but is not classified c in testdata/service_quota_classes.json", k)
	}
}
