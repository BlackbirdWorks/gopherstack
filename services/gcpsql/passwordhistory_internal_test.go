package gcpsql

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPasswordHistoryIsSaltedAndNotPlaintext(t *testing.T) {
	t.Parallel()

	u := &userRec{User: User{Name: "alice"}}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	recordPassword(u, "Sup3r!secret", now)
	recordPassword(u, "Sup3r!secret", now)
	recordPassword(u, "Other!pw1", now)

	require.Len(t, u.History, 3)

	for _, e := range u.History {
		assert.NotContains(t, e, "secret")
		assert.Contains(t, e, "$")
		assert.Greater(t, len(e), 64+16, "salt and a 32 byte key")
	}

	assert.NotEqual(t, u.History[0], u.History[1], "same password, different salt")

	pol := passwordPolicy{enabled: true, reuse: 3}

	require.Error(t, checkPassword(pol, u, "Sup3r!secret", false, now))
	require.Error(t, checkPassword(pol, u, "Other!pw1", false, now))
	require.NoError(t, checkPassword(pol, u, "Fresh!pw2", false, now))

	polOne := passwordPolicy{enabled: true, reuse: 1}
	assert.NoError(t, checkPassword(polOne, u, "Sup3r!secret", false, now), "only the newest one is remembered")
	assert.NotContains(t, strings.Join(u.History, ","), "alice")
}

func TestHistoryMatchesIgnoresMalformedEntries(t *testing.T) {
	t.Parallel()

	assert.False(t, historyMatches([]string{"", "zz$zz", "nodollar", "aa$"}, "u", "pw"))
}
