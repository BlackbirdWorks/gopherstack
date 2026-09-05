package lightsail_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/lightsail"
	"github.com/stretchr/testify/require"
)

// walkAttempts is how many times each paginated walk is repeated against the
// same, unchanged backend state. Go randomises map iteration order per
// range, not per map instance, so a non-total sort over store.Table.All()
// can (and, per the glue precedent, reliably does) disagree with itself
// across separate calls with nothing changed in between. One walk can pass
// by luck; the bug is about instability *across* calls.
const walkAttempts = 30

// walkAndVerify repeats a paginated walk walkAttempts times, failing if any
// attempt drops or duplicates an item relative to want, or returns the same
// id on two different pages within one walk.
func walkAndVerify(t *testing.T, want map[string]bool, listPage func(token string) (ids []string, next string)) {
	t.Helper()

	for attempt := range walkAttempts {
		got := make(map[string]bool, len(want))
		token := ""

		for {
			ids, next := listPage(token)
			for _, id := range ids {
				require.Falsef(t, got[id], "attempt %d: id %q returned on more than one page", attempt, id)
				got[id] = true
			}

			if next == "" {
				break
			}

			token = next
		}

		require.Equalf(t, want, got, "attempt %d: paginated walk did not reproduce the created set exactly", attempt)
	}
}

// GetOperations/GetOperationsForResource/GetSetupHistory always page at the
// fixed defaultPageLimit (100, since none of the three Get* calls the real
// Lightsail API mirrors takes a page-size parameter), so the tie group must
// exceed one page to exercise a real page boundary.
const tieGroupSize = 105

func TestGetOperationsSortIsTotal(t *testing.T) {
	t.Parallel()

	b := lightsail.NewInMemoryBackend(context.Background(), "111111111111", "us-east-1")
	tie := time.Now().UTC()

	want := make(map[string]bool, tieGroupSize)
	for i := range tieGroupSize {
		id := fmt.Sprintf("op-%04d", i)
		b.SeedOperationForTest(&lightsail.Operation{
			ID:        id,
			Type:      "CreateInstance",
			Status:    "Succeeded",
			CreatedAt: tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		p, err := b.GetOperations(token)
		require.NoError(t, err)
		ids := make([]string, len(p.Data))
		for i, op := range p.Data {
			ids[i] = op.ID
		}

		return ids, p.Next
	})
}

func TestGetOperationsForResourceSortIsTotal(t *testing.T) {
	t.Parallel()

	b := lightsail.NewInMemoryBackend(context.Background(), "111111111111", "us-east-1")
	tie := time.Now().UTC()
	const resourceName = "my-instance"

	want := make(map[string]bool, tieGroupSize)
	for i := range tieGroupSize {
		id := fmt.Sprintf("op-%04d", i)
		b.SeedOperationForTest(&lightsail.Operation{
			ID:           id,
			Type:         "CreateInstance",
			Status:       "Succeeded",
			ResourceName: resourceName,
			CreatedAt:    tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		p, err := b.GetOperationsForResource(resourceName, token)
		require.NoError(t, err)
		ids := make([]string, len(p.Data))
		for i, op := range p.Data {
			ids[i] = op.ID
		}

		return ids, p.Next
	})
}

func TestGetSetupHistorySortIsTotal(t *testing.T) {
	t.Parallel()

	b := lightsail.NewInMemoryBackend(context.Background(), "111111111111", "us-east-1")
	tie := time.Now().UTC()
	const resourceName = "my-instance"

	want := make(map[string]bool, tieGroupSize)
	for i := range tieGroupSize {
		id := fmt.Sprintf("setup-%04d", i)
		b.SeedSetupHistoryEntryForTest(&lightsail.SetupHistoryEntry{
			OperationID:  id,
			ResourceName: resourceName,
			InstanceName: resourceName,
			Status:       "Succeeded",
			CreatedAt:    tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		p, err := b.GetSetupHistory(resourceName, token)
		require.NoError(t, err)
		ids := make([]string, len(p.Data))
		for i, e := range p.Data {
			ids[i] = e.OperationID
		}

		return ids, p.Next
	})
}

// TestGetSetupHistoryAllResourcesSortIsTotal exercises the resourceName==""
// branch, which reads from store.Table.All() (unordered map iteration)
// rather than the byResource Index.
func TestGetSetupHistoryAllResourcesSortIsTotal(t *testing.T) {
	t.Parallel()

	b := lightsail.NewInMemoryBackend(context.Background(), "111111111111", "us-east-1")
	tie := time.Now().UTC()

	want := make(map[string]bool, tieGroupSize)
	for i := range tieGroupSize {
		id := fmt.Sprintf("setup-%04d", i)
		b.SeedSetupHistoryEntryForTest(&lightsail.SetupHistoryEntry{
			OperationID:  id,
			ResourceName: fmt.Sprintf("instance-%04d", i),
			Status:       "Succeeded",
			CreatedAt:    tie,
		})
		want[id] = true
	}

	walkAndVerify(t, want, func(token string) ([]string, string) {
		p, err := b.GetSetupHistory("", token)
		require.NoError(t, err)
		ids := make([]string, len(p.Data))
		for i, e := range p.Data {
			ids[i] = e.OperationID
		}

		return ids, p.Next
	})
}
