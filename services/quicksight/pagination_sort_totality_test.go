package quicksight_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/require"
)

// walkAttempts is how many times each paginated walk is repeated against the
// same, unchanged backend state. Go randomises map iteration order per
// range, not per map instance, so a non-total sort over store.Table.All()
// can (and, per the glue precedent, reliably does) disagree with itself
// across separate calls with nothing changed in between. One walk can pass
// by luck; the bug is about instability *across* calls.
const walkAttempts = 30

// TestListUsersIndexCapacityCrossNamespaceSortIsTotal proves
// ListUsersIndexCapacity(namespace="") -- which scans every namespace, per
// its own handler passing an empty namespace straight through -- cannot
// safely sort/paginate on UserName alone. storedUser's store.Table key is
// accountID/namespace/UserName, so UserName is only guaranteed unique
// *within* one namespace; two different namespaces can each hold a user
// named "alice". Before the fix this also broke the cursor itself:
// paginateUserIndexCapacity matched nextToken by equality against UserName,
// so a tied UserName made every subsequent page resolve back to the first
// "alice" and repeat forever, not just reorder.
func TestListUsersIndexCapacityCrossNamespaceSortIsTotal(t *testing.T) {
	t.Parallel()

	const accountID = "111111111111"
	b := quicksight.NewInMemoryBackend(accountID, "us-east-1")

	_, err := b.CreateNamespace(accountID, "ns-a", "", nil)
	require.NoError(t, err)
	_, err = b.CreateNamespace(accountID, "ns-b", "", nil)
	require.NoError(t, err)

	userA, err := b.RegisterUser(accountID, "ns-a", "alice", "alice@ns-a.example.com", "READER", "QUICKSIGHT", "", nil)
	require.NoError(t, err)
	userB, err := b.RegisterUser(accountID, "ns-b", "alice", "alice@ns-b.example.com", "READER", "QUICKSIGHT", "", nil)
	require.NoError(t, err)

	want := map[string]bool{userA.Arn: true, userB.Arn: true}

	for attempt := range walkAttempts {
		got := make(map[string]bool, len(want))
		token := ""

		pages := 0
		for {
			pages++
			require.LessOrEqualf(t, pages, 10, "attempt %d: paginated walk did not terminate (stuck cursor)", attempt)

			page, next, listErr := b.ListUsersIndexCapacity(
				accountID, "", quicksight.UserIndexCapacityQuery{}, 1, token,
			)
			require.NoError(t, listErr)

			for _, u := range page {
				require.Falsef(
					t,
					got[u.UserArn],
					"attempt %d: UserArn %q returned on more than one page",
					attempt,
					u.UserArn,
				)
				got[u.UserArn] = true
			}

			if next == "" {
				break
			}

			token = next
		}

		require.Equalf(t, want, got, "attempt %d: paginated walk did not reproduce the created set exactly", attempt)
	}
}
