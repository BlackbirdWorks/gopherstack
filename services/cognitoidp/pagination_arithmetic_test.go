package cognitoidp_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestListDevices_Pagination covers paginateDevicesLocked (shared by
// ListDevices and AdminListDevices), which searched for the resuming
// device by equality and left start at 0 on a miss (Class B: a stale
// cursor served page one forever).
func TestListDevices_Pagination(t *testing.T) {
	t.Parallel()

	b, _, client := setupTestPoolAndClient(t)
	tokens := signUpConfirmAndLogin(t, b, client.ClientID, "devices-pg-user")

	const n = 7
	for i := range n {
		_, _, err := b.ConfirmDevice(tokens.AccessToken, fmt.Sprintf("dev-%03d", i), "")
		require.NoError(t, err)
	}

	t.Run("boundary_walk", func(t *testing.T) {
		t.Parallel()

		var got []string

		token := ""
		for range n + 1 {
			devices, next, err := b.ListDevices(tokens.AccessToken, 3, token)
			require.NoError(t, err)

			for _, d := range devices {
				got = append(got, d.DeviceKey)
			}

			token = next
			if token == "" {
				break
			}
		}

		want := make([]string, n)
		for i := range n {
			want[i] = fmt.Sprintf("dev-%03d", i)
		}

		assert.Equal(t, want, got, "concatenating every page must reproduce the collection exactly")
	})

	t.Run("exact_division", func(t *testing.T) {
		t.Parallel()

		page1, next1, err := b.ListDevices(tokens.AccessToken, 7, "")
		require.NoError(t, err)
		require.Len(t, page1, 7)
		assert.Empty(t, next1, "a page equal to the collection size must not emit a cursor")
	})

	t.Run("single_page", func(t *testing.T) {
		t.Parallel()

		page, next, err := b.ListDevices(tokens.AccessToken, 100, "")
		require.NoError(t, err)
		assert.Len(t, page, n)
		assert.Empty(t, next)
	})

	t.Run("stale_cursor", func(t *testing.T) {
		t.Parallel()

		page, next, err := b.ListDevices(tokens.AccessToken, 3, "dev-does-not-exist")
		require.NoError(t, err)
		assert.Empty(t, next, "a stale cursor must not produce another cursor")
		assert.Empty(t, page, "a stale cursor must default to the end of the collection, not the start")
	})
}

// TestListGroupsPage_Pagination covers ListGroupsPage's own inline
// equality-scan cursor.
func TestListGroupsPage_Pagination(t *testing.T) {
	t.Parallel()

	b, pool, _ := setupTestPoolAndClient(t)

	const n = 7
	for i := range n {
		_, err := b.CreateGroup(pool.ID, fmt.Sprintf("group-%03d", i), "", 0)
		require.NoError(t, err)
	}

	t.Run("boundary_walk", func(t *testing.T) {
		t.Parallel()

		var got []string

		token := ""
		for range n + 1 {
			groups, next, err := b.ListGroupsPage(pool.ID, 3, token)
			require.NoError(t, err)

			for _, g := range groups {
				got = append(got, g.GroupName)
			}

			token = next
			if token == "" {
				break
			}
		}

		want := make([]string, n)
		for i := range n {
			want[i] = fmt.Sprintf("group-%03d", i)
		}

		assert.Equal(t, want, got)
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		empty, err := b.CreateUserPool("empty-groups-pool")
		require.NoError(t, err)

		groups, next, err := b.ListGroupsPage(empty.ID, 10, "")
		require.NoError(t, err)
		assert.Empty(t, groups)
		assert.Empty(t, next)
	})

	t.Run("stale_cursor", func(t *testing.T) {
		t.Parallel()

		groups, next, err := b.ListGroupsPage(pool.ID, 3, "group-does-not-exist")
		require.NoError(t, err)
		assert.Empty(t, next)
		assert.Empty(t, groups)
	})

	t.Run("stale_cursor_after_deletion", func(t *testing.T) {
		t.Parallel()

		delPool, err := b.CreateUserPool("del-groups-pool")
		require.NoError(t, err)

		_, err = b.CreateGroup(delPool.ID, "group-alpha", "", 0)
		require.NoError(t, err)
		_, err = b.CreateGroup(delPool.ID, "group-beta", "", 0)
		require.NoError(t, err)

		page1, next1, err := b.ListGroupsPage(delPool.ID, 1, "")
		require.NoError(t, err)
		require.Len(t, page1, 1)
		require.NotEmpty(t, next1)

		require.NoError(t, b.DeleteGroup(delPool.ID, next1))

		page2, next2, err := b.ListGroupsPage(delPool.ID, 1, next1)
		require.NoError(t, err, "resuming with a cursor naming a deleted group must not error or hang")
		assert.Empty(t, next2)
		assert.Empty(t, page2)
	})
}

// TestListUsersInGroupPage_Pagination covers ListUsersInGroupPage's own
// inline equality-scan cursor.
func TestListUsersInGroupPage_Pagination(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)

	_, err := b.CreateGroup(pool.ID, "members", "", 0)
	require.NoError(t, err)

	const n = 7

	usernames := make([]string, n)

	for i := range n {
		username := fmt.Sprintf("member-%03d", i)
		usernames[i] = username
		signUpConfirmAndLogin(t, b, client.ClientID, username)
		require.NoError(t, b.AdminAddUserToGroup(pool.ID, username, "members"))
	}

	t.Run("boundary_walk", func(t *testing.T) {
		t.Parallel()

		var got []string

		token := ""
		for range n + 1 {
			users, next, listErr := b.ListUsersInGroupPage(pool.ID, "members", 3, token)
			require.NoError(t, listErr)

			for _, u := range users {
				got = append(got, u.Username)
			}

			token = next
			if token == "" {
				break
			}
		}

		assert.Equal(t, usernames, got)
	})

	t.Run("stale_cursor", func(t *testing.T) {
		t.Parallel()

		users, next, listErr := b.ListUsersInGroupPage(pool.ID, "members", 3, "member-does-not-exist")
		require.NoError(t, listErr)
		assert.Empty(t, next)
		assert.Empty(t, users)
	})
}

// TestListWebAuthnCredentials_Pagination covers ListWebAuthnCredentials's
// own inline equality-scan cursor. Each subtest gets its own user/credential
// set (rather than sharing one across parallel subtests) since the
// stale-cursor subtest mutates its data by deleting a credential.
func TestListWebAuthnCredentials_Pagination(t *testing.T) {
	t.Parallel()

	t.Run("boundary_walk", func(t *testing.T) {
		t.Parallel()

		b, _, client := setupTestPoolAndClient(t)
		tokens := signUpConfirmAndLogin(t, b, client.ClientID, "webauthn-walk-user")

		const n = 7
		for i := range n {
			_, err := b.CompleteWebAuthnRegistration(tokens.AccessToken, fmt.Sprintf("cred-%03d", i), "", nil)
			require.NoError(t, err)
		}

		var got []string

		token := ""
		for range n + 1 {
			creds, next, err := b.ListWebAuthnCredentials(tokens.AccessToken, 3, token)
			require.NoError(t, err)

			for _, c := range creds {
				got = append(got, c.CredentialID)
			}

			token = next
			if token == "" {
				break
			}
		}

		want := make([]string, n)
		for i := range n {
			want[i] = fmt.Sprintf("cred-%03d", i)
		}

		assert.Equal(t, want, got)
	})

	t.Run("stale_cursor_after_deletion", func(t *testing.T) {
		t.Parallel()

		b, _, client := setupTestPoolAndClient(t)
		tokens := signUpConfirmAndLogin(t, b, client.ClientID, "webauthn-stale-user")

		for i := range 3 {
			_, err := b.CompleteWebAuthnRegistration(tokens.AccessToken, fmt.Sprintf("cred-%03d", i), "", nil)
			require.NoError(t, err)
		}

		page1, next1, err := b.ListWebAuthnCredentials(tokens.AccessToken, 1, "")
		require.NoError(t, err)
		require.Len(t, page1, 1)
		require.NotEmpty(t, next1)

		require.NoError(t, b.DeleteWebAuthnCredential(tokens.AccessToken, next1))

		page2, next2, err := b.ListWebAuthnCredentials(tokens.AccessToken, 1, next1)
		require.NoError(t, err, "resuming with a cursor naming a deleted credential must not error or hang")
		assert.Empty(t, next2)
		assert.Empty(t, page2)
	})
}

// TestAdminListUserAuthEvents_Pagination covers paginateAuthEventsLocked.
// This emulator never populates authEvents through any sign-in flow (see
// AdminListUserAuthEvents's own doc comment), so the bug is currently
// unreachable in practice; SeedAuthEventForTest exercises the arithmetic
// directly to prove it is still correct in principle.
func TestAdminListUserAuthEvents_Pagination(t *testing.T) {
	t.Parallel()

	b, pool, client := setupTestPoolAndClient(t)
	user, err := b.SignUp(client.ClientID, "auth-events-user", "Pass1234!", nil)
	require.NoError(t, err)

	const n = 7

	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range n {
		b.SeedAuthEventForTest(pool.ID, user.Username, &cognitoidp.AuthEvent{
			EventID:   fmt.Sprintf("evt-%03d", i),
			CreatedAt: fixed,
		})
	}

	t.Run("boundary_walk", func(t *testing.T) {
		t.Parallel()

		var got []string

		token := ""
		for range n + 1 {
			events, next, listErr := b.AdminListUserAuthEvents(pool.ID, user.Username, 3, token)
			require.NoError(t, listErr)

			for _, e := range events {
				got = append(got, e.EventID)
			}

			token = next
			if token == "" {
				break
			}
		}

		want := make([]string, n)
		for i := range n {
			want[i] = fmt.Sprintf("evt-%03d", i)
		}

		assert.Equal(t, want, got)
	})

	t.Run("stale_cursor", func(t *testing.T) {
		t.Parallel()

		events, next, listErr := b.AdminListUserAuthEvents(pool.ID, user.Username, 3, "evt-does-not-exist")
		require.NoError(t, listErr)
		assert.Empty(t, next)
		assert.Empty(t, events)
	})
}

// TestListUsers_Pagination_StaleCursor covers handleListUsers's inline
// equality-scan cursor (this list is built in the handler itself, not a
// backend helper). TestListUsers_Pagination (handler_users_lifecycle_test.go)
// already proves the boundary walk never drops/duplicates users; it never
// presents a stale cursor, which is the check that finds this bug.
func TestListUsers_Pagination_StaleCursor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, clientID := setupHandlerPoolAndClient(t, h, "list-users-stale-pool")

	const n = 3
	for i := range n {
		signUpAndConfirmViaHandler(t, h, clientID, fmt.Sprintf("user-%03d", i))
	}

	sdkClient := newTestCognitoIDPClient(t, h)

	page1, err := sdkClient.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{
		UserPoolId: aws.String(poolID),
		Limit:      aws.Int32(1),
	})
	require.NoError(t, err)
	require.NotNil(t, page1.PaginationToken)
	staleToken := aws.ToString(page1.PaginationToken)

	_, err = sdkClient.AdminDeleteUser(t.Context(), &cognitoidpsdk.AdminDeleteUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(staleToken),
	})
	require.NoError(t, err)

	page2, err := sdkClient.ListUsers(t.Context(), &cognitoidpsdk.ListUsersInput{
		UserPoolId:      aws.String(poolID),
		Limit:           aws.Int32(3),
		PaginationToken: aws.String(staleToken),
	})
	require.NoError(t, err, "resuming with a cursor naming a deleted user must not error or hang")
	assert.Empty(t, aws.ToString(page2.PaginationToken))
	assert.Empty(t, page2.Users)
}

// TestListUserPools_Pagination_StaleCursor covers handleListUserPools's
// inline equality-scan cursor. TestListUserPools_Pagination
// (user_pools_config_test.go) already proves the boundary walk never
// drops/duplicates pools; it never presents a stale cursor, which is the
// check that finds this bug.
func TestListUserPools_Pagination_StaleCursor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	sdkClient := newTestCognitoIDPClient(t, h)

	const n = 3

	poolIDs := make([]string, n)
	for i := range n {
		out, err := sdkClient.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
			PoolName: aws.String(fmt.Sprintf("listpools-stale-%03d", i)),
		})
		require.NoError(t, err)
		poolIDs[i] = aws.ToString(out.UserPool.Id)
	}

	page1, err := sdkClient.ListUserPools(t.Context(), &cognitoidpsdk.ListUserPoolsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.NotNil(t, page1.NextToken)
	staleToken := aws.ToString(page1.NextToken)

	for _, id := range poolIDs {
		_, err = sdkClient.DeleteUserPool(t.Context(), &cognitoidpsdk.DeleteUserPoolInput{UserPoolId: aws.String(id)})
		require.NoError(t, err)
	}

	page2, err := sdkClient.ListUserPools(t.Context(), &cognitoidpsdk.ListUserPoolsInput{
		MaxResults: aws.Int32(3),
		NextToken:  aws.String(staleToken),
	})
	require.NoError(t, err, "resuming with a cursor naming a deleted pool must not error or hang")
	assert.Empty(t, aws.ToString(page2.NextToken))
	assert.Empty(t, page2.UserPools)
}

// TestListUserPools_PaginationOrderIsReproducible walks every user pool via
// NextToken-based pagination and asserts the concatenation of pages
// reproduces the full set exactly -- no drops, no duplicates. Cognito does
// not enforce unique pool names (CreateUserPool has no "already exists"
// exception -- see TestInMemoryBackend_CreateUserPool's duplicate_name case
// in user_pools_test.go), so ListUserPools' sort-by-Name (user_pools.go) can
// have genuine ties; the backing store (pools.All()) is also an
// unspecified-order map walk, so two same-named pools can swap relative
// order between the call that produced a page's NextToken and the call that
// resumes from it, even though the NextToken itself (pool ID) is unique.
func TestListUserPools_PaginationOrderIsReproducible(t *testing.T) {
	t.Parallel()

	const numPools = 16
	const pageSize = 3

	for iter := range 30 {
		h := newTestHandler(t)
		client := newTestCognitoIDPClient(t, h)
		ctx := t.Context()

		want := make(map[string]bool, numPools)

		for range numPools {
			out, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("dup-pool-name"),
			})
			require.NoErrorf(t, err, "iteration %d: setup create pool", iter)
			want[aws.ToString(out.UserPool.Id)] = true
		}

		got := make(map[string]int, numPools)

		var nextToken *string

		for page := range numPools/pageSize + 5 {
			out, err := client.ListUserPools(ctx, &cognitoidpsdk.ListUserPoolsInput{
				MaxResults: aws.Int32(pageSize),
				NextToken:  nextToken,
			})
			require.NoErrorf(t, err, "iteration %d page %d", iter, page)

			for _, p := range out.UserPools {
				got[aws.ToString(p.Id)]++
			}

			if aws.ToString(out.NextToken) == "" {
				break
			}

			nextToken = out.NextToken
		}

		for id := range want {
			assert.Equalf(t, 1, got[id], "iteration %d: pool %s expected exactly once, got %d", iter, id, got[id])
		}

		assert.Lenf(t, got, numPools, "iteration %d: total distinct pools returned", iter)
	}
}
