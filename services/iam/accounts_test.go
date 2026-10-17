package iam_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	testAcctA = "111111111111"
	testAcctB = "222222222222"
	openTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}`
)

func TestForAccountIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		acct     string
		wantArn  string
		wantSame bool
	}{
		{name: "default account is the receiver", acct: iam.IAMAccountID, wantSame: true,
			wantArn: "arn:aws:iam::" + iam.IAMAccountID + ":role/x"},
		{name: "empty account is the receiver", acct: "", wantSame: true,
			wantArn: "arn:aws:iam::" + iam.IAMAccountID + ":role/x"},
		{name: "account a", acct: testAcctA, wantArn: "arn:aws:iam::" + testAcctA + ":role/x"},
		{name: "account b", acct: testAcctB, wantArn: "arn:aws:iam::" + testAcctB + ":role/x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			bk := root.ForAccount(tt.acct)
			assert.Equal(t, tt.wantSame, bk == root)

			role, err := bk.CreateRole("x", "/", openTrust, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, role.Arn)

			got, err := root.GetRoleByArn(tt.wantArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, got.Arn)
		})
	}
}

func TestListAndGetStayInAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queryAcct string
		role      string
		wantFound bool
	}{
		{name: "owner finds role", queryAcct: testAcctA, role: "only-a", wantFound: true},
		{name: "other account misses role", queryAcct: testAcctB, role: "only-a"},
		{name: "default account misses role", queryAcct: iam.IAMAccountID, role: "only-a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			_, err := root.ForAccount(testAcctA).CreateRole("only-a", "/", openTrust, "")
			require.NoError(t, err)

			_, err = root.ForAccount(testAcctB).CreateRole("b-role", "/", openTrust, "")
			require.NoError(t, err)

			_, getErr := root.ForAccount(tt.queryAcct).GetRole(tt.role)
			if tt.wantFound {
				require.NoError(t, getErr)
			} else {
				require.ErrorIs(t, getErr, iam.ErrRoleNotFound)
			}

			p, listErr := root.ForAccount(tt.queryAcct).ListRoles("", 0)
			require.NoError(t, listErr)

			for _, r := range p.Data {
				assert.Contains(t, r.Arn, ":"+tt.queryAcct+":")
			}
		})
	}
}

func TestGetRoleByArnRoutesByAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{name: "account a role", arn: "arn:aws:iam::" + testAcctA + ":role/r"},
		{name: "unknown account", arn: "arn:aws:iam::333333333333:role/r", wantErr: true},
		{name: "wrong account for name", arn: "arn:aws:iam::" + testAcctB + ":role/r", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			_, err := root.ForAccount(testAcctA).CreateRole("r", "/", openTrust, "")
			require.NoError(t, err)

			_, err = root.ForAccount(testAcctB).CreateRole("other", "/", openTrust, "")
			require.NoError(t, err)

			role, getErr := root.GetRoleByArn(tt.arn)
			if tt.wantErr {
				require.ErrorIs(t, getErr, iam.ErrRoleNotFound)

				return
			}

			require.NoError(t, getErr)
			assert.Equal(t, tt.arn, role.Arn)
		})
	}
}

func TestAccessKeyLookupFallsThroughToAccounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		acct string
	}{
		{name: "default account", acct: iam.IAMAccountID},
		{name: "account a", acct: testAcctA},
		{name: "account b", acct: testAcctB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			bk := root.ForAccount(tt.acct)

			_, err := bk.CreateUser("u", "/", "")
			require.NoError(t, err)

			key, err := bk.CreateAccessKey("u")
			require.NoError(t, err)

			arn, err := root.GetUserArnByAccessKeyID(key.AccessKeyID)
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:iam::"+tt.acct+":user/u", arn)

			user, err := root.GetUserByAccessKeyID(key.AccessKeyID)
			require.NoError(t, err)
			assert.Equal(t, "u", user.UserName)

			p, ok := root.ResolvePrincipal(t.Context(), key.AccessKeyID, "")
			require.True(t, ok)
			assert.Equal(t, tt.acct, p.AccountID)

			_, err = root.GetUserArnByAccessKeyID("AKIAUNKNOWN")
			require.ErrorIs(t, err, iam.ErrAccessKeyNotFound)
		})
	}
}

func TestAccountsSnapshotRoundTripAndReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		accts []string
	}{
		{name: "one extra account", accts: []string{testAcctA}},
		{name: "two extra accounts", accts: []string{testAcctA, testAcctB}},
		{name: "default only", accts: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := iam.NewInMemoryBackend()
			h := iam.NewHandler(src)

			_, err := src.CreateRole("x", "/", openTrust, "")
			require.NoError(t, err)

			for _, a := range tt.accts {
				_, err = src.ForAccount(a).CreateRole("x", "/", openTrust, "")
				require.NoError(t, err)
			}

			data := h.Snapshot(t.Context())
			require.NotEmpty(t, data)

			dst := iam.NewInMemoryBackend()
			require.NoError(t, iam.NewHandler(dst).Restore(t.Context(), data))

			assert.ElementsMatch(t, tt.accts, dst.Accounts())

			for _, a := range tt.accts {
				got, getErr := dst.GetRoleByArn("arn:aws:iam::" + a + ":role/x")
				require.NoError(t, getErr)
				assert.Equal(t, "x", got.RoleName)
			}

			dst.Reset()

			for _, a := range tt.accts {
				_, getErr := dst.GetRoleByArn("arn:aws:iam::" + a + ":role/x")
				require.ErrorIs(t, getErr, iam.ErrRoleNotFound)
			}
		})
	}
}

func TestPoliciesForPrincipalArn(t *testing.T) {
	t.Parallel()

	const doc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`

	tests := []struct {
		name    string
		arn     string
		wantLen int
		wantErr bool
	}{
		{name: "user in account b", arn: "arn:aws:iam::" + testAcctB + ":user/u", wantLen: 2},
		{name: "role session in account b", arn: "arn:aws:sts::" + testAcctB + ":assumed-role/r/sess", wantLen: 1},
		{name: "role arn in account b", arn: "arn:aws:iam::" + testAcctB + ":role/r", wantLen: 1},
		{name: "user name only exists in b", arn: "arn:aws:iam::" + testAcctA + ":user/u", wantErr: true},
		{name: "unknown account", arn: "arn:aws:iam::333333333333:user/u", wantErr: true},
		{name: "default account lacks user", arn: "arn:aws:iam::" + iam.IAMAccountID + ":user/u", wantErr: true},
		{name: "not a principal", arn: "arn:aws:iam::" + testAcctB + ":policy/p", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			b := root.ForAccount(testAcctB)
			root.ForAccount(testAcctA)

			_, err := b.CreateUser("u", "/", "")
			require.NoError(t, err)
			require.NoError(t, b.PutUserPolicy("u", "inline", doc))

			remote, err := root.ForAccount(testAcctA).CreatePolicy("shared", "/", doc)
			require.NoError(t, err)
			require.NoError(t, b.AttachUserPolicy("u", remote.Arn))

			_, err = b.CreateRole("r", "/", openTrust, "")
			require.NoError(t, err)
			require.NoError(t, b.PutRolePolicy("r", "inline", doc))

			docs, getErr := root.GetPoliciesForPrincipalArn(tt.arn)
			if tt.wantErr {
				require.Error(t, getErr)

				return
			}

			require.NoError(t, getErr)
			assert.Len(t, docs, tt.wantLen)
		})
	}
}

func TestCrossAccountPolicyAccessDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	const (
		workers    = 8
		iterations = 300
		doc        = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	)

	tests := []struct {
		op   func(local *iam.InMemoryBackend, user, policyArn string) error
		name string
	}{
		{name: "attach and detach", op: func(local *iam.InMemoryBackend, user, policyArn string) error {
			if err := local.AttachUserPolicy(user, policyArn); err != nil {
				return err
			}

			return local.DetachUserPolicy(user, policyArn)
		}},
		{name: "attach and evaluate", op: func(local *iam.InMemoryBackend, user, policyArn string) error {
			if err := local.AttachUserPolicy(user, policyArn); err != nil {
				return err
			}

			_, err := local.GetPoliciesForUser(user)

			return err
		}},
		{name: "role attach and evaluate", op: func(local *iam.InMemoryBackend, _, policyArn string) error {
			if err := local.AttachRolePolicy("r", policyArn); err != nil {
				return err
			}

			_, err := local.GetPoliciesForRole("r")

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := iam.NewInMemoryBackend()
			a, b := root.ForAccount(testAcctA), root.ForAccount(testAcctB)

			polA, err := a.CreatePolicy("pa", "/", doc)
			require.NoError(t, err)

			polB, err := b.CreatePolicy("pb", "/", doc)
			require.NoError(t, err)

			for _, bk := range []*iam.InMemoryBackend{a, b} {
				_, err = bk.CreateUser("u", "/", "")
				require.NoError(t, err)

				_, err = bk.CreateRole("r", "/", openTrust, "")
				require.NoError(t, err)
			}

			errs := make(chan error, 2*workers)
			done := make(chan struct{})

			var wg sync.WaitGroup

			run := func(local *iam.InMemoryBackend, foreignArn string) {
				defer wg.Done()

				for range iterations {
					if opErr := tt.op(local, "u", foreignArn); opErr != nil {
						errs <- opErr

						return
					}
				}
			}

			for range workers {
				wg.Add(2)

				go run(a, polB.Arn)
				go run(b, polA.Arn)
			}

			go func() {
				wg.Wait()
				close(done)
			}()

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			select {
			case <-done:
			case <-ctx.Done():
				require.Fail(t, "cross-account attach/evaluate deadlocked")
			}

			close(errs)

			for opErr := range errs {
				assert.NoError(t, opErr)
			}
		})
	}
}
