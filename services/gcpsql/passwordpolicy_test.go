package gcpsql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

func policySettings(p *sqladmin.PasswordValidationPolicy) *sqladmin.Settings {
	return &sqladmin.Settings{PasswordValidationPolicy: p}
}

func TestPasswordPolicyOnInsertUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   *sqladmin.PasswordValidationPolicy
		user     string
		password string
		wantCode int
	}{
		{"no policy accepts weak", nil, "alice", "x", http.StatusOK},
		{
			"disabled policy accepts weak",
			&sqladmin.PasswordValidationPolicy{MinLength: 20}, "alice", "x", http.StatusOK,
		},
		{
			"too short",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true, MinLength: 8},
			"alice", "Ab1!", http.StatusBadRequest,
		},
		{
			"long enough",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true, MinLength: 8},
			"alice", "abcdefgh", http.StatusOK,
		},
		{
			"complexity missing class",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true, Complexity: "COMPLEXITY_DEFAULT"},
			"alice", "abcdEFGH12", http.StatusBadRequest,
		},
		{
			"complexity satisfied",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true, Complexity: "COMPLEXITY_DEFAULT"},
			"alice", "abcdEFGH12!", http.StatusOK,
		},
		{
			"username substring",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true, DisallowUsernameSubstring: true},
			"alice", "xxALICExx", http.StatusBadRequest,
		},
		{
			"username substring allowed when off",
			&sqladmin.PasswordValidationPolicy{EnablePasswordPolicy: true},
			"alice", "xxALICExx", http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "p1", "POSTGRES_16", policySettings(tt.policy))

			op, err := e.svc.Users.Insert(project, "p1", &sqladmin.User{Name: tt.user, Password: tt.password}).
				Context(t.Context()).Do()
			if tt.wantCode == http.StatusOK {
				e.mustWait(t, op, err)

				return
			}

			code, _ := apiCode(t, err)
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

func TestPasswordPolicyReuseAndInterval(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	e.backend.SetClock(func() time.Time { return now })

	e.createInstance(t, "p2", "POSTGRES_16", policySettings(&sqladmin.PasswordValidationPolicy{
		EnablePasswordPolicy: true, ReuseInterval: 2, PasswordChangeInterval: "3600s",
	}))

	op, err := e.svc.Users.Insert(project, "p2", &sqladmin.User{Name: "bob", Password: "one"}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	update := func(pw string) error {
		uop, uerr := e.svc.Users.Update(project, "p2", &sqladmin.User{Name: "bob", Password: pw}).
			Name("bob").Context(t.Context()).Do()
		if uerr != nil {
			return uerr
		}

		e.mustWait(t, uop, nil)

		return nil
	}

	code, _ := apiCode(t, update("two"))
	assert.Equal(t, http.StatusBadRequest, code, "changed less than an hour ago")

	now = now.Add(2 * time.Hour)
	require.NoError(t, update("two"))

	now = now.Add(2 * time.Hour)
	code, _ = apiCode(t, update("one"))
	assert.Equal(t, http.StatusBadRequest, code, "one is among the last two passwords")

	require.NoError(t, update("three"))

	now = now.Add(2 * time.Hour)
	require.NoError(t, update("one"), "one fell out of the last two")
}

func TestPasswordPolicyBounds(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: "p3", DatabaseVersion: "POSTGRES_16", Region: "us-central1", RootPassword: "pw-root",
		Settings: &sqladmin.Settings{
			Tier: "db-custom-1-3840", PasswordValidationPolicy: &sqladmin.PasswordValidationPolicy{MinLength: 100000},
		},
	}).Context(t.Context()).Do()
	_ = op
	code, _ := apiCode(t, err)
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestPasswordHistorySurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ctx := t.Context()
	e.createInstance(t, "p4", "POSTGRES_16", policySettings(&sqladmin.PasswordValidationPolicy{
		EnablePasswordPolicy: true, ReuseInterval: 3,
	}))

	op, err := e.svc.Users.Insert(project, "p4", &sqladmin.User{Name: "carol", Password: "first"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	b := gcpsql.NewInMemoryBackend()
	b.SetLauncher(&fakeLauncher{})
	t.Cleanup(b.Close)
	require.NoError(t, b.Restore(ctx, e.backend.Snapshot(ctx)))

	_, err = b.UpdateUser(project, "p4", "carol", "", gcpsql.UserInput{Password: "first"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be reused")
}
