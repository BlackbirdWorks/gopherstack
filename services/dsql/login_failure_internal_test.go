package dsql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoginFailureMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
		user string
		want string
	}{
		{errTokenExpired, "expired", "admin", `IAM authentication failed for user "admin"`},
		{errTokenSignature, "signature", "app", `IAM authentication failed for user "app"`},
		{errTokenRole, "user token as admin", "admin", "Wrong user to action mapping. user: admin, action: DbConnect"},
		{errTokenRole, "admin token as user", "app", "Wrong user to action mapping. user: app, action: DbConnectAdmin"},
		{
			errors.Join(errTokenRole), "wrapped", "app",
			"Wrong user to action mapping. user: app, action: DbConnectAdmin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, loginFailureMessage(tt.err, tt.user))
		})
	}
}
