package azuresql

import (
	"context"
	"regexp"
)

// LaunchSpec describes the engine of one logical server.
type LaunchSpec struct {
	ID       string
	Login    string
	Password string
}

// Engine is one running SQL Server. Methods are safe for sequential use per server.
type Engine interface {
	// Endpoint is the host and published TDS port clients connect to.
	Endpoint() (string, int)
	// SetLogin creates the server-admin login, or resets its password, with sysadmin rights.
	SetLogin(ctx context.Context, login, password string) error
	CreateDatabase(ctx context.Context, name, collation string) error
	DropDatabase(ctx context.Context, name string) error
	ListDatabases(ctx context.Context) ([]string, error)
	Close(ctx context.Context)
}

// Launcher starts engines.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Engine, error)
}

const maxIdentLen = 128

var collationRe = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

func hasControl(s string) bool {
	for _, r := range s {
		if r < ' ' || r == 0x7f {
			return true
		}
	}

	return false
}

func validIdent(s string) bool {
	return s != "" && len([]rune(s)) <= maxIdentLen && !hasControl(s)
}
