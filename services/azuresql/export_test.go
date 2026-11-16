package azuresql

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Exported for the external tests.

func CreateDatabaseSQL(name, collation string) (string, error) {
	return createDatabaseSQL(name, collation)
}

func DropDatabaseSQL(name string) (string, error) { return dropDatabaseSQL(name) }

func LoginSQL(login, password string) string { return loginSQL(login, password) }

// NewDockerEngineForTest builds a dockerEngine over rt that believes container "c" is running.
func NewDockerEngineForTest(rt container.Execer) DataEngine {
	return &dockerEngine{
		rt: execOnly{rt}, mu: lockmetrics.New("azuresql.test"), ctrID: "c",
	}
}

type execOnly struct{ container.Execer }

func (execOnly) CreateAndStart(context.Context, container.Spec) (string, error) { return "", nil }
func (execOnly) StopAndRemove(context.Context, string) error                    { return nil }
