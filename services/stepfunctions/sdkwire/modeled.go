package sdkwire

import (
	"embed"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/schemair"
)

//go:embed models/*.json.gz
var modeledSchemas embed.FS

// modeledService builds the Service for key from its embedded schema graph,
// so the SDK schemas package is never linked.
func modeledService(key, signing, userAgentAPI string, protocol protocolFor) *Service {
	f, err := modeledSchemas.Open("models/" + key + ".json.gz")
	if err != nil {
		panic(fmt.Sprintf("sdkwire: no schema model for %s: %v", key, err))
	}

	defer f.Close()

	m, err := schemair.Read(f)
	if err != nil {
		panic(fmt.Sprintf("sdkwire: model %s: %v", key, err))
	}

	built, err := m.Build()
	if err != nil {
		panic(fmt.Sprintf("sdkwire: model %s: %v", key, err))
	}

	ops := make(map[string]*Op, len(built.Ops))
	for name, op := range built.Ops {
		ops[name] = &Op{Schema: op.Schema, Token: op.Token}
	}

	return &Service{
		Protocol: protocol(built.Schema), Ops: ops,
		SigningName: signing, UserAgentAPI: userAgentAPI,
	}
}
