package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// wireDynamoDBFaultStore hands the chaos store to DynamoDB so its dynamodb:transaction-conflict rules apply.
func wireDynamoDBFaultStore(services []service.Registerable, store *chaos.FaultStore) {
	if store == nil {
		return
	}

	for _, svc := range services {
		if h, ok := svc.(interface{ SetChaosFaultStore(*chaos.FaultStore) }); ok {
			h.SetChaosFaultStore(store)
		}
	}
}
