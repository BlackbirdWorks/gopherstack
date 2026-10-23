package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// cosmosDevKey is the Cosmos DB emulator's public, well-known master key.
const cosmosDevKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="

func ignoreConflict(err error) error {
	var re *azcore.ResponseError
	if errors.As(err, &re) && re.StatusCode == http.StatusConflict {
		return nil
	}

	return err
}

// openOrders returns the orders container, creating database and container when absent.
func openOrders(ctx context.Context) (*azcosmos.ContainerClient, error) {
	cred, err := azcosmos.NewKeyCredential(cosmosDevKey)
	if err != nil {
		return nil, err
	}

	client, err := azcosmos.NewClientWithKey(env("COSMOS_URL", "http://localhost:8081")+"/", cred, nil)
	if err != nil {
		return nil, err
	}

	_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: database}, nil)
	if err = ignoreConflict(err); err != nil {
		return nil, err
	}

	db, err := client.NewDatabase(database)
	if err != nil {
		return nil, err
	}

	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID:                     container,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/customer"}},
	}, nil)
	if err = ignoreConflict(err); err != nil {
		return nil, err
	}

	return db.NewContainer(container)
}
