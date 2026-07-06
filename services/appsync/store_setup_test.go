package appsync_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

// registryFixtureIDs carries the identifiers built by
// populateRegistryFixture, needed later to look the same resources back up
// on the restored backend.
type registryFixtureIDs struct {
	apiID       string
	mergedAPIID string
	eventAPIID  string
	dsName      string
	functionID  string
}

// populateRegistryFixture creates one instance of every store.Table-backed
// resource kind on b (see store_setup.go), plus one entry in the
// deliberately-unconverted raw apiKeys map, and returns the identifiers
// needed to look each of them back up afterward.
func populateRegistryFixture(t *testing.T, b *appsync.InMemoryBackend) registryFixtureIDs {
	t.Helper()

	api, err := b.CreateGraphqlAPI("api1", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	if err != nil {
		t.Fatalf("CreateGraphqlAPI(api1): %v", err)
	}

	mergedAPI, err := b.CreateGraphqlAPI("api2", appsync.AuthTypeAPIKey, false, "MERGED", "", nil, nil, nil)
	if err != nil {
		t.Fatalf("CreateGraphqlAPI(api2, MERGED): %v", err)
	}

	_, err = b.StartSchemaCreation(api.APIID, "type Query { hello: String }")
	if err != nil {
		t.Fatalf("StartSchemaCreation: %v", err)
	}

	ds, err := b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "ds1", Type: appsync.DataSourceTypeNone})
	if err != nil {
		t.Fatalf("CreateDataSource: %v", err)
	}

	_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{FieldName: "hello", DataSourceName: ds.Name})
	if err != nil {
		t.Fatalf("CreateResolver: %v", err)
	}

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	if err != nil {
		t.Fatalf("CreateAPICache: %v", err)
	}

	fn, err := b.CreateFunction(api.APIID, &appsync.Function{Name: "fn1", DataSourceName: ds.Name})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}

	_, err = b.CreateType(api.APIID, "type Foo { id: ID }", appsync.TypeFormatSDL)
	if err != nil {
		t.Fatalf("CreateType: %v", err)
	}

	const certARN = "arn:aws:acm:us-east-1:123456789012:certificate/abc"

	_, err = b.CreateDomainName("example.com", certARN, "desc", nil)
	if err != nil {
		t.Fatalf("CreateDomainName: %v", err)
	}

	_, err = b.AssociateAPI("example.com", api.APIID)
	if err != nil {
		t.Fatalf("AssociateAPI: %v", err)
	}

	eventAPI, err := b.CreateAPI("events1", "owner@example.com", nil, nil)
	if err != nil {
		t.Fatalf("CreateAPI: %v", err)
	}

	_, err = b.CreateChannelNamespace(eventAPI.APIID, "ns1", nil, nil)
	if err != nil {
		t.Fatalf("CreateChannelNamespace: %v", err)
	}

	_, err = b.AssociateSourceGraphqlAPI(mergedAPI.APIID, api.APIID, "merge desc", "")
	if err != nil {
		t.Fatalf("AssociateSourceGraphqlAPI: %v", err)
	}

	// Also populate the one deliberately-unconverted raw map (apiKeys) so we
	// can confirm the round trip below leaves it alone (it is not registered
	// on the registry, so RestoreRegistry never touches it).
	_, err = b.CreateAPIKey(api.APIID, "test key", 0)
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	return registryFixtureIDs{
		apiID:       api.APIID,
		mergedAPIID: mergedAPI.APIID,
		eventAPIID:  eventAPI.APIID,
		dsName:      ds.Name,
		functionID:  fn.FunctionID,
	}
}

// verifyRegistryFixture asserts that every resource populateRegistryFixture
// created is retrievable on restored with the expected wire-visible fields,
// and that the raw (non-registry) apiKeys map was NOT carried over.
func verifyRegistryFixture(t *testing.T, restored *appsync.InMemoryBackend, ids registryFixtureIDs) {
	t.Helper()

	verifyRegistryFixtureAPIs(t, restored, ids)
	verifyRegistryFixtureChildren(t, restored, ids)
	verifyRegistryFixtureFlatResources(t, restored, ids)
}

func verifyRegistryFixtureAPIs(t *testing.T, restored *appsync.InMemoryBackend, ids registryFixtureIDs) {
	t.Helper()

	gotAPI, err := restored.GetGraphqlAPI(ids.apiID)
	if err != nil {
		t.Errorf("GetGraphqlAPI: %v", err)
	} else if gotAPI.Name != "api1" {
		t.Errorf("apis: Name = %q, want api1", gotAPI.Name)
	}

	// schemas -- NOTE: Schema's unexported, non-JSON parsed-AST field is
	// therefore never expected to survive a Snapshot/Restore round trip; only
	// the wire-visible fields are checked here.
	gotSchema, err := restored.GetSchemaCreationStatus(ids.apiID)
	if err != nil {
		t.Errorf("GetSchemaCreationStatus: %v", err)
	} else if gotSchema.Status != appsync.SchemaStatusActive {
		t.Errorf("schemas: Status = %v, want %v", gotSchema.Status, appsync.SchemaStatusActive)
	}

	gotEventAPI, err := restored.GetAPI(ids.eventAPIID)
	if err != nil {
		t.Errorf("GetAPI: %v", err)
	} else if gotEventAPI.Name != "events1" {
		t.Errorf("eventAPIs: Name = %q, want events1", gotEventAPI.Name)
	}
}

func verifyRegistryFixtureChildren(t *testing.T, restored *appsync.InMemoryBackend, ids registryFixtureIDs) {
	t.Helper()

	gotDS, err := restored.GetDataSource(ids.apiID, ids.dsName)
	if err != nil {
		t.Errorf("GetDataSource: %v", err)
	} else if gotDS.APIID != ids.apiID {
		t.Errorf("datasources: APIID = %q, want %q", gotDS.APIID, ids.apiID)
	}

	gotResolver, err := restored.GetResolver(ids.apiID, "Query", "hello")
	if err != nil {
		t.Errorf("GetResolver: %v", err)
	} else if gotResolver.DataSourceName != ids.dsName {
		t.Errorf("resolvers: DataSourceName = %q, want %q", gotResolver.DataSourceName, ids.dsName)
	}

	gotFn, err := restored.GetFunction(ids.apiID, ids.functionID)
	if err != nil {
		t.Errorf("GetFunction: %v", err)
	} else if gotFn.Name != "fn1" {
		t.Errorf("functions: Name = %q, want fn1", gotFn.Name)
	}

	gotType, err := restored.GetType(ids.apiID, "Foo")
	if err != nil {
		t.Errorf("GetType: %v", err)
	} else if gotType.APIID != ids.apiID {
		t.Errorf("types: APIID = %q, want %q", gotType.APIID, ids.apiID)
	}

	gotNS, err := restored.GetChannelNamespace(ids.eventAPIID, "ns1")
	if err != nil {
		t.Errorf("GetChannelNamespace: %v", err)
	} else if gotNS.Name != "ns1" {
		t.Errorf("channelNamespaces: Name = %q, want ns1", gotNS.Name)
	}
}

func verifyRegistryFixtureFlatResources(t *testing.T, restored *appsync.InMemoryBackend, ids registryFixtureIDs) {
	t.Helper()

	gotCache, err := restored.GetAPICache(ids.apiID)
	if err != nil {
		t.Errorf("GetAPICache: %v", err)
	} else if gotCache.Type != "SMALL" {
		t.Errorf("apiCaches: Type = %q, want SMALL", gotCache.Type)
	}

	gotDomain, err := restored.GetDomainName("example.com")
	if err != nil {
		t.Errorf("GetDomainName: %v", err)
	} else if gotDomain.DomainName != "example.com" {
		t.Errorf("domainNames: DomainName = %q, want example.com", gotDomain.DomainName)
	}

	gotAssoc, err := restored.GetAPIAssociation("example.com")
	if err != nil {
		t.Errorf("GetAPIAssociation: %v", err)
	} else if gotAssoc.APIID != ids.apiID {
		t.Errorf("apiAssociations: APIID = %q, want %q", gotAssoc.APIID, ids.apiID)
	}

	assocs, err := restored.ListSourceAPIAssociations(ids.mergedAPIID)
	if err != nil {
		t.Errorf("ListSourceAPIAssociations: %v", err)
	} else if len(assocs) != 1 || assocs[0].SourceAPIID != ids.apiID {
		t.Errorf("sourceAssocs: got %+v, want one association with SourceAPIID %q", assocs, ids.apiID)
	}

	// apiKeys is deliberately NOT registered on the registry (see
	// store_setup.go), so it is untouched by RestoreRegistry -- the fresh
	// `restored` backend must NOT have inherited the api key created above.
	keys, err := restored.ListAPIKeys(ids.apiID)
	if err != nil {
		t.Errorf("ListAPIKeys: %v", err)
	} else if len(keys) != 0 {
		t.Errorf("apiKeys: got %d keys on a fresh backend, want 0 (apiKeys is not registry-backed)", len(keys))
	}
}

// Test_RegistrySnapshotRestore exercises the store.Registry round trip added
// by the Phase 3.3 pkgs/store conversion (see store_setup.go). AppSync has no
// persistence.go / Snapshot-Restore wiring of its own (unlike ec2/sqs), so
// this is the substitute proof that every converted table survives a
// SnapshotAll -> RestoreAll cycle into a fresh backend with the same
// key/value shape a real persistence layer would rely on if one is ever
// wired up for this service.
//
// This is a single sequential lifecycle test (populate -> snapshot -> restore
// -> verify), not a table of independent cases, so it does not use t.Run --
// see .claude/memories/parity-principles.md's test-isolation note: a
// sequential lifecycle asserted end-to-end in one function is the documented
// exception to the table-test convention.
func Test_RegistrySnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	ids := populateRegistryFixture(t, b)

	data, err := b.SnapshotRegistry()
	if err != nil {
		t.Fatalf("SnapshotRegistry: %v", err)
	}

	restored := newTestBackend()

	err = restored.RestoreRegistry(data)
	if err != nil {
		t.Fatalf("RestoreRegistry: %v", err)
	}

	verifyRegistryFixture(t, restored, ids)
}
