package apprunner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving IDs/ARNs.
type persistenceFixtureIDs struct {
	serviceArn    string
	serviceName   string
	asgArn        string
	asgName       string
	connectionArn string
	connName      string
	obsArn        string
	obsName       string
	vpcConnArn    string
	vicArn        string
	vicName       string
	domainName    string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (services, autoScalingConfigs, connections,
// observabilityConfigs, vpcConnectors, vpcIngressConnections) -- and,
// transitively, every secondary store.Index derived from them (byName on
// services/connections/vpcIngressConnections) -- plus every raw map left
// un-converted (asgByName, obsByName, customDomains, tags), so a Snapshot
// from it exercises the entire persisted surface of the backend.
//
// AutoScalingConfigurations and ObservabilityConfigurations each get a
// second revision (both under the same name) so the round-trip test can
// verify asgByName/obsByName -- which are rebuilt from the restored tables,
// not persisted directly -- come back with "latest" still resolving to the
// highest revision.
func newPersistenceTestBackend(t *testing.T) (*apprunner.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := apprunner.NewInMemoryBackend("000000000000", "us-east-1")

	svc, err := b.CreateService(apprunner.CreateServiceParams{
		Name: "svc1",
		Source: apprunner.SourceConfig{
			ImageRepository: &apprunner.ImageSource{
				ImageIdentifier:     "public.ecr.aws/x/y:latest",
				ImageRepositoryType: "ECR_PUBLIC",
			},
		},
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	_, err = b.CreateAutoScalingConfiguration("asg1", 0, 0, 0, nil)
	require.NoError(t, err)
	asg2, err := b.CreateAutoScalingConfiguration("asg1", 0, 0, 0, nil)
	require.NoError(t, err)

	conn, err := b.CreateConnection("conn1", "GITHUB", map[string]string{"team": "apprunner"})
	require.NoError(t, err)

	_, err = b.CreateObservabilityConfiguration("obs1", "AWSXRAY", nil)
	require.NoError(t, err)
	obs2, err := b.CreateObservabilityConfiguration("obs1", "AWSXRAY", nil)
	require.NoError(t, err)

	vc, err := b.CreateVpcConnector("vpcconn1", []string{"subnet-1"}, []string{"sg-1"}, nil)
	require.NoError(t, err)

	vic, err := b.CreateVpcIngressConnection("vic1", svc.ServiceArn, "vpc-1", "vpce-1", nil)
	require.NoError(t, err)

	domain, err := b.AssociateCustomDomain(svc.ServiceArn, "example.com", false)
	require.NoError(t, err)

	return b, persistenceFixtureIDs{
		serviceArn:    svc.ServiceArn,
		serviceName:   svc.ServiceName,
		asgArn:        asg2.AutoScalingConfigurationArn,
		asgName:       asg2.AutoScalingConfigurationName,
		connectionArn: conn.ConnectionArn,
		connName:      conn.ConnectionName,
		obsArn:        obs2.ObservabilityConfigurationArn,
		obsName:       obs2.ObservabilityConfigurationName,
		vpcConnArn:    vc.VpcConnectorArn,
		vicArn:        vic.VpcIngressConnectionArn,
		vicName:       vic.VpcIngressConnectionName,
		domainName:    domain.DomainName,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (services, autoScalingConfigs, connections,
// observabilityConfigs, vpcConnectors, vpcIngressConnections), every
// secondary store.Index derived from them (byName on services/connections/
// vpcIngressConnections), and every raw map left un-converted (asgByName,
// obsByName, customDomains, tags) through Snapshot -> Restore into a fresh
// backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// services table + byName index (exercised by the duplicate-name check).
	svc, err := fresh.DescribeService(ids.serviceArn)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceName, svc.ServiceName)

	_, err = fresh.CreateService(apprunner.CreateServiceParams{
		Name: ids.serviceName,
		Source: apprunner.SourceConfig{
			ImageRepository: &apprunner.ImageSource{ImageIdentifier: "img", ImageRepositoryType: "ECR_PUBLIC"},
		},
	})
	require.ErrorIs(t, err, apprunner.ErrAlreadyExists)

	// autoScalingConfigs table + asgByName raw map (rebuilt on Restore, not
	// persisted directly -- verify "latest" still resolves to revision 2).
	asg, err := fresh.DescribeAutoScalingConfiguration(ids.asgArn)
	require.NoError(t, err)
	assert.Equal(t, int32(2), asg.AutoScalingConfigurationRevision)

	latest, _, err := fresh.ListAutoScalingConfigurations(ids.asgName, true, 0, "")
	require.NoError(t, err)
	require.Len(t, latest, 1)
	assert.Equal(t, ids.asgArn, latest[0].AutoScalingConfigurationArn)

	all, _, err := fresh.ListAutoScalingConfigurations(ids.asgName, false, 0, "")
	require.NoError(t, err)
	assert.Len(t, all, 2)

	// connections table + connByName index.
	conns, _, err := fresh.ListConnections(ids.connName, 0, "")
	require.NoError(t, err)
	require.Len(t, conns, 1)
	assert.Equal(t, ids.connectionArn, conns[0].ConnectionArn)

	_, err = fresh.CreateConnection(ids.connName, "GITHUB", nil)
	require.ErrorIs(t, err, apprunner.ErrAlreadyExists)

	// observabilityConfigs table + obsByName raw map (rebuilt on Restore).
	obs, err := fresh.DescribeObservabilityConfiguration(ids.obsArn)
	require.NoError(t, err)
	assert.Equal(t, int32(2), obs.ObservabilityConfigurationRevision)

	latestObs, _, err := fresh.ListObservabilityConfigurations(ids.obsName, true, 0, "")
	require.NoError(t, err)
	require.Len(t, latestObs, 1)
	assert.Equal(t, ids.obsArn, latestObs[0].ObservabilityConfigurationArn)

	// vpcConnectors table.
	vc, err := fresh.DescribeVpcConnector(ids.vpcConnArn)
	require.NoError(t, err)
	assert.Equal(t, ids.vpcConnArn, vc.VpcConnectorArn)

	// vpcIngressConnections table + vicByName index.
	vic, err := fresh.DescribeVpcIngressConnection(ids.vicArn)
	require.NoError(t, err)
	assert.Equal(t, ids.vicArn, vic.VpcIngressConnectionArn)

	_, err = fresh.CreateVpcIngressConnection(ids.vicName, ids.serviceArn, "vpc-1", "vpce-1", nil)
	require.ErrorIs(t, err, apprunner.ErrAlreadyExists)

	// customDomains raw map (order-sensitive slice-map, left un-converted).
	domains, _, _, err := fresh.DescribeCustomDomains(ids.serviceArn, 0, "")
	require.NoError(t, err)
	require.Len(t, domains, 1)
	assert.Equal(t, ids.domainName, domains[0].DomainName)

	// tags raw map.
	tags, err := fresh.ListTagsForResource(ids.serviceArn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	connTags, err := fresh.ListTagsForResource(ids.connectionArn)
	require.NoError(t, err)
	assert.Equal(t, "apprunner", connTags["team"])

	// Sanity: account/region carried through construction (not part of the
	// snapshot -- see persistence.go's backendSnapshot doc comment).
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0 since it never had a version
// field) is discarded cleanly rather than partially decoded: the backend
// resets to empty state and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, apprunner.ServiceCount(b))
	// 1, not 0: the discarded-snapshot reset path re-seeds the account's
	// always-present DefaultConfiguration (see
	// ensureDefaultAutoScalingConfiguration).
	assert.Equal(t, 1, apprunner.AutoScalingConfigCount(b))
	assert.Equal(t, 0, apprunner.ConnectionCount(b))
	assert.Equal(t, 0, apprunner.ObservabilityConfigCount(b))
	assert.Equal(t, 0, apprunner.VpcConnectorCount(b))
	assert.Equal(t, 0, apprunner.VpcIngressConnectionCount(b))

	_, err = b.DescribeService(ids.serviceArn)
	require.ErrorIs(t, err, apprunner.ErrNotFound)

	_, _, _, err = b.DescribeCustomDomains(ids.serviceArn, 0, "")
	require.ErrorIs(t, err, apprunner.ErrNotFound)

	_, err = b.ListTagsForResource(ids.serviceArn)
	require.ErrorIs(t, err, apprunner.ErrNotFound) // resource itself is gone, not just its tags.
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, App Runner had no such
// delegation even though InMemoryBackend implemented both methods -- dead
// wiring, since nothing ever called them through the Handler/Persistable
// path the rest of the fleet uses (see cli.go's setupPersistence).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := apprunner.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := apprunner.NewHandler(apprunner.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	svc, err := h2.Backend.DescribeService(ids.serviceArn)
	require.NoError(t, err)
	assert.Equal(t, ids.serviceName, svc.ServiceName)
}
