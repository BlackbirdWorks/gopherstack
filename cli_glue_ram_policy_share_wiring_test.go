package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
)

// TestInitializeServices_GlueRAMPolicyShareWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than calling
// wireGlueRAMPolicyShares directly, so that deleting the wiring call from
// wireGovernanceIntegrations -- not just breaking the helper function itself -- is what
// this test is sensitive to.
//
// Regression test for gopherstack-kvyy: without glue's ResourceShareCreator seam wired
// to a real RAM backend, no path in this codebase can ever create a CREATED_FROM_POLICY
// resource share, so PromoteResourceShareCreatedFromPolicy's featureSet state machine is
// unreachable. This asserts the real production composition root wires a hybrid,
// cross-account Glue resource policy into a real RAM share, and that promoting it
// actually flips featureSet to STANDARD.
func TestInitializeServices_GlueRAMPolicyShareWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	portAlloc, err := portalloc.New(19200, 19300)
	require.NoError(t, err)

	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
		PortAlloc:  portAlloc,
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	glueH, ok := byName["Glue"].(*gluebackend.Handler)
	require.True(t, ok, "Glue handler must be registered")

	ramH, ok := byName["RAM"].(*rambackend.Handler)
	require.True(t, ok, "RAM handler must be registered")

	const resourceARN = "arn:aws:glue:us-east-1:000000000000:database/wiring-db"

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":["111122223333"]},"Action":["glue:GetDatabase"]}]}`

	_, err = glueH.Backend.PutResourcePolicy(policy, resourceARN, "", "", "TRUE")
	require.NoError(t, err)

	shares := ramH.Backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1,
		"a hybrid cross-account Glue resource policy must create exactly one RAM resource share "+
			"via the real cli.go composition root's wireGlueRAMPolicyShares wiring")
	require.Equal(t, "CREATED_FROM_POLICY", shares[0].FeatureSet)
	require.Equal(t, "000000000000", shares[0].OwningAccountID)

	_, err = ramH.Backend.PromoteResourceShareCreatedFromPolicy(shares[0].ARN)
	require.NoError(t, err)

	shares = ramH.Backend.ListResourceShares("SELF", "")
	require.Len(t, shares, 1)
	require.Equal(t, "STANDARD", shares[0].FeatureSet,
		"PromoteResourceShareCreatedFromPolicy must flip the wired share to STANDARD")
}
