package amplify_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestDomainAssociationSubDomainsRace reproduces a race where
// GetDomainAssociation's shallow copy (cp := *da) shares da.SubDomains'
// backing array with the stored DomainAssociation. The janitor's
// advanceDomains mutates domain.SubDomains[i].Verified in place through the
// live pointer, which races any concurrent unsynchronized read of a
// previously-returned copy's SubDomains elements -- the same shape as
// eks's Cluster.VpcConfig race (053d1a7e8).
func TestDomainAssociationSubDomainsRace(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	app, err := b.CreateApp("TestApp", "", "", "", nil)
	require.NoError(t, err)

	subs := []amplify.SubDomainSetting{{Prefix: "www", BranchName: "main"}}
	_, err = b.CreateDomainAssociation(app.AppID, "example.com", subs, true, nil, "", nil)
	require.NoError(t, err)

	j := amplify.NewJanitor(b, 0)
	ctx := context.Background()

	const iterations = 2000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for range iterations {
			da, getErr := b.GetDomainAssociation(app.AppID, "example.com")
			if getErr != nil {
				continue
			}

			if len(da.SubDomains) > 0 {
				_ = da.SubDomains[0].Verified
			}
		}
	}()

	go func() {
		defer wg.Done()

		for range iterations {
			j.SweepOnce(ctx)
		}
	}()

	wg.Wait()
}
