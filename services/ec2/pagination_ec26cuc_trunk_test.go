package ec2_test

// DescribeTrunkInterfaceAssociations defines MaxResults/NextToken on its real
// SDK input (api_op_DescribeTrunkInterfaceAssociations.go) but the handler
// ignored both, always returning every association in one page with no
// NextToken. Found during the gopherstack-6cuc pass while auditing
// handler_trunk_enclave.go.

import (
	"fmt"
	"testing"

	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/require"
)

const trunk6cucSeedCount = 3

func TestDescribeTrunkInterfaceAssociations_Pagination(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	for i := range trunk6cucSeedCount {
		branch, err := b.CreateNetworkInterface("subnet-default", fmt.Sprintf("branch-%d", i))
		require.NoError(t, err)
		trunkENI, err := b.CreateNetworkInterface("subnet-default", fmt.Sprintf("trunk-%d", i))
		require.NoError(t, err)

		_, err = b.AssociateTrunkInterface(branch.ID, trunkENI.ID, int32(i+1), 0, nil)
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewDescribeTrunkInterfaceAssociationsPaginator(
		client, &ec2sdk.DescribeTrunkInterfaceAssociationsInput{},
		func(o *ec2sdk.DescribeTrunkInterfaceAssociationsPaginatorOptions) {
			o.Limit = 1
		},
	)

	seen := make(map[string]bool, trunk6cucSeedCount)
	pages := 0

	for paginator.HasMorePages() {
		require.Lessf(t, pages, 10, "paginator did not terminate")

		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		for _, a := range out.InterfaceAssociations {
			id := *a.AssociationId
			require.Falsef(t, seen[id], "association %q returned on more than one page", id)
			seen[id] = true
		}

		pages++
	}

	require.Len(t, seen, trunk6cucSeedCount)
	require.GreaterOrEqualf(
		t, pages, trunk6cucSeedCount,
		"expected MaxResults=1 to split %d associations across pages", trunk6cucSeedCount,
	)
}
