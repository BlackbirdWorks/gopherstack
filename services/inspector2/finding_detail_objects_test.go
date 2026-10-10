package inspector2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestBatchGetFindingDetails_NestedObjects(t *testing.T) {
	t.Parallel()

	backend, client := newRealClient(t)

	added := time.Unix(1700000000, 0)
	due := time.Unix(1700086400, 0)

	seeded, err := backend.SeedFinding(inspector2.Finding{
		CisaData:        &inspector2.CisaData{DateAdded: added, DateDue: due, Action: "patch"},
		ExploitObserved: &inspector2.ExploitObserved{FirstSeen: added, LastSeen: due},
		Evidences: []inspector2.Evidence{
			{EvidenceRule: "rule-1", EvidenceDetail: "detail-1", Severity: "HIGH"},
		},
	})
	require.NoError(t, err)

	plain, err := backend.SeedFinding(inspector2.Finding{})
	require.NoError(t, err)

	out, err := client.BatchGetFindingDetails(t.Context(), &inspector2sdk.BatchGetFindingDetailsInput{
		FindingArns: []string{seeded.FindingArn, plain.FindingArn},
	})
	require.NoError(t, err)
	require.Len(t, out.FindingDetails, 2)

	byArn := map[string]int{}
	for i, d := range out.FindingDetails {
		byArn[aws.ToString(d.FindingArn)] = i
	}

	full := out.FindingDetails[byArn[seeded.FindingArn]]
	require.NotNil(t, full.CisaData)
	assert.Equal(t, "patch", aws.ToString(full.CisaData.Action))
	assert.True(t, full.CisaData.DateAdded.Equal(added))
	assert.True(t, full.CisaData.DateDue.Equal(due))
	require.NotNil(t, full.ExploitObserved)
	assert.True(t, full.ExploitObserved.FirstSeen.Equal(added))
	require.Len(t, full.Evidences, 1)
	assert.Equal(t, "rule-1", aws.ToString(full.Evidences[0].EvidenceRule))
	assert.Equal(t, "detail-1", aws.ToString(full.Evidences[0].EvidenceDetail))
	assert.Equal(t, "HIGH", aws.ToString(full.Evidences[0].Severity))

	bare := out.FindingDetails[byArn[plain.FindingArn]]
	assert.Nil(t, bare.CisaData)
	assert.Nil(t, bare.ExploitObserved)
	assert.Empty(t, bare.Evidences)
}
