package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQuotas(t *testing.T) {
	t.Parallel()

	const row = `<tr><td>Concurrent executions</td><td>Each supported Region: 1,000 Count</td>
<td><a href="https://console.aws.amazon.com/servicequotas/home/services/lambda/quotas/L-B99A9384">Yes</a></td>
<td>Max events.</td></tr>
<tr><td>Regional</td><td>us-east-1: 6,000<br />Each of the other supported Regions: 3,000 Kilobytes</td>
<td><a href="https://console.aws.amazon.com/servicequotas/home/services/lambda/quotas/L-A0D6E196">Yes</a></td>
<td>d</td></tr>
<tr><td>Fixed</td><td>Each supported Region: 5</td><td>No</td><td>x</td></tr>`

	got := parseQuotas(row)["lambda"]
	require.Len(t, got, 2)
	assert.Equal(
		t,
		quota{
			Code:        "L-B99A9384",
			Name:        "Concurrent executions",
			Description: "Max events.",
			Unit:        "None",
			Default:     1000,
			Adjustable:  true,
		},
		got[0],
	)
	assert.InDelta(t, 6000, got[1].Default, 0)
	assert.Equal(t, "None", got[1].Unit)
}
