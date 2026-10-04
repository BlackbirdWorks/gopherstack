package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestDescribeInstancesMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		match     func(*ec2.Instance, map[string]string) bool
		name      string
		ids       []string
		wantCount int
		wantTags  int
	}{
		{name: "nil matcher returns all", match: nil, wantCount: 3, wantTags: 2},
		{
			name:      "tag matcher keeps only matches",
			match:     func(_ *ec2.Instance, tags map[string]string) bool { return tags["env"] == "prod" },
			wantCount: 1,
			wantTags:  1,
		},
		{
			name:      "reject all",
			match:     func(*ec2.Instance, map[string]string) bool { return false },
			wantCount: 0,
			wantTags:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			insts, err := b.RunInstances("ami-1", "t3.micro", "", 3)
			require.NoError(t, err)
			require.NoError(t, b.CreateTags([]string{insts[0].ID}, map[string]string{"env": "prod"}))
			require.NoError(t, b.CreateTags([]string{insts[1].ID}, map[string]string{"env": "dev"}))

			got, tags := b.DescribeInstancesMatching(tt.ids, tt.match)
			assert.Len(t, got, tt.wantCount)
			assert.Len(t, tags, tt.wantTags)

			for _, inst := range got {
				inst.ImageID = "mutated"
			}

			for _, inst := range b.DescribeInstances(nil, "") {
				assert.Equal(t, "ami-1", inst.ImageID)
			}
		})
	}
}
