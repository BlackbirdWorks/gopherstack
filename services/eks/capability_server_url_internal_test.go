package eks

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArgoCdServerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		capability string
		cluster    string
		want       string
	}{
		{
			"user_guide_example", "my-argocd", "my-cluster",
			"https://my-argocd-dc855fdf-111122223333.eks-capabilities.us-west-2.amazonaws.com",
		},
		{
			"normalized_name", "Payments_GitOps", "my-cluster",
			"https://payments-gitops-4ce0b382-111122223333.eks-capabilities.us-west-2.amazonaws.com",
		},
		{
			"truncated_name", strings.Repeat("a", 50), "c",
			"https://" + strings.Repeat("a", 41) + "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := argoCdServerURL(tt.capability, tt.cluster, "111122223333", "us-west-2")
			if tt.name == "truncated_name" {
				assert.True(t, strings.HasPrefix(got, tt.want))

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
