package ocicontainerinstances_test

import (
	"testing"

	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func basicSecret(endpoint, user string) ci.CreateImagePullSecretDetails {
	return ci.CreateBasicImagePullSecretDetails{
		RegistryEndpoint: new(endpoint), Username: new(user), Password: new("pw-" + user),
	}
}

func TestImagePullSecretMatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		secrets  []ci.CreateImagePullSecretDetails
		failUser map[string]error
		wantUser []string
	}{
		{
			name:     "path prefix",
			secrets:  []ci.CreateImagePullSecretDetails{basicSecret("docker.io/library", "a")},
			wantUser: []string{"a"},
		},
		{
			name:     "other path",
			secrets:  []ci.CreateImagePullSecretDetails{basicSecret("docker.io/other", "a")},
			wantUser: []string{""},
		},
		{
			name: "tried in sequence",
			secrets: []ci.CreateImagePullSecretDetails{
				basicSecret("docker.io", "bad"), basicSecret("docker.io/library/alpine", "good"),
			},
			failUser: map[string]error{"bad": errRefused},
			wantUser: []string{"bad", "good"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := newDockerEnv(t, nil)
			d.rt.pullErrFor = tt.failUser

			resp, err := d.create(t, createOpts{mutate: func(c *ci.CreateContainerInstanceDetails) {
				c.ImagePullSecrets = tt.secrets
			}})
			require.NoError(t, err)
			d.await(t, resp.Id, ci.ContainerInstanceLifecycleStateActive)

			d.rt.mu.Lock()
			defer d.rt.mu.Unlock()

			got := make([]string, 0, len(d.rt.pulls))

			for _, p := range d.rt.pulls {
				user := ""
				if p.cred != nil {
					user = p.cred.Username
				}

				got = append(got, user)
			}

			assert.Equal(t, tt.wantUser, got)
		})
	}
}
