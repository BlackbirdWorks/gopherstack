package serverlessrepo_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// uuidPattern locks in the fix for CreateCloudFormationTemplate's TemplateId
// and CreateCloudFormationChangeSet's StackId suffix, which used to collide under synctest.
var uuidPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestServerlessRepoBackend_IDs_Unique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, b *serverlessrepo.InMemoryBackend) string
		name   string
	}{
		{
			name: "cloud_formation_template",
			create: func(t *testing.T, b *serverlessrepo.InMemoryBackend) string {
				t.Helper()

				tmpl, err := b.CreateCloudFormationTemplate("my-app", "1.0.0")
				require.NoError(t, err)

				return tmpl.TemplateID
			},
		},
		{
			name: "cloud_formation_change_set_stack_id",
			create: func(t *testing.T, b *serverlessrepo.InMemoryBackend) string {
				t.Helper()

				cs, err := b.CreateCloudFormationChangeSet("my-app", "my-stack", "", "1.0.0")
				require.NoError(t, err)

				// StackID is an ARN "...:stack/{stackName}/{uuid}"; extract the
				// trailing UUID suffix.
				idx := len(cs.StackID) - uuidLen
				require.GreaterOrEqual(t, idx, 0)

				return cs.StackID[idx:]
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
				_, err := b.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
				require.NoError(t, err)

				id1 := tt.create(t, b)
				id2 := tt.create(t, b)

				assert.NotEqual(t, id1, id2, "two resources created back-to-back must get distinct IDs")
				assert.Regexp(t, uuidPattern, id1)
				assert.Regexp(t, uuidPattern, id2)
			})
		})
	}
}

const uuidLen = 36
