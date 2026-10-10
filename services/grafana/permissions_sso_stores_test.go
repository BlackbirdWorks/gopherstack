package grafana_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/grafana"
	"github.com/blackbirdworks/gopherstack/services/identitystore"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

type ssoSiblings struct {
	sso *ssoadmin.Handler
	ids *identitystore.Handler
}

func (ssoSiblings) GetIAMHandler() service.Registerable           { return nil }
func (ssoSiblings) GetEC2Handler() service.Registerable           { return nil }
func (ssoSiblings) GetOrganizationsHandler() service.Registerable { return nil }
func (s ssoSiblings) GetSsoAdminHandler() service.Registerable    { return s.sso }
func (s ssoSiblings) GetIdentityStoreHandler() service.Registerable {
	return s.ids
}
func (ssoSiblings) GetFaultStore() *chaos.FaultStore { return nil }

func TestPermissionSSOUserAcrossIdentityStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		store   string
		wantErr bool
	}{
		{name: "user_in_first_store", store: "d-1111111111"},
		{name: "user_in_second_store", store: "d-2222222222"},
		{name: "unknown_user", store: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ssoBk := ssoadmin.NewInMemoryBackend("000000000000", rtTestRegion)
			for _, store := range []string{"d-1111111111", "d-2222222222"} {
				_, err := ssoBk.CreateInstance("inst", "", store, nil)
				require.NoError(t, err)
			}

			isBk := identitystore.NewInMemoryBackend("000000000000", rtTestRegion)
			userID := "10a20b30-4c5d-4e6f-8a9b-0c1d2e3f4a5b"

			if tt.store != "" {
				u, err := isBk.CreateUser(
					t.Context(),
					tt.store,
					&identitystore.CreateUserRequest{UserName: "alice", DisplayName: "Alice"},
				)
				require.NoError(t, err)

				userID = u.UserID
			}

			backend := grafana.NewInMemoryBackend(t.Context(), "000000000000", rtTestRegion)
			t.Cleanup(backend.Close)
			backend.SetAppConfig(ssoSiblings{sso: ssoadmin.NewHandler(ssoBk), ids: identitystore.NewHandler(isBk)})

			client := newRoundTripClient(t, grafana.NewHandler(backend))
			id := createActiveWorkspace(t, client, minimalCreateWorkspaceInput())

			upd, err := client.UpdatePermissions(t.Context(), &grafanasdk.UpdatePermissionsInput{
				WorkspaceId: aws.String(id),
				UpdateInstructionBatch: []types.UpdateInstruction{{
					Action: types.UpdateActionAdd,
					Role:   types.RoleAdmin,
					Users:  []types.User{{Id: aws.String(userID), Type: types.UserTypeSsoUser}},
				}},
			})

			if tt.wantErr {
				assert.True(t, err != nil || len(upd.Errors) > 0)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, upd.Errors)
		})
	}
}
