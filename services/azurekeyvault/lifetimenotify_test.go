package azurekeyvault_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestLifetimeNotificationsRecorded_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		kind       string
		action     string
		recipients []string
	}{
		{name: "key_notify", kind: "Key", action: "Notify"},
		{
			name:       "cert_email_contacts",
			kind:       "Certificate",
			action:     "EmailContacts",
			recipients: []string{"sec@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, clk := clockedServer(t)
			ctx := t.Context()

			switch tt.kind {
			case "Key":
				c := newKeysClient(t, s, "nv")
				_, err := c.CreateKey(ctx, "k", azkeys.CreateKeyParameters{Kty: new(azkeys.KeyTypeRSA)}, nil)
				require.NoError(t, err)

				_, err = c.UpdateKeyRotationPolicy(ctx, "k", azkeys.KeyRotationPolicy{
					LifetimeActions: []*azkeys.LifetimeAction{{
						Action:  &azkeys.LifetimeActionType{Type: new(azkeys.KeyRotationPolicyActionNotify)},
						Trigger: &azkeys.LifetimeActionTrigger{TimeAfterCreate: new("P1D")},
					}},
				}, nil)
				require.NoError(t, err)
			default:
				c := newCertsClient(t, s, "nv")
				_, err := c.SetContacts(ctx, azcertificates.Contacts{ContactList: []*azcertificates.Contact{
					{Email: new("sec@example.com")},
				}}, nil)
				require.NoError(t, err)

				params := selfSignedPolicy("CN=n.example.com", azcertificates.KeyTypeEC)
				params.CertificatePolicy.LifetimeActions = []*azcertificates.LifetimeAction{
					{
						Action: &azcertificates.LifetimeActionType{
							ActionType: new(azcertificates.CertificatePolicyActionEmailContacts),
						},
						Trigger: &azcertificates.LifetimeActionTrigger{LifetimePercentage: new(int32(10))},
					},
				}
				_, err = c.CreateCertificate(ctx, "c", params, nil)
				require.NoError(t, err)
			}

			clk.advance(170 * day)

			var logBuf bytes.Buffer

			lctx := logger.Save(ctx, slog.New(slog.NewTextHandler(&logBuf, nil)))
			azurekeyvault.SweepJanitor(lctx, s.handler.Backend)
			azurekeyvault.SweepJanitor(lctx, s.handler.Backend)

			got, err := s.handler.Backend.Notifications("nv")
			require.NoError(t, err)
			require.Len(t, got, 1, "second sweep must not record again")
			assert.Equal(t, tt.kind, got[0].Kind)
			assert.Equal(t, tt.action, got[0].Action)
			assert.Equal(t, tt.recipients, got[0].Recipients)
			assert.Contains(t, logBuf.String(), "would send lifetime notification")

			req, err := http.NewRequestWithContext(ctx, http.MethodGet,
				s.vaultURL("nv")+"/_gopherstack/notifications?api-version=7.5", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer x")

			resp, err := s.srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}
