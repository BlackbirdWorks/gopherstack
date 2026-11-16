package servicequotas_test

import (
	"testing"

	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

func TestTemplateAppliesToNewAccounts(t *testing.T) {
	t.Parallel()

	const member = "222222222222"

	tests := []struct {
		name      string
		account   string
		want      int
		associate bool
	}{
		{name: "associated", associate: true, account: member, want: 1},
		{name: "not associated", associate: false, account: member, want: 0},
		{name: "management account", associate: true, account: awsmeta.DefaultAccount, want: 0},
		{name: "unknown account", associate: true, account: "333333333333", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetOrgDirectory(fakeOrgs{
				awsmeta.DefaultAccount: goodOrg(),
				member:                 goodOrg(),
			})
			putItem(t, f, 50)

			if tt.associate {
				_, err := f.client.AssociateServiceQuotaTemplate(
					t.Context(), &sqsdk.AssociateServiceQuotaTemplateInput{},
				)
				require.NoError(t, err)
			}

			before := f.backend.RequestCount()
			assert.Equal(t, tt.want, f.backend.ApplyTemplateToNewAccount(tt.account))
			assert.Equal(t, before+tt.want, f.backend.RequestCount())
		})
	}
}

func TestTemplateApplyIsIdempotent(t *testing.T) {
	t.Parallel()

	const member = "222222222222"

	f := newFixture(t)
	f.backend.SetOrgDirectory(fakeOrgs{awsmeta.DefaultAccount: goodOrg(), member: goodOrg()})
	putItem(t, f, 50)

	_, err := f.client.AssociateServiceQuotaTemplate(t.Context(), &sqsdk.AssociateServiceQuotaTemplateInput{})
	require.NoError(t, err)

	assert.Equal(t, 1, f.backend.ApplyTemplateToNewAccount(member))
	assert.Equal(t, 0, f.backend.ApplyTemplateToNewAccount(member))
	assert.Equal(t, 1, f.backend.RequestCount())
}
