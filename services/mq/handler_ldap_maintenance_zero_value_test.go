package mq_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// TestLdapServerMetadata_RequiredEmptyStrings_RealClient drives CreateBroker
// through the real SDK client with LdapServerMetadataInput.RoleSearchMatching
// (and UserSearchMatching) set to legitimate empty strings. Both are *string
// on the real wire -- the client-side required check (mq@v1.39.4
// validators.go:564-589) only rejects nil, not an empty string -- so a
// conformant client can send "" and still have the field round-trip in
// DescribeBroker's LdapServerMetadataOutput (types.go:375-425, both "This
// member is required"). The pre-fix domain struct tagged them `omitempty`,
// silently dropping the empty value instead of echoing it --
// gopherstack-mven required-output nested-domain-struct sweep.
func TestLdapServerMetadata_RequiredEmptyStrings_RealClient(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMQClient(t, mq.NewHandler(backend))
	ctx := t.Context()

	out, err := client.CreateBroker(ctx, &mqsdk.CreateBrokerInput{
		BrokerName:             aws.String("ldap-zero-value-broker"),
		EngineType:             mqtypes.EngineTypeActivemq,
		EngineVersion:          aws.String("5.17.6"),
		HostInstanceType:       aws.String("mq.t3.micro"),
		DeploymentMode:         mqtypes.DeploymentModeSingleInstance,
		PubliclyAccessible:     aws.Bool(false),
		AuthenticationStrategy: mqtypes.AuthenticationStrategyLdap,
		Users: []mqtypes.User{
			{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
		},
		LdapServerMetadata: &mqtypes.LdapServerMetadataInput{
			Hosts:                  []string{"ldap.example.com:389"},
			RoleBase:               aws.String("ou=roles,dc=example,dc=com"),
			RoleSearchMatching:     aws.String(""),
			ServiceAccountUsername: aws.String("cn=admin,dc=example,dc=com"),
			ServiceAccountPassword: aws.String("secret"),
			UserBase:               aws.String("ou=users,dc=example,dc=com"),
			UserSearchMatching:     aws.String(""),
		},
		MaintenanceWindowStartTime: &mqtypes.WeeklyStartTime{
			DayOfWeek: mqtypes.DayOfWeekMonday,
			TimeOfDay: aws.String(""),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeBroker(ctx, &mqsdk.DescribeBrokerInput{BrokerId: out.BrokerId})
	require.NoError(t, err)

	require.NotNil(t, desc.LdapServerMetadata)
	require.NotNil(t, desc.LdapServerMetadata.RoleSearchMatching,
		"RoleSearchMatching is required and must round-trip even when empty")
	assert.Empty(t, aws.ToString(desc.LdapServerMetadata.RoleSearchMatching))
	require.NotNil(t, desc.LdapServerMetadata.UserSearchMatching)
	assert.Empty(t, aws.ToString(desc.LdapServerMetadata.UserSearchMatching))

	require.NotNil(t, desc.MaintenanceWindowStartTime)
	require.NotNil(t, desc.MaintenanceWindowStartTime.TimeOfDay,
		"TimeOfDay is required and must round-trip even when empty")
	assert.Empty(t, aws.ToString(desc.MaintenanceWindowStartTime.TimeOfDay))
}
