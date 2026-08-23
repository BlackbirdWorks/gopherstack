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

// TestLDAP_ServiceAccountPassword_RealSDKRoundTrip drives CreateBroker
// through the real aws-sdk-go-v2 mq client with LdapServerMetadata.
// ServiceAccountPassword set (LdapServerMetadataInput.ServiceAccountPassword
// is "This member is required" in the real SDK, types.go:322). Before this
// fix, LdapServerMetadata.ServiceAccountPassword carried json:"-", which
// blocks json.Unmarshal as well as json.Marshal -- a real client's password
// was silently discarded on ingest, never reaching the backend at all. It
// must now be stored server-side (matching real AWS, which needs it to
// authenticate against the LDAP server) while still never appearing in a
// DescribeBroker response (LdapServerMetadataOutput has no such member,
// types.go:375).
func TestLDAP_ServiceAccountPassword_RealSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mq.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestMQClient(t, mq.NewHandler(backend))

	out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
		BrokerName:             aws.String("ldap-sdk-broker"),
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
			RoleSearchMatching:     aws.String("(member={0})"),
			ServiceAccountUsername: aws.String("cn=admin,dc=example,dc=com"),
			ServiceAccountPassword: aws.String("real-sdk-secret"),
			UserBase:               aws.String("ou=users,dc=example,dc=com"),
			UserSearchMatching:     aws.String("(uid={0})"),
		},
	})
	require.NoError(t, err)

	brokerID := aws.ToString(out.BrokerId)

	desc, err := client.DescribeBroker(t.Context(), &mqsdk.DescribeBrokerInput{BrokerId: out.BrokerId})
	require.NoError(t, err)
	require.NotNil(t, desc.LdapServerMetadata)
	assert.Equal(t, "cn=admin,dc=example,dc=com", aws.ToString(desc.LdapServerMetadata.ServiceAccountUsername))
	// LdapServerMetadataOutput has no ServiceAccountPassword member at all --
	// the real SDK type simply cannot carry it back, which is itself part of
	// the proof: this assertion would fail to even compile if it could leak.

	stored, err := backend.DescribeBroker(brokerID)
	require.NoError(t, err)
	require.NotNil(t, stored.LdapServerMetadata)
	assert.Equal(t, "real-sdk-secret", stored.LdapServerMetadata.ServiceAccountPassword,
		"a real client's ServiceAccountPassword must be stored on ingest, not dropped by json:\"-\"")
}
