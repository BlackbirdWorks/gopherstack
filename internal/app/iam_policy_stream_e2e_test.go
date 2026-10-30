package app

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func newEnforcedIAMServer(t *testing.T) *httptest.Server {
	t.Helper()

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1", EnforceIAM: true,
		iamStream: iampolicystream.New(100)}
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func awsConfigFor(t *testing.T, url, key, secret string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
		awscfg.WithBaseEndpoint(url),
	)
	require.NoError(t, err)

	return cfg
}

type boundedUser struct{ key, secret, boundaryArn string }

func seedBoundedUser(t *testing.T, url string) boundedUser {
	t.Helper()

	ic := iam.NewFromConfig(awsConfigFor(t, url, "test", "test"))
	allowS3 := `{"Version":"2012-10-17","Statement":[{"Sid":"AllowS3","Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	boundary := `{"Version":"2012-10-17","Statement":[{"Sid":"BoundList","Effect":"Allow",` +
		`"Action":"s3:List*","Resource":"*"}]}`

	_, err := ic.CreateUser(t.Context(), &iam.CreateUserInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	_, err = ic.PutUserPolicy(t.Context(), &iam.PutUserPolicyInput{
		UserName: aws.String("alice"), PolicyName: aws.String("s3all"), PolicyDocument: aws.String(allowS3),
	})
	require.NoError(t, err)

	bp, err := ic.CreatePolicy(t.Context(), &iam.CreatePolicyInput{
		PolicyName: aws.String("list-only"), PolicyDocument: aws.String(boundary),
	})
	require.NoError(t, err)

	_, err = ic.PutUserPermissionsBoundary(t.Context(), &iam.PutUserPermissionsBoundaryInput{
		UserName: aws.String("alice"), PermissionsBoundary: bp.Policy.Arn,
	})
	require.NoError(t, err)

	k, err := ic.CreateAccessKey(t.Context(), &iam.CreateAccessKeyInput{UserName: aws.String("alice")})
	require.NoError(t, err)

	return boundedUser{*k.AccessKey.AccessKeyId, *k.AccessKey.SecretAccessKey, *bp.Policy.Arn}
}

func TestIAMPolicyStreamEndToEnd(t *testing.T) {
	t.Parallel()

	srv := newEnforcedIAMServer(t)
	u := seedBoundedUser(t, srv.URL)
	boundaryArn := u.boundaryArn

	s3c := s3.NewFromConfig(awsConfigFor(t, srv.URL, u.key, u.secret), func(o *s3.Options) { o.UsePathStyle = true })

	_, err := s3c.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err)

	_, err = s3c.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("k")})
	require.Error(t, err)

	resp, err := testclient.New(t).
		Get(srv.URL + iamPolicyStreamPath + "?principal=arn:aws:iam::000000000000:user/alice")
	require.NoError(t, err)

	defer resp.Body.Close()

	var body struct {
		Events []iampolicystream.Event `json:"events"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Len(t, body.Events, 2)

	allow, deny := body.Events[0], body.Events[1]
	assert.Equal(t, "s3:ListAllMyBuckets", allow.Action)
	assert.Equal(t, iampolicystream.DecisionAllow, allow.Decision)
	require.NotNil(t, allow.Policy)
	assert.Equal(t, iampolicystream.PolicyInline, allow.Policy.Type)
	assert.Equal(t, "s3all", allow.Policy.Name)
	assert.Equal(t, "AllowS3", allow.MatchedSid)
	require.NotNil(t, allow.Boundary)
	assert.Equal(t, "BoundList", allow.Boundary.Sid)

	assert.Equal(t, "s3:GetObject", deny.Action)
	assert.Equal(t, iampolicystream.DecisionDeny, deny.Decision)
	assert.Equal(t, iampolicystream.DenyImplicit, deny.DenyType)
	assert.Equal(t, iampolicystream.ReasonPermissionBound, deny.Reason)
	require.NotNil(t, deny.Policy)
	assert.Equal(t, boundaryArn, deny.Policy.Arn)
	require.NotNil(t, deny.Boundary)
	assert.Equal(t, boundaryArn, deny.Boundary.Arn)
	assert.False(t, deny.Boundary.Matched)
}
