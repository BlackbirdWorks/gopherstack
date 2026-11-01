package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type irsaToken struct {
	Token  string `json:"token"`
	Issuer string `json:"issuer"`
}

func mintIRSAToken(t *testing.T, cluster string, body map[string]any) irsaToken {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	resp, err := testclient.New(t).Post(endpoint+"/_gopherstack/eks/"+cluster+"/serviceaccount-token",
		"application/json", bytes.NewReader(raw))
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(out))

	var tok irsaToken
	require.NoError(t, json.Unmarshal(out, &tok))

	return tok
}

func TestIntegration_EKS_IRSA(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	eksClient := createEKSClient(t)
	iamClient := createIAMClient(t)
	stsClient := createSTSClient(t)
	suffix := uuid.NewString()[:8]
	clusterName := "irsa-" + suffix
	subnetID := mustCreateEKSSubnet(t, createEC2Client(t), "172.22")

	_, err := eksClient.CreateCluster(ctx, &eks.CreateClusterInput{
		Name:               aws.String(clusterName),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/eks-role"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{SubnetIds: []string{subnetID}},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = eksClient.DeleteCluster(cleanupCtx, &eks.DeleteClusterInput{Name: aws.String(clusterName)})
	})

	require.Eventually(t, func() bool {
		out, dErr := eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(clusterName)})

		return dErr == nil && out.Cluster.Status == ekstypes.ClusterStatusActive
	}, 30*time.Second, 100*time.Millisecond)

	desc, err := eksClient.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(clusterName)})
	require.NoError(t, err)

	issuer := aws.ToString(desc.Cluster.Identity.Oidc.Issuer)
	require.Regexp(t, `^https://oidc\.eks\.[a-z0-9-]+\.amazonaws\.com/id/[0-9a-f]{32}$`, issuer)

	host := strings.TrimPrefix(issuer, "https://")
	id := issuer[strings.LastIndex(issuer, "/")+1:]

	disc, err := testclient.New(t).
		Get(endpoint + "/_gopherstack/eks/oidc/id/" + id + "/.well-known/openid-configuration")
	require.NoError(t, err)

	defer disc.Body.Close()

	var discDoc map[string]any
	require.NoError(t, json.NewDecoder(disc.Body).Decode(&discDoc))
	assert.Equal(t, issuer, discDoc["issuer"])

	provider, err := iamClient.CreateOpenIDConnectProvider(ctx, &iamsdk.CreateOpenIDConnectProviderInput{
		Url:            aws.String(issuer),
		ClientIDList:   []string{"sts.amazonaws.com"},
		ThumbprintList: []string{"9e99a48a9960b14926bb7f3b02e22da2b0ab7280"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = iamClient.DeleteOpenIDConnectProvider(cleanupCtx, &iamsdk.DeleteOpenIDConnectProviderInput{
			OpenIDConnectProviderArn: provider.OpenIDConnectProviderArn,
		})
	})

	subject := "system:serviceaccount:default:app"
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"` +
		aws.ToString(provider.OpenIDConnectProviderArn) + `"},"Action":"sts:AssumeRoleWithWebIdentity",` +
		`"Condition":{"StringEquals":{"` + host + `:sub":"` + subject + `","` + host + `:aud":"sts.amazonaws.com"}}}]}`
	roleName := "irsa-role-" + suffix

	role, err := iamClient.CreateRole(ctx, &iamsdk.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(trust),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = iamClient.DeleteRole(cleanupCtx, &iamsdk.DeleteRoleInput{RoleName: aws.String(roleName)})
	})

	assume := func(token string) (*stssdk.AssumeRoleWithWebIdentityOutput, error) {
		return stsClient.AssumeRoleWithWebIdentity(ctx, &stssdk.AssumeRoleWithWebIdentityInput{
			RoleArn:          role.Role.Arn,
			RoleSessionName:  aws.String("pod"),
			WebIdentityToken: aws.String(token),
		})
	}

	good := mintIRSAToken(t, clusterName, map[string]any{"namespace": "default", "name": "app"})
	require.Equal(t, issuer, good.Issuer)

	out, err := assume(good.Token)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sts::000000000000:assumed-role/"+roleName+"/pod", aws.ToString(out.AssumedRoleUser.Arn))
	assert.Equal(t, subject, aws.ToString(out.SubjectFromWebIdentityToken))

	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(good.Token), 0o600))

	creds, err := stscreds.NewWebIdentityRoleProvider(stsClient, aws.ToString(role.Role.Arn),
		stscreds.IdentityTokenFile(tokenFile)).Retrieve(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, creds.AccessKeyID)
	assert.NotEmpty(t, creds.SessionToken)

	parts := strings.Split(good.Token, ".")
	tampered := parts[0] + "." + parts[1] + "." + parts[2][:len(parts[2])-4] + "AAAA"
	wrongSub := mintIRSAToken(t, clusterName, map[string]any{"namespace": "default", "name": "other"})
	wrongAud := mintIRSAToken(
		t,
		clusterName,
		map[string]any{"namespace": "default", "name": "app", "audience": "other"},
	)
	short := mintIRSAToken(t, clusterName, map[string]any{"namespace": "default", "name": "app", "ttlSeconds": 1})

	tests := []struct {
		name  string
		token string
		code  string
	}{
		{name: "wrong sub", token: wrongSub.Token, code: "AccessDenied"},
		{name: "wrong aud", token: wrongAud.Token, code: "InvalidIdentityToken"},
		{name: "tampered", token: tampered, code: "InvalidIdentityToken"},
	}

	var rejected sync.WaitGroup

	rejected.Add(len(tests) + 1)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer rejected.Done()

			_, aErr := assume(tt.token)
			require.Error(t, aErr)
			assert.Contains(t, aErr.Error(), tt.code)
		})
	}

	t.Run("expired", func(t *testing.T) {
		t.Parallel()
		defer rejected.Done()

		require.Eventually(t, func() bool {
			_, aErr := assume(short.Token)

			return aErr != nil && strings.Contains(aErr.Error(), "ExpiredToken")
		}, 10*time.Second, 250*time.Millisecond)
	})

	t.Run("unregistered provider", func(t *testing.T) {
		t.Parallel()
		rejected.Wait()

		_, dErr := iamClient.DeleteOpenIDConnectProvider(ctx, &iamsdk.DeleteOpenIDConnectProviderInput{
			OpenIDConnectProviderArn: provider.OpenIDConnectProviderArn,
		})
		require.NoError(t, dErr)

		_, aErr := assume(good.Token)
		require.Error(t, aErr)
		assert.Contains(t, aErr.Error(), "InvalidIdentityToken")
	})
}
