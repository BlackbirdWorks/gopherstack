package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	cloudhsmv2backend "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func signCloudHSMCSR(t *testing.T, csrPEM string) (string, string) {
	t.Helper()

	blk, _ := pem.Decode([]byte(csrPEM))
	require.NotNil(t, blk)

	csr, err := x509.ParseCertificateRequest(blk.Bytes)
	require.NoError(t, err)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	notBefore, notAfter := time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "root"},
		NotBefore: notBefore, NotAfter: notAfter,
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "cluster"},
		NotBefore: notBefore, NotAfter: notAfter,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, csr.PublicKey, caKey)
	require.NoError(t, err)

	enc := func(der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}

	return enc(leafDER), enc(caDER)
}

func TestInitializeServices_CloudHSMV2TagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["CloudHSMV2"].(*cloudhsmv2backend.Handler)
	require.True(t, ok)

	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	require.True(t, ok)

	ctx := awsmeta.WithRegion(t.Context(), "us-east-1")
	h.Backend.SetLifecycleDelay(time.Nanosecond)

	vpc, err := ec2H.BackendFor("us-east-1").CreateVpc("10.0.0.0/16", "")
	require.NoError(t, err)

	subnet, err := ec2H.BackendFor("us-east-1").CreateSubnet(vpc.ID, "10.0.1.0/24", "us-east-1a")
	require.NoError(t, err)

	cluster, err := h.Backend.CreateCluster(ctx, cloudhsmv2backend.CreateClusterInput{
		HsmType: "hsm1.medium", SubnetIDs: []string{subnet.ID},
	})
	require.NoError(t, err)

	_, err = h.Backend.CreateHsm(ctx, cluster.ClusterID, "us-east-1a", "")
	require.NoError(t, err)

	clusterARN := "arn:aws:cloudhsm:us-east-1:000000000000:cluster/" + cluster.ClusterID

	tagged, err := rgtH.Backend.TagResources(ctx, &rgtapibackend.TagResourcesInput{
		ResourceARNList: []string{clusterARN}, Tags: map[string]string{"Team": "emu"},
	})
	require.NoError(t, err)
	require.Empty(t, tagged.FailedResourcesMap)

	described, err := h.Backend.DescribeClusters(ctx, map[string][]string{"clusterIds": {cluster.ClusterID}}, "", nil)
	require.NoError(t, err)
	require.Len(t, described.Data, 1)
	require.NotNil(t, described.Data[0].Certificates)

	cert, anchor := signCloudHSMCSR(t, described.Data[0].Certificates.ClusterCsr)
	_, _, err = h.Backend.InitializeCluster(ctx, cluster.ClusterID, cert, anchor)
	require.NoError(t, err)

	backups, err := h.Backend.DescribeBackups(ctx, cloudhsmv2backend.DescribeBackupsInput{})
	require.NoError(t, err)
	require.Len(t, backups.Data, 1)

	backupARN := backups.Data[0].BackupARN

	listTags := func(ref string) func(context.Context) (map[string]string, error) {
		return func(ctx context.Context) (map[string]string, error) {
			p, lerr := h.Backend.ListTags(ctx, ref, "", nil)
			out := map[string]string{}

			for _, tag := range p.Data {
				out[tag.Key] = tag.Value
			}

			return out, lerr
		}
	}

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{name: "cluster", arn: clusterARN, resourceType: "cloudhsm:cluster", listTags: listTags(clusterARN)},
		{name: "backup", arn: backupARN, resourceType: "cloudhsm:backup", listTags: listTags(backupARN)},
	})
}
