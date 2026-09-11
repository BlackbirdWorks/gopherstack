package eks_test

import (
	"crypto/x509"
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eks"
)

// gopherstack-lruaw (2026-09-11): the five EKS Hybrid Nodes CertificateAuthority
// ops (Activate/Create/Delete/Describe/ListCertificateAuthorities), previously
// unimplemented -- see PARITY.md.

func TestCertificateAuthority_UnknownCluster_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*ekssdk.Client) error
		name string
	}{
		{name: "create", run: func(c *ekssdk.Client) error {
			_, err := c.CreateCertificateAuthority(t.Context(), &ekssdk.CreateCertificateAuthorityInput{
				ClusterName: aws.String("no-such-cluster"),
			})

			return err
		}},
		{name: "activate", run: func(c *ekssdk.Client) error {
			_, err := c.ActivateCertificateAuthority(t.Context(), &ekssdk.ActivateCertificateAuthorityInput{
				ClusterName: aws.String("no-such-cluster"), CertificateAuthorityId: aws.String("ca-1"),
			})

			return err
		}},
		{name: "delete", run: func(c *ekssdk.Client) error {
			_, err := c.DeleteCertificateAuthority(t.Context(), &ekssdk.DeleteCertificateAuthorityInput{
				ClusterName: aws.String("no-such-cluster"), CertificateAuthorityId: aws.String("ca-1"),
			})

			return err
		}},
		{name: "describe", run: func(c *ekssdk.Client) error {
			_, err := c.DescribeCertificateAuthority(t.Context(), &ekssdk.DescribeCertificateAuthorityInput{
				ClusterName: aws.String("no-such-cluster"), CertificateAuthorityId: aws.String("ca-1"),
			})

			return err
		}},
		{name: "list", run: func(c *ekssdk.Client) error {
			_, err := c.ListCertificateAuthorities(t.Context(), &ekssdk.ListCertificateAuthoritiesInput{
				ClusterName: aws.String("no-such-cluster"),
			})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			client := newTestEKSClient(t, h)

			err := tc.run(client)
			require.Error(t, err)

			var nf *ekstypes.ResourceNotFoundException
			require.ErrorAs(t, err, &nf)
		})
	}
}

func TestCertificateAuthority_UnknownID_ResourceNotFoundException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*ekssdk.Client, string) error
		name string
	}{
		{name: "activate", run: func(c *ekssdk.Client, cluster string) error {
			_, err := c.ActivateCertificateAuthority(t.Context(), &ekssdk.ActivateCertificateAuthorityInput{
				ClusterName: aws.String(cluster), CertificateAuthorityId: aws.String("bogus"),
			})

			return err
		}},
		{name: "delete", run: func(c *ekssdk.Client, cluster string) error {
			_, err := c.DeleteCertificateAuthority(t.Context(), &ekssdk.DeleteCertificateAuthorityInput{
				ClusterName: aws.String(cluster), CertificateAuthorityId: aws.String("bogus"),
			})

			return err
		}},
		{name: "describe", run: func(c *ekssdk.Client, cluster string) error {
			_, err := c.DescribeCertificateAuthority(t.Context(), &ekssdk.DescribeCertificateAuthorityInput{
				ClusterName: aws.String(cluster), CertificateAuthorityId: aws.String("bogus"),
			})

			return err
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			client := newTestEKSClient(t, h)
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
				Name:               aws.String("ca-unknown-id-" + tc.name),
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
				ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
			})
			require.NoError(t, err)

			err = tc.run(client, "ca-unknown-id-"+tc.name)
			require.Error(t, err)

			var nf *ekstypes.ResourceNotFoundException
			require.ErrorAs(t, err, &nf)
		})
	}
}

// TestCertificateAuthority_Create_GeneratesRealCACert drives
// CreateCertificateAuthority through the real client and verifies Data is a
// genuine, parseable, self-signed x509 CA certificate -- not a fabricated
// placeholder string -- since Create/ActivateCertificateAuthorityInput
// accept no client-supplied key/CSR/certificate for this op family (verified
// against api_op_*CertificateAuthorit*.go/types/types.go).
func TestCertificateAuthority_Create_GeneratesRealCACert(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-cert-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	out, err := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-cert-cluster"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.CertificateAuthority)
	assert.Equal(t, ekstypes.CertificateAuthoritySigningStatusNotUsed, out.CertificateAuthority.SigningStatus)
	require.NotNil(t, out.Update)
	assert.Equal(t, ekstypes.UpdateTypeCertificateAuthorityUpdate, out.Update.Type)
	assert.Equal(t, ekstypes.UpdateStatusInProgress, out.Update.Status)

	id := aws.ToString(out.CertificateAuthority.Id)

	desc, err := client.DescribeCertificateAuthority(ctx, &ekssdk.DescribeCertificateAuthorityInput{
		ClusterName: aws.String("ca-cert-cluster"), CertificateAuthorityId: aws.String(id),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.CertificateAuthority.Data)

	der, err := base64.StdEncoding.DecodeString(aws.ToString(desc.CertificateAuthority.Data))
	require.NoError(t, err, "Data must be base64-encoded")

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err, "Data must decode to a real x509 certificate")
	assert.True(t, cert.IsCA, "generated certificate must be a CA certificate")
	require.NotNil(t, desc.CertificateAuthority.Validity)
	assert.True(t, cert.NotAfter.After(cert.NotBefore))
}

func TestCertificateAuthority_Create_MaxTwoPerCluster_ResourceLimitExceededException(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-max-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	for range 2 {
		_, err = client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
			ClusterName: aws.String("ca-max-cluster"),
		})
		require.NoError(t, err)
	}

	_, err = client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-max-cluster"),
	})
	require.Error(t, err)

	var limitErr *ekstypes.ResourceLimitExceededException
	require.ErrorAs(t, err, &limitErr)
}

func TestCertificateAuthority_Activate_RequiresDistributionComplete_InvalidParameterException(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "ca-not-distributed")

	ca, _, err := b.CreateCertificateAuthority("ca-not-distributed")
	require.NoError(t, err)
	require.Equal(t, "IN_PROGRESS", ca.DistributionStatus, "must start IN_PROGRESS, not fabricated as already complete")

	_, _, err = b.ActivateCertificateAuthority("ca-not-distributed", ca.ID)
	require.ErrorIs(t, err, eks.ErrValidation)
}

// TestCertificateAuthority_Activate_Lifecycle_RetiresOutgoing drives a full
// rotation: activate the first CA (no outgoing signer yet), append a
// successor, wait for its distribution, activate it, and confirm the first
// CA is retired to NOT_USED with RollbackAvailable set -- matching
// api_op_ActivateCertificateAuthority.go's doc comment ("the outgoing CA is
// retired (NOT_USED)" and "CA rollback is available").
func TestCertificateAuthority_Activate_Lifecycle_RetiresOutgoing(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "ca-rotation")

	first, _, err := b.CreateCertificateAuthority("ca-rotation")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-rotation", first.ID)

		return descErr == nil && got.DistributionStatus == "COMPLETE"
	}, 2*time.Second, 10*time.Millisecond)

	activated, upd, err := b.ActivateCertificateAuthority("ca-rotation", first.ID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVATING", activated.SigningStatus, "must not fabricate an immediate IN_USE")
	assert.Equal(t, "InProgress", upd.Status)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-rotation", first.ID)

		return descErr == nil && got.SigningStatus == "IN_USE"
	}, 2*time.Second, 10*time.Millisecond)

	successor, _, err := b.CreateCertificateAuthority("ca-rotation")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-rotation", successor.ID)

		return descErr == nil && got.DistributionStatus == "COMPLETE"
	}, 2*time.Second, 10*time.Millisecond)

	_, _, err = b.ActivateCertificateAuthority("ca-rotation", successor.ID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-rotation", successor.ID)

		return descErr == nil && got.SigningStatus == "IN_USE"
	}, 2*time.Second, 10*time.Millisecond)

	outgoing, err := b.DescribeCertificateAuthority("ca-rotation", first.ID)
	require.NoError(t, err)
	assert.Equal(t, "NOT_USED", outgoing.SigningStatus)
	assert.True(t, outgoing.RollbackAvailable)
}

func TestCertificateAuthority_Activate_AlreadyActive_InvalidParameterException(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "ca-already-active")

	ca, _, err := b.CreateCertificateAuthority("ca-already-active")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-already-active", ca.ID)

		return descErr == nil && got.DistributionStatus == "COMPLETE"
	}, 2*time.Second, 10*time.Millisecond)

	_, _, err = b.ActivateCertificateAuthority("ca-already-active", ca.ID)
	require.NoError(t, err)

	_, _, err = b.ActivateCertificateAuthority("ca-already-active", ca.ID)
	require.ErrorIs(t, err, eks.ErrValidation, "re-activating a NOT_USED-ineligible CA must fail, not silently no-op")
}

func TestCertificateAuthority_Delete_InUse_ResourceInUseException(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "ca-delete-inuse")

	ca, _, err := b.CreateCertificateAuthority("ca-delete-inuse")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-delete-inuse", ca.ID)

		return descErr == nil && got.DistributionStatus == "COMPLETE"
	}, 2*time.Second, 10*time.Millisecond)

	_, _, err = b.ActivateCertificateAuthority("ca-delete-inuse", ca.ID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, descErr := b.DescribeCertificateAuthority("ca-delete-inuse", ca.ID)

		return descErr == nil && got.SigningStatus == "IN_USE"
	}, 2*time.Second, 10*time.Millisecond)

	_, _, err = b.DeleteCertificateAuthority("ca-delete-inuse", ca.ID)
	require.ErrorIs(t, err, eks.ErrAlreadyExists)
}

func TestCertificateAuthority_Delete_Success(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "ca-delete-ok")

	ca, _, err := b.CreateCertificateAuthority("ca-delete-ok")
	require.NoError(t, err)

	deleted, upd, err := b.DeleteCertificateAuthority("ca-delete-ok", ca.ID)
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.DistributionStatus)
	assert.Equal(t, "CertificateAuthorityUpdate", upd.Type)

	_, err = b.DescribeCertificateAuthority("ca-delete-ok", ca.ID)
	require.ErrorIs(t, err, eks.ErrNotFound)
}

func TestCertificateAuthority_List_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-list-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	ids := make(map[string]bool)

	for range 2 {
		out, createErr := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
			ClusterName: aws.String("ca-list-cluster"),
		})
		require.NoError(t, createErr)
		ids[aws.ToString(out.CertificateAuthority.Id)] = true
	}

	one := int32(1)

	page1, err := client.ListCertificateAuthorities(ctx, &ekssdk.ListCertificateAuthoritiesInput{
		ClusterName: aws.String("ca-list-cluster"), MaxResults: &one,
	})
	require.NoError(t, err)
	require.Len(t, page1.CertificateAuthorities, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListCertificateAuthorities(ctx, &ekssdk.ListCertificateAuthoritiesInput{
		ClusterName: aws.String("ca-list-cluster"), MaxResults: &one, NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.CertificateAuthorities, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{
		aws.ToString(page1.CertificateAuthorities[0].Id): true,
		aws.ToString(page2.CertificateAuthorities[0].Id): true,
	}
	assert.Equal(t, ids, seen)
}

// TestCertificateAuthority_ClientRequestToken_Create_Idempotent drives a
// same-token replay through the real client per api_op_CreateCertificateAuthority.go's
// doc comment.
func TestCertificateAuthority_ClientRequestToken_Create_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-idem-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	token := "fixed-token-1"

	first, err := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-cluster"), ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err)

	second, err := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-cluster"), ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.CertificateAuthority.Id), aws.ToString(second.CertificateAuthority.Id))

	list, err := client.ListCertificateAuthorities(ctx, &ekssdk.ListCertificateAuthoritiesInput{
		ClusterName: aws.String("ca-idem-cluster"),
	})
	require.NoError(t, err)
	assert.Len(t, list.CertificateAuthorities, 1, "a replayed token must not create a second resource")
}

// TestCertificateAuthority_ClientRequestToken_Create_DifferentCluster_Rejected
// proves the route-identity fingerprint fix in
// certificateAuthorityIdempotencyFingerprintBody. CreateCertificateAuthorityInput's
// body carries ONLY clientRequestToken (verified against serializers.go), so
// naively fingerprinting the raw body would canonicalize to "{}" regardless
// of clusterName and INCORRECTLY REPLAY cluster A's CreateCertificateAuthority
// response for cluster B's entirely different request -- silently returning
// a certificate authority that does not exist in cluster B at all, and never
// calling the backend for cluster B's request. Folding clusterName into the
// fingerprint makes this the same "different parameters" InvalidParameterException
// every other eks op's withIdempotency call already returns for a token
// reused with a genuinely different request (idempotency.go) -- not a silent
// misapplied replay, and not a silently-allowed fresh create either.
func TestCertificateAuthority_ClientRequestToken_Create_DifferentCluster_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	for _, name := range []string{"ca-idem-cluster-a", "ca-idem-cluster-b"} {
		_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
			Name:               aws.String(name),
			RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
			ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
		})
		require.NoError(t, err)
	}

	token := "shared-token"

	_, err := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-cluster-a"), ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err)

	_, err = client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-cluster-b"), ClientRequestToken: aws.String(token),
	})
	require.Error(t, err, "a token reused against a different cluster must not silently replay cluster A's resource")

	var invalidErr *ekstypes.InvalidParameterException
	require.ErrorAs(t, err, &invalidErr)

	listB, err := client.ListCertificateAuthorities(ctx, &ekssdk.ListCertificateAuthoritiesInput{
		ClusterName: aws.String("ca-idem-cluster-b"),
	})
	require.NoError(t, err)
	assert.Empty(t, listB.CertificateAuthorities, "the rejected request must not have created anything in cluster B")
}

// TestCertificateAuthority_ClientRequestToken_Delete_QueryParam_Idempotent
// exercises DeleteCertificateAuthority's ClientRequestToken through the real
// client -- the only op in this service where the SDK serializes that field
// as a query parameter rather than a JSON body (serializers.go's
// awsRestjson1_serializeOpHttpBindingsDeleteCertificateAuthorityInput uses
// encoder.SetQuery, not the body encoder).
func TestCertificateAuthority_ClientRequestToken_Delete_QueryParam_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-idem-delete-cluster"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	out, err := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-delete-cluster"),
	})
	require.NoError(t, err)

	id := aws.ToString(out.CertificateAuthority.Id)
	token := "delete-token-1"

	_, err = client.DeleteCertificateAuthority(ctx, &ekssdk.DeleteCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-delete-cluster"), CertificateAuthorityId: aws.String(id),
		ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err)

	// A real, non-idempotent retry would 404 here (the CA is already gone).
	// The same ClientRequestToken must instead replay the original success.
	_, err = client.DeleteCertificateAuthority(ctx, &ekssdk.DeleteCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-delete-cluster"), CertificateAuthorityId: aws.String(id),
		ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err, "a same-token retry after success must replay, not 404")
}

// TestCertificateAuthority_ClientRequestToken_Delete_DifferentID_Rejected
// proves the same route-identity fingerprint fix for Delete: two distinct
// certificate authorities in the same cluster, deleted with the same reused
// token, must not have the second delete silently replay the first's
// response (which would leave the second certificate authority undeleted
// while falsely reporting success). The second call is instead rejected as
// "different parameters," matching every other eks op's withIdempotency
// contract -- and the first delete's own result is unaffected.
func TestCertificateAuthority_ClientRequestToken_Delete_DifferentID_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	client := newTestEKSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
		Name:               aws.String("ca-idem-delete-two"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
		ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
	})
	require.NoError(t, err)

	ids := make([]string, 0, 2)

	for range 2 {
		out, createErr := client.CreateCertificateAuthority(ctx, &ekssdk.CreateCertificateAuthorityInput{
			ClusterName: aws.String("ca-idem-delete-two"),
		})
		require.NoError(t, createErr)
		ids = append(ids, aws.ToString(out.CertificateAuthority.Id))
	}

	token := "shared-delete-token"

	_, err = client.DeleteCertificateAuthority(ctx, &ekssdk.DeleteCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-delete-two"), CertificateAuthorityId: aws.String(ids[0]),
		ClientRequestToken: aws.String(token),
	})
	require.NoError(t, err)

	_, err = client.DeleteCertificateAuthority(ctx, &ekssdk.DeleteCertificateAuthorityInput{
		ClusterName: aws.String("ca-idem-delete-two"), CertificateAuthorityId: aws.String(ids[1]),
		ClientRequestToken: aws.String(token),
	})
	require.Error(t, err, "reusing the token against a different certificate authority must not silently replay")

	var invalidErr *ekstypes.InvalidParameterException
	require.ErrorAs(t, err, &invalidErr)

	list, err := client.ListCertificateAuthorities(ctx, &ekssdk.ListCertificateAuthoritiesInput{
		ClusterName: aws.String("ca-idem-delete-two"),
	})
	require.NoError(t, err)
	require.Len(t, list.CertificateAuthorities, 1, "the second (rejected) delete must not have removed anything")
	assert.Equal(t, ids[1], aws.ToString(list.CertificateAuthorities[0].Id), "the undeleted CA must be the second one")
}

func TestCertificateAuthority_Describe_ViaRawREST(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "ca-raw-cluster", "roleArn": "arn:aws:iam::123456789012:role/eks",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doREST(t, h, http.MethodPost, "/clusters/ca-raw-cluster/certificate-authorities", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	created := parseResp(t, rec)
	ca, ok := created["certificateAuthority"].(map[string]any)
	require.True(t, ok)
	id, _ := ca["id"].(string)
	require.NotEmpty(t, id)

	rec = doREST(t, h, http.MethodGet, "/clusters/ca-raw-cluster/certificate-authorities/"+id, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	desc := parseResp(t, rec)
	descCA, ok := desc["certificateAuthority"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, id, descCA["id"])
	assert.NotEmpty(t, descCA["data"])
	assert.Contains(t, descCA, "validity")
}
