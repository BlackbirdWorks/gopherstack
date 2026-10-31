package ociartifacts_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testKey        = "ocid1.key.oc1.iad.abcdefghijklmnop"
	testKeyVersion = "ocid1.keyversion.oc1.iad.qrstuvwxyz0123"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func (e *env) pushed(t *testing.T) artifacts.ContainerImageSummary {
	t.Helper()

	e.createRepo(t, "signed", nil)
	e.registry(t).PushImage(testNamespace+"/signed", "v1")

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	return images[0]
}

func sigDetails(imageID *string) artifacts.CreateContainerImageSignatureDetails {
	return artifacts.CreateContainerImageSignatureDetails{
		CompartmentId: new(testCompartment), ImageId: imageID, KmsKeyId: new(testKey),
		KmsKeyVersionId: new(testKeyVersion), Message: new(b64("payload")), Signature: new(b64("signature")),
		SigningAlgorithm: artifacts.CreateContainerImageSignatureDetailsSigningAlgorithm256RsaPkcsPss,
	}
}

func TestCreateImageSignatureValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(d *artifacts.CreateContainerImageSignatureDetails)
		wantCode string
		wantHTTP int
	}{
		{name: "valid"},
		{name: "sha224", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.SigningAlgorithm = artifacts.CreateContainerImageSignatureDetailsSigningAlgorithm224RsaPkcsPss
		}},
		{name: "sha512", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.SigningAlgorithm = artifacts.CreateContainerImageSignatureDetailsSigningAlgorithm512RsaPkcsPss
		}},
		{name: "empty algorithm", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.SigningAlgorithm = ""
		}, wantCode: "MissingParameter", wantHTTP: http.StatusBadRequest},
		{name: "message not base64", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.Message = new("!!!")
		}, wantCode: "InvalidParameter", wantHTTP: http.StatusBadRequest},
		{name: "signature not base64", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.Signature = new("***")
		}, wantCode: "InvalidParameter", wantHTTP: http.StatusBadRequest},
		{name: "no key", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.KmsKeyId = new("")
		}, wantCode: "MissingParameter", wantHTTP: http.StatusBadRequest},
		{name: "unknown image", mutate: func(d *artifacts.CreateContainerImageSignatureDetails) {
			d.ImageId = new("ocid1.containerimage.oc1..nope")
		}, wantCode: "NotAuthorizedOrNotFound", wantHTTP: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			img := e.pushed(t)

			d := sigDetails(img.Id)
			if tt.mutate != nil {
				tt.mutate(&d)
			}

			resp, err := e.client.CreateContainerImageSignature(t.Context(),
				artifacts.CreateContainerImageSignatureRequest{CreateContainerImageSignatureDetails: d})
			if tt.wantCode != "" {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantCode, se.GetCode())
				assert.Equal(t, tt.wantHTTP, se.GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, artifacts.ContainerImageSignatureLifecycleStateAvailable, resp.LifecycleState)
			assert.Equal(t, *img.Id, *resp.ImageId)
			assert.Equal(t, string(d.SigningAlgorithm), string(resp.SigningAlgorithm))
			assert.Contains(t, *resp.DisplayName, "::"+string(d.SigningAlgorithm)+"::")
			assert.Contains(t, *resp.DisplayName, "cdefghijklmnop"[len("cdefghijklmnop")-10:])
		})
	}
}

func TestImageSignatureLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	img := e.pushed(t)

	created, err := e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: sigDetails(img.Id)})
	require.NoError(t, err)
	assert.Equal(t, testUser, *created.CreatedBy)

	// The same signature again is a conflict.
	_, err = e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: sigDetails(img.Id)})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	other := sigDetails(img.Id)
	other.KmsKeyVersionId = new("ocid1.keyversion.oc1.iad.second0000000")
	second, err := e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: other})
	require.NoError(t, err)

	got, err := e.client.GetContainerImageSignature(t.Context(), artifacts.GetContainerImageSignatureRequest{
		ImageSignatureId: created.Id})
	require.NoError(t, err)
	assert.Equal(t, b64("signature"), *got.Signature)

	upd, err := e.client.UpdateContainerImageSignature(t.Context(), artifacts.UpdateContainerImageSignatureRequest{
		ImageSignatureId: created.Id,
		FreeformTags:     map[string]string{"verified": "yes"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"verified": "yes"}, upd.FreeformTags)

	_, err = e.client.DeleteContainerImageSignature(t.Context(), artifacts.DeleteContainerImageSignatureRequest{
		ImageSignatureId: created.Id, IfMatch: new("stale")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteContainerImageSignature(t.Context(), artifacts.DeleteContainerImageSignatureRequest{
		ImageSignatureId: created.Id})
	require.NoError(t, err)

	_, err = e.client.GetContainerImageSignature(
		t.Context(),
		artifacts.GetContainerImageSignatureRequest{ImageSignatureId: created.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	// Deleting the repository drops the remaining signature with its image.
	repos, err := e.client.ListContainerRepositories(
		t.Context(),
		artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, repos.Items, 1)

	_, err = e.client.DeleteContainerRepository(
		t.Context(),
		artifacts.DeleteContainerRepositoryRequest{RepositoryId: repos.Items[0].Id},
	)
	require.NoError(t, err)

	_, err = e.client.GetContainerImageSignature(
		t.Context(),
		artifacts.GetContainerImageSignatureRequest{ImageSignatureId: second.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCreateImageSignatureRawAlgorithm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		alg  string
	}{
		{"unsupported", "SHA_256_RSA_PKCS1"},
		{"lowercase", "sha_256_rsa_pkcs_pss"},
		{"ecdsa", "SHA_256_ECDSA"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			img := e.pushed(t)

			body := jsonBody(t, map[string]string{
				"compartmentId":    testCompartment,
				"imageId":          *img.Id,
				"kmsKeyId":         testKey,
				"kmsKeyVersionId":  testKeyVersion,
				"message":          b64("m"),
				"signature":        b64("s"),
				"signingAlgorithm": tt.alg,
			})
			status, out := rawDo(t, http.MethodPost, e.base+"/20160918/container/imageSignatures", body)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, out, "InvalidParameter")
		})
	}
}

func TestListImageSignatures(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	img := e.pushed(t)

	_, err := e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: sigDetails(img.Id)})
	require.NoError(t, err)

	other := sigDetails(img.Id)
	other.KmsKeyVersionId = new("ocid1.keyversion.oc1.iad.second0000000")
	second, err := e.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: other})
	require.NoError(t, err)

	listTests := []struct {
		mut  func(*artifacts.ListContainerImageSignaturesRequest)
		name string
		want int
	}{
		{nil, "all", 2},
		{func(r *artifacts.ListContainerImageSignaturesRequest) { r.ImageId = img.Id }, "image id", 2},
		{
			func(r *artifacts.ListContainerImageSignaturesRequest) { r.ImageId = new("ocid1.containerimage.oc1..x") },
			"other image",
			0,
		},
		{
			func(r *artifacts.ListContainerImageSignaturesRequest) { r.KmsKeyVersionId = new(testKeyVersion) },
			"key version",
			1,
		},
		{
			func(r *artifacts.ListContainerImageSignaturesRequest) { r.RepositoryName = new("sig*") },
			"repository name",
			2,
		},
		{
			func(r *artifacts.ListContainerImageSignaturesRequest) { r.RepositoryName = new("zzz") },
			"repository name miss",
			0,
		},
		{func(r *artifacts.ListContainerImageSignaturesRequest) { r.ImageDigest = img.Digest }, "digest", 2},
		{func(r *artifacts.ListContainerImageSignaturesRequest) {
			r.SigningAlgorithm = artifacts.ListContainerImageSignaturesSigningAlgorithm256RsaPkcsPss
		}, "algorithm", 2},
		{func(r *artifacts.ListContainerImageSignaturesRequest) {
			r.SigningAlgorithm = artifacts.ListContainerImageSignaturesSigningAlgorithm512RsaPkcsPss
		}, "algorithm miss", 0},
		{
			func(r *artifacts.ListContainerImageSignaturesRequest) { r.DisplayName = second.DisplayName },
			"display name",
			1,
		},
	}

	for _, tt := range listTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := artifacts.ListContainerImageSignaturesRequest{CompartmentId: new(testCompartment)}
			if tt.mut != nil {
				tt.mut(&r)
			}

			resp, listErr := e.client.ListContainerImageSignatures(t.Context(), r)
			require.NoError(t, listErr)
			assert.Len(t, resp.Items, tt.want)
		})
	}
}
