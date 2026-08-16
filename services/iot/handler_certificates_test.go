package iot_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestBatch2_CACertificate tests CA certificate lifecycle.
func TestCACertificate(t *testing.T) {
	t.Parallel()
	h := newIoTHandler(t)

	// Register
	out := iotOK(t, h, http.MethodPost, "/cacertificate/register", map[string]any{
		"caCertificate": "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
	})
	certID, _ := out["certificateId"].(string)
	if certID == "" {
		t.Fatalf("expected certificateId: %v", out)
	}

	// Describe
	out2 := iotOK(t, h, http.MethodGet, "/cacertificates/"+certID, nil)
	desc := out2["certificateDescription"].(map[string]any)
	if desc["certificateId"] != certID {
		t.Errorf("describe mismatch: %v", desc)
	}

	// List
	out3 := iotOK(t, h, http.MethodGet, "/cacertificates", nil)
	certs, _ := out3["certificates"].([]any)
	if len(certs) != 1 {
		t.Errorf("expected 1 CA cert, got %d", len(certs))
	}

	// Update
	iotOK(t, h, http.MethodPut, "/cacertificates/"+certID, map[string]any{
		"newStatus": "INACTIVE",
	})

	// Delete
	iotOK(t, h, http.MethodDelete, "/cacertificates/"+certID, nil)

	iotExpectError(t, h, "/cacertificates/"+certID)
}

// TestBatch3_CreateKeysAndCertificate tests CreateKeysAndCertificate.
func TestCreateKeysAndCertificate(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	out := iotOK(t, h, http.MethodPost, "/keys-and-certificate?setAsActive=true", nil)
	if out["certificateId"] == "" {
		t.Errorf("expected certificateId, got %v", out)
	}
	kp, ok := out["keyPair"].(map[string]any)
	if !ok || kp["PublicKey"] == "" {
		t.Errorf("expected keyPair, got %v", out)
	}
}

// TestBatch3_TransferCertificate tests TransferCertificate and RejectCertificateTransfer.
func TestTransferCertificate(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create a cert first
	out := iotOK(t, h, http.MethodPost, "/keys-and-certificate?setAsActive=true", nil)
	certID, _ := out["certificateId"].(string)
	if certID == "" {
		t.Fatal("expected certificateId")
	}

	// Transfer
	iotOK(t, h, http.MethodPatch, "/certificates/"+certID+"/transfer?targetAwsAccount=111111111111", nil)

	// Reject
	iotOK(t, h, http.MethodPatch, "/reject-certificate-transfer/"+certID, nil)
}

// TestBatch3_ProvisioningClaim tests CreateProvisioningClaim (needs a provisioning template).
func TestProvisioningClaim(t *testing.T) {
	t.Parallel()
	h, _ := newHandlerForBatch3Test(t)

	// Create provisioning template first
	iotOK(t, h, http.MethodPost, "/provisioning-templates", map[string]any{
		"templateName":        "my-template",
		"templateBody":        `{"Parameters":{},"Resources":{}}`,
		"provisioningRoleArn": "arn:aws:iam::000000000000:role/ProvisioningRole",
	})

	// Create provisioning claim
	out := iotOK(t, h, http.MethodPost, "/provisioning-templates/my-template/provisioning-claim", nil)
	if out["certificatePem"] == "" {
		t.Errorf("expected certificatePem, got %v", out)
	}
	kp, _ := out["keyPair"].(map[string]any)
	if kp == nil {
		t.Errorf("expected keyPair, got %v", out)
	}
}

func TestCertificateIDs_AreUnique(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	seen := make(map[string]bool)

	for range 10 {
		var out map[string]string
		code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
			"certificateSigningRequest": "csr-data",
			"setAsActive":               true,
		}, &out)
		require.Equal(t, http.StatusOK, code)
		id := out["certificateId"]
		assert.NotEmpty(t, id)
		assert.False(t, seen[id], "duplicate certificate ID: %s", id)
		seen[id] = true
	}
}

func TestRegisterCertificate_MultipleHaveUniqueIDs(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	seen := make(map[string]bool)

	for range 20 {
		cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
			CertificatePem: "PEM",
			Status:         "ACTIVE",
		})
		require.NoError(t, err)
		assert.False(t, seen[cert.CertificateID], "duplicate cert ID: %s", cert.CertificateID)
		seen[cert.CertificateID] = true
	}
}

func TestCreateCertFromCsr_ResponseIncludesStatus_Active(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr-data",
		"setAsActive":               true,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ACTIVE", out["status"])
}

func TestCreateCertFromCsr_ResponseIncludesStatus_Inactive(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var out map[string]string
	code := r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr-data",
		"setAsActive":               false,
	}, &out)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "INACTIVE", out["status"])
}

func TestCertificate_LastModifiedDate_SetOnCreate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero(), "LastModifiedAt should be set on certificate creation")
}

func TestDescribeCertificate_ReturnsLastModifiedDate(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
		"setAsActive":               true,
	}, &createOut)
	certID := createOut["certificateId"]

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/certificates/"+certID, nil, &out)
	require.Equal(t, http.StatusOK, code)
	desc, ok := out["certificateDescription"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, desc["lastModifiedDate"])
}

// TestDescribeCertificate_WireShape verifies DescribeCertificate's
// certificateDescription includes the full real CertificateDescription field
// set (ownedBy, generationId, certificateMode, customerVersion, validity),
// field-diffed against aws-sdk-go-v2/service/iot@v1.77.4. These fields were
// entirely missing before this pass (bd: gopherstack-jy57). It also verifies
// creationDate/lastModifiedDate are JSON numbers of epoch seconds, not
// RFC3339 strings (the restjson1 protocol's DateType wire format).
func TestDescribeCertificate_WireShape(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
		"setAsActive":               true,
	}, &createOut)
	certID := createOut["certificateId"]

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/certificates/"+certID, nil, &out)
	require.Equal(t, http.StatusOK, code)
	desc, ok := out["certificateDescription"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "123456789012", desc["ownedBy"])
	assert.Equal(t, "DEFAULT", desc["certificateMode"])
	assert.NotEmpty(t, desc["generationId"])
	assert.InDelta(t, float64(1), desc["customerVersion"], 0)
	assert.NotContains(t, desc, "previousOwnedBy", "never-transferred cert must omit previousOwnedBy")
	assert.NotContains(t, desc, "transferData", "never-transferred cert must omit transferData")

	validity, ok := desc["validity"].(map[string]any)
	require.True(t, ok, "expected validity object, got %v", desc["validity"])
	assert.Greater(t, validity["notAfter"], validity["notBefore"])

	for _, field := range []string{"creationDate", "lastModifiedDate"} {
		_, isNumber := desc[field].(float64)
		assert.True(t, isNumber, "%s must be a JSON number (epoch seconds), got %T: %v",
			field, desc[field], desc[field])
	}
}

// TestCertificateTransferLifecycle_WireShape exercises
// TransferCertificate -> AcceptCertificateTransfer end to end and verifies
// DescribeCertificate surfaces ownedBy/previousOwnedBy/transferData
// correctly afterward. Regression test for the bug where
// AcceptCertificateTransfer never validated the certificate existed or was
// pending transfer, and never actually updated ownership.
func TestCertificateTransferLifecycle_WireShape(t *testing.T) {
	t.Parallel()

	h, b := newR3Handler()
	cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{Status: "ACTIVE"})
	require.NoError(t, err)

	const originalOwner = "123456789012"

	// Accepting before any transfer was initiated fails.
	rec := doRefRequest(t, h, http.MethodPatch,
		"/accept-certificate-transfer/"+cert.CertificateID, map[string]any{"setAsActive": true}, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code, "accept with no pending transfer must fail: %s", rec.Body.String())

	rec = doRefRequest(t, h, http.MethodPatch,
		"/certificates/"+cert.CertificateID+"/transfer?targetAwsAccount=999999999999",
		map[string]any{"transferMessage": "please take this cert"}, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRefRequest(t, h, http.MethodPatch,
		"/accept-certificate-transfer/"+cert.CertificateID, map[string]any{"setAsActive": true}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]any
	code := r3JSON(t, h, http.MethodGet, "/certificates/"+cert.CertificateID, nil, &out)
	require.Equal(t, http.StatusOK, code)
	desc, ok := out["certificateDescription"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "999999999999", desc["ownedBy"])
	assert.Equal(t, originalOwner, desc["previousOwnedBy"])
	assert.Equal(t, "ACTIVE", desc["status"])

	transferData, ok := desc["transferData"].(map[string]any)
	require.True(t, ok, "expected transferData object, got %v", desc["transferData"])
	assert.Equal(t, "please take this cert", transferData["transferMessage"])
	assert.NotEmpty(t, transferData["acceptDate"])

	// Accepting a second time now fails: the pending transfer was consumed.
	rec = doRefRequest(t, h, http.MethodPatch,
		"/accept-certificate-transfer/"+cert.CertificateID, map[string]any{"setAsActive": true}, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestListCertificates_WireShape verifies ListCertificates returns the real
// AWS IoT "Certificate" summary shape (certificateArn, certificateId,
// certificateMode, creationDate, status) — no lastModifiedDate, unlike
// DescribeCertificate's CertificateDescription — and creationDate as an
// epoch-second JSON number, per restjson1's DateType wire format
// (aws-sdk-go-v2/service/iot@v1.77.4's awsRestjson1_deserializeDocumentCertificate).
func TestListCertificates_WireShape(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	r3Req(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
		"setAsActive":               true,
	})

	var out struct {
		Certificates []map[string]any `json:"certificates"`
	}
	code := r3JSON(t, h, http.MethodGet, "/certificates", nil, &out)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, out.Certificates, 1)

	cert := out.Certificates[0]
	assert.NotEmpty(t, cert["certificateId"])
	assert.NotEmpty(t, cert["certificateArn"])
	assert.Equal(t, "ACTIVE", cert["status"])
	assert.Equal(t, "DEFAULT", cert["certificateMode"])
	assert.NotContains(t, cert, "lastModifiedDate")

	_, isNumber := cert["creationDate"].(float64)
	assert.True(t, isNumber, "creationDate must be a JSON number (epoch seconds), got %T: %v",
		cert["creationDate"], cert["creationDate"])
}

func TestUpdateCertificate_StatusTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newStatus   string
		setAsActive bool
		wantErr     bool
	}{
		{name: "ACTIVE", newStatus: "ACTIVE"},
		{name: "REVOKED", setAsActive: true, newStatus: "REVOKED"},
		{
			name:      "PENDING_TRANSFER cannot be set via UpdateCertificate",
			newStatus: "PENDING_TRANSFER", wantErr: true,
		},
		{
			name:      "PENDING_ACTIVATION cannot be set via UpdateCertificate",
			newStatus: "PENDING_ACTIVATION", wantErr: true,
		},
		{name: "INACTIVE", setAsActive: true, newStatus: "INACTIVE"},
		{name: "BOGUS_STATUS is rejected", newStatus: "BOGUS_STATUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, b := newR3Handler()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: tt.setAsActive})
			require.NoError(t, err)

			err = b.UpdateCertificate(&iot.UpdateCertificateInput{
				CertificateID: cert.CertificateID,
				NewStatus:     tt.newStatus,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, iot.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUpdateCertificate_UpdatesLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
	require.NoError(t, err)
	initial := cert.LastModifiedAt

	err = b.UpdateCertificate(&iot.UpdateCertificateInput{
		CertificateID: cert.CertificateID,
		NewStatus:     "ACTIVE",
	})
	require.NoError(t, err)

	updated, err := b.DescribeCertificate(cert.CertificateID)
	require.NoError(t, err)
	assert.False(t, updated.LastModifiedAt.Before(initial),
		"LastModifiedAt should be updated after UpdateCertificate")
}

func TestUpdateCertificate_InvalidStatus_HTTP400(t *testing.T) {
	t.Parallel()

	h, _ := newR3Handler()
	var createOut map[string]string
	r3JSON(t, h, http.MethodPost, "/certificates/create-from-csr", map[string]any{
		"certificateSigningRequest": "csr",
	}, &createOut)
	certID := createOut["certificateId"]

	var out map[string]string
	code := r3JSON(t, h, http.MethodPut, "/certificates/"+certID+"?newStatus=INVALID_STATUS", nil, &out)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "InvalidRequestException", out["__type"])
}

func TestCertificate_IDIs64HexChars(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	assert.Len(t, cert.CertificateID, 64, "certificate ID should be 64 hex characters")

	for _, ch := range cert.CertificateID {
		assert.True(t, (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'),
			"certificate ID should be lowercase hex, got char: %c", ch)
	}
}

func TestCertificate_ARNContainsRegionAndAccount(t *testing.T) {
	t.Parallel()

	b := iot.NewInMemoryBackendWithConfig("111122223333", "eu-west-1")
	cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(cert.ARN, "arn:aws:iot:eu-west-1:111122223333:cert/"),
		"cert ARN should contain region+account, got: %s", cert.ARN)
}

func TestRegisterCertificate_HasLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
		CertificatePem: "PEM",
		Status:         "ACTIVE",
	})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero())
}

func TestRegisterCertificateWithoutCA_HasLastModifiedDate(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	cert, err := b.RegisterCertificateWithoutCA(&iot.RegisterCertificateInput{
		CertificatePem: "PEM",
		Status:         "INACTIVE",
	})
	require.NoError(t, err)
	assert.False(t, cert.LastModifiedAt.IsZero())
}

func TestReset_ClearsCertificates(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	_, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: true})
	require.NoError(t, err)

	b.Reset()
	assert.Empty(t, b.ListCertificates())
}

func TestCertificate_StatusTransitions(t *testing.T) {
	t.Parallel()

	statuses := []string{"ACTIVE", "INACTIVE", "REVOKED"}

	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			_, b := newR3Handler()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
			require.NoError(t, err)

			err = b.UpdateCertificate(&iot.UpdateCertificateInput{
				CertificateID: cert.CertificateID,
				NewStatus:     status,
			})
			require.NoError(t, err)

			updated, err := b.DescribeCertificate(cert.CertificateID)
			require.NoError(t, err)
			assert.Equal(t, status, updated.Status)
		})
	}
}

func TestDeleteCertificate_NotFound_Error(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()
	err := b.DeleteCertificate(strings.Repeat("0", 64))
	require.ErrorIs(t, err, iot.ErrCertificateNotFound)
}

func TestListCertificates_SortedByID(t *testing.T) {
	t.Parallel()

	_, b := newR3Handler()

	for range 3 {
		_, err := b.RegisterCertificate(&iot.RegisterCertificateInput{
			CertificatePem: "PEM",
			Status:         "ACTIVE",
		})
		require.NoError(t, err)
	}

	certs := b.ListCertificates()
	require.Len(t, certs, 3)

	for idx := 1; idx < len(certs); idx++ {
		assert.LessOrEqual(t, certs[idx-1].CertificateID, certs[idx].CertificateID,
			"certificates should be sorted by ID")
	}
}

// TestParityB_DescribeCertificate_ReturnsPem verifies DescribeCertificate returns certificatePem.
func TestDescribeCertificate_ReturnsPem(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, http.MethodPost, "/certificate/register-no-ca",
		map[string]any{"setAsActive": true})
	require.Equal(t, http.StatusOK, rec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
	certID, _ := cr["certificateId"].(string)
	require.NotEmpty(t, certID)

	rec2 := doRequest(t, h, http.MethodGet, "/certificates/"+certID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &desc))
	certDesc, _ := desc["certificateDescription"].(map[string]any)
	pem, _ := certDesc["certificatePem"].(string)
	assert.NotEmpty(t, pem, "certificatePem must be present in DescribeCertificate response")
}

// TestParityB_UpdateCertificate_RejectsPendingStatuses verifies PENDING_TRANSFER and
// PENDING_ACTIVATION cannot be set via UpdateCertificate.
func TestUpdateCertificate_RejectsPendingStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		newStatus string
		wantErr   bool
	}{
		{name: "PENDING_TRANSFER_rejected", newStatus: "PENDING_TRANSFER", wantErr: true},
		{name: "PENDING_ACTIVATION_rejected", newStatus: "PENDING_ACTIVATION", wantErr: true},
		{name: "ACTIVE_accepted", newStatus: "ACTIVE", wantErr: false},
		{name: "INACTIVE_accepted", newStatus: "INACTIVE", wantErr: false},
		{name: "REVOKED_accepted", newStatus: "REVOKED", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{})
			require.NoError(t, err)

			err = b.UpdateCertificate(&iot.UpdateCertificateInput{
				CertificateID: cert.CertificateID,
				NewStatus:     tt.newStatus,
			})
			if tt.wantErr {
				require.ErrorIs(t, err, iot.ErrValidation)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_DeleteCertificate_ActiveBlocked verifies ACTIVE certs cannot be deleted.
func TestDeleteCertificate_ActiveBlocked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		setActive bool
	}{
		{
			name:      "active_cert_blocked",
			setActive: true,
			wantErr:   iot.ErrDeleteConflict,
		},
		{
			name:      "inactive_cert_allowed",
			setActive: false,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iot.NewInMemoryBackend()
			cert, err := b.CreateCertificateFromCsr(&iot.CreateCertificateFromCsrInput{SetAsActive: tt.setActive})
			require.NoError(t, err)

			err = b.DeleteCertificate(cert.CertificateID)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParityB_TransferCertificate_ReturnsArn verifies TransferCertificate returns the cert ARN.
func TestTransferCertificate_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createRec := doRequest(t, h, http.MethodPost, "/certificate/register-no-ca",
		map[string]any{"setAsActive": true})
	require.Equal(t, http.StatusOK, createRec.Code)

	var cr map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &cr))
	certID, _ := cr["certificateId"].(string)
	certARN, _ := cr["certificateArn"].(string)
	require.NotEmpty(t, certID)
	require.NotEmpty(t, certARN)

	rec := doRequest(t, h, http.MethodPatch,
		"/certificates/"+certID+"/transfer?targetAwsAccount=123456789012", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	transferredARN, _ := resp["transferredCertificateArn"].(string)
	assert.Equal(t, certARN, transferredARN, "transferredCertificateArn must be the cert ARN, not ID")
}

func TestListOutgoingCertificates(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	cert, err := b.RegisterCertificate(&iot.RegisterCertificateInput{Status: "ACTIVE"})
	require.NoError(t, err)

	// Not pending transfer yet: should not appear.
	rec := doRefRequest(t, h, http.MethodGet, "/certificates-out-going", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), cert.CertificateID)

	// PENDING_TRANSFER is set via TransferCertificate, matching real AWS
	// behavior (UpdateCertificate rejects that status directly).
	require.NoError(t, b.TransferCertificate(cert.CertificateID, "123456789012", ""))

	rec = doRefRequest(t, h, http.MethodGet, "/certificates-out-going", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), cert.CertificateID)
}
