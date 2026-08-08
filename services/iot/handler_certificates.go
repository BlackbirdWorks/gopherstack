package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveCertificateOps(path, method string) string {
	switch {
	case path == "/certificates" && method == http.MethodGet:

		return opListCertificates
	case path == "/certificate/register" && method == http.MethodPost:

		return opRegisterCertificate
	case path == "/certificate/register-no-ca" && method == http.MethodPost:

		return opRegisterCertificateWithoutCA
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodPost:

		return opCreateCertificateFromCsr
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodGet:

		return opDescribeCertificate
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodPut:

		return opUpdateCertificate
	case strings.HasPrefix(path, "/certificates/") && method == http.MethodDelete:

		return opDeleteCertificate
	}

	return unknownOperation
}

func resolveCertificateProviderOps(path, method string) string {
	switch {
	case path == "/certificate-providers" && method == http.MethodGet:

		return opListCertificateProviders
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodPost:

		return opCreateCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodGet:

		return opDescribeCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodPut:

		return opUpdateCertificateProvider
	case strings.HasPrefix(path, "/certificate-providers/") && method == http.MethodDelete:

		return opDeleteCertificateProvider
	}

	return unknownOperation
}

func (h *Handler) dispatchCertificateOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateCertificateFromCsr:

		return true, h.handleCreateCertificateFromCsr(c)
	case opRegisterCertificate:

		return true, h.handleRegisterCertificate(c)
	case opRegisterCertificateWithoutCA:

		return true, h.handleRegisterCertificateWithoutCA(c)
	case opDescribeCertificate:

		return true, h.handleDescribeCertificate(c)
	case opListCertificates:

		return true, h.handleListCertificates(c)
	case opUpdateCertificate:

		return true, h.handleUpdateCertificate(c)
	case opDeleteCertificate:

		return true, h.handleDeleteCertificate(c)
	}

	return false, nil
}

func (h *Handler) dispatchCertificateProviderOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateCertificateProvider:

		return true, h.handleCreateCertificateProvider(c)
	case opDescribeCertificateProvider:

		return true, h.handleDescribeCertificateProvider(c)
	case opListCertificateProviders:

		return true, h.handleListCertificateProviders(c)
	case opUpdateCertificateProvider:

		return true, h.handleUpdateCertificateProvider(c)
	case opDeleteCertificateProvider:

		return true, h.handleDeleteCertificateProvider(c)
	}

	return false, nil
}

func (h *Handler) handleCreateCertificateFromCsr(c *echo.Context) error {
	var body struct {
		CertificateSigningRequest string `json:"certificateSigningRequest"`
		SetAsActive               bool   `json:"setAsActive"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.CreateCertificateFromCsr(&CreateCertificateFromCsrInput{
		CertificateSigningRequest: body.CertificateSigningRequest,
		SetAsActive:               body.SetAsActive,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
		keyCertificatePem: cert.PEM,
		keyStatus:         cert.Status,
	})
}

func (h *Handler) handleRegisterCertificate(c *echo.Context) error {
	var body struct {
		CertificatePem string `json:"certificatePem"`
		Status         string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.RegisterCertificate(&RegisterCertificateInput{
		CertificatePem: body.CertificatePem,
		Status:         body.Status,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
	})
}

func (h *Handler) handleRegisterCertificateWithoutCA(c *echo.Context) error {
	var body struct {
		CertificatePem string `json:"certificatePem"`
		Status         string `json:"status"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}
	cert, err := h.Backend.RegisterCertificateWithoutCA(&RegisterCertificateInput{
		CertificatePem: body.CertificatePem,
		Status:         body.Status,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateID:  cert.CertificateID,
		keyCertificateArn: cert.ARN,
	})
}

// certificateTransferData builds the DescribeCertificate "transferData"
// sub-object. Real AWS only populates it once a transfer has been initiated
// (TransferCertificate) and/or resolved (Accept/RejectCertificateTransfer);
// this mock omits the field entirely (returns nil) when the certificate has
// never been part of a transfer, matching the real API's omitted-object
// behavior for certificates that were never transferred.
func certificateTransferData(cert *Certificate) map[string]any {
	if cert.TransferDate.IsZero() && cert.TransferAcceptDate.IsZero() && cert.TransferRejectDate.IsZero() {
		return nil
	}

	data := map[string]any{}
	if !cert.TransferDate.IsZero() {
		data["transferDate"] = awstime.Epoch(cert.TransferDate)
		data["transferMessage"] = cert.TransferMessage
	}

	if !cert.TransferAcceptDate.IsZero() {
		data["acceptDate"] = awstime.Epoch(cert.TransferAcceptDate)
	}

	if !cert.TransferRejectDate.IsZero() {
		data["rejectDate"] = awstime.Epoch(cert.TransferRejectDate)
		data["rejectReason"] = cert.TransferRejectReason
	}

	return data
}

// certificateDescriptionFields builds the full CertificateDescription wire
// shape returned by DescribeCertificate (aws-sdk-go-v2/service/iot@v1.76.0).
// creationDate/lastModifiedDate must be epoch-second JSON numbers, not
// RFC3339 strings — the real restjson1 deserializer requires numbers.
func certificateDescriptionFields(cert *Certificate) map[string]any {
	out := map[string]any{
		keyCertificateID:    cert.CertificateID,
		keyCertificateArn:   cert.ARN,
		keyStatus:           cert.Status,
		keyCreationDate:     awstime.Epoch(cert.CreatedAt),
		keyLastModifiedDate: awstime.Epoch(cert.LastModifiedAt),
		"certificatePem":    cert.PEM,
		"ownedBy":           cert.OwnedBy,
		"certificateMode":   cert.CertificateMode,
		"customerVersion":   cert.CustomerVersion,
	}

	if cert.CACertificateID != "" {
		out["caCertificateId"] = cert.CACertificateID
	}

	if cert.PreviousOwnedBy != "" {
		out["previousOwnedBy"] = cert.PreviousOwnedBy
	}

	if cert.GenerationID != "" {
		out["generationId"] = cert.GenerationID
	}

	if !cert.ValidityNotBefore.IsZero() || !cert.ValidityNotAfter.IsZero() {
		out["validity"] = map[string]any{
			"notBefore": awstime.Epoch(cert.ValidityNotBefore),
			"notAfter":  awstime.Epoch(cert.ValidityNotAfter),
		}
	}

	if td := certificateTransferData(cert); td != nil {
		out["transferData"] = td
	}

	return out
}

func (h *Handler) handleDescribeCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	cert, err := h.Backend.DescribeCertificate(certID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificateDescription": certificateDescriptionFields(cert),
	})
}

func (h *Handler) handleListCertificates(c *echo.Context) error {
	certs := h.Backend.ListCertificates()
	out := make([]map[string]any, 0, len(certs))

	for _, cert := range certs {
		// ListCertificates returns the lighter-weight Certificate shape (not
		// CertificateDescription): certificateArn, certificateId,
		// certificateMode, creationDate, status -- no lastModifiedDate.
		out = append(out, map[string]any{
			keyCertificateID:  cert.CertificateID,
			keyCertificateArn: cert.ARN,
			keyStatus:         cert.Status,
			keyCreationDate:   awstime.Epoch(cert.CreatedAt),
			"certificateMode": cert.CertificateMode,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"certificates": out})
}

func (h *Handler) handleUpdateCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	newStatus := c.QueryParam("newStatus")
	if err := h.Backend.UpdateCertificate(&UpdateCertificateInput{
		CertificateID: certID,
		NewStatus:     newStatus,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCertificate(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	if err := h.Backend.DeleteCertificate(certID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")

	var body struct {
		LambdaFunctionARN           string   `json:"lambdaFunctionArn"`
		AccountDefaultForOperations []string `json:"accountDefaultForOperations"`
		// []types.Tag on the wire, not a map (serializers.go:1992, aws-sdk-go-v2/service/iot@v1.77.4).
		Tags []tags.KV `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	cp, err := h.Backend.CreateCertificateProvider(&CreateCertificateProviderInput{
		CertificateProviderName:     name,
		LambdaFunctionARN:           body.LambdaFunctionARN,
		AccountDefaultForOperations: body.AccountDefaultForOperations,
		Tags:                        tags.MapFromKV(body.Tags),
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyCertificateProviderName: cp.CertificateProviderName,
		keyCertificateProviderArn:  cp.ARN,
	})
}

func (h *Handler) handleDescribeCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")
	cp, err := h.Backend.DescribeCertificateProvider(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCertificateProviderName:    cp.CertificateProviderName,
		keyCertificateProviderArn:     cp.ARN,
		"lambdaFunctionArn":           cp.LambdaFunctionARN,
		"accountDefaultForOperations": cp.AccountDefaultForOperations,
		keyCreationDate:               awstime.Epoch(cp.CreatedAt),
		keyLastModifiedDate:           awstime.Epoch(cp.LastModifiedAt),
	})
}

func (h *Handler) handleListCertificateProviders(c *echo.Context) error {
	providers := h.Backend.ListCertificateProviders()
	out := make([]map[string]string, 0, len(providers))
	for _, cp := range providers {
		out = append(out, map[string]string{
			keyCertificateProviderName: cp.CertificateProviderName,
			keyCertificateProviderArn:  cp.ARN,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"certificateProviders": out})
}

func (h *Handler) handleUpdateCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")

	var body struct {
		LambdaFunctionARN           string   `json:"lambdaFunctionArn"`
		AccountDefaultForOperations []string `json:"accountDefaultForOperations"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	if err := h.Backend.UpdateCertificateProvider(&UpdateCertificateProviderInput{
		CertificateProviderName:     name,
		LambdaFunctionARN:           body.LambdaFunctionARN,
		AccountDefaultForOperations: body.AccountDefaultForOperations,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCertificateProvider(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/certificate-providers/")
	if err := h.Backend.DeleteCertificateProvider(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func resolveCACertOps(path, method string) string {
	switch {
	case path == "/cacertificates" && method == http.MethodGet:
		return opListCACertificates
	case path == "/cacertificate/register" && method == http.MethodPost:
		return opRegisterCACertificate
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodGet:
		return opDescribeCACertificate
	case strings.HasPrefix(path, "/cacertificates/") &&
		strings.HasSuffix(path, "/certificates") &&
		method == http.MethodGet:
		return opListCertificatesByCA
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodPut:
		return opUpdateCACertificate
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodDelete:
		return opDeleteCACertificate
	}

	return unknownOperation
}

func (h *Handler) handleRegisterCACertificate(c *echo.Context) error {
	var req struct {
		CACertificate           string `json:"caCertificate"`
		VerificationCertificate string `json:"verificationCertificate,omitempty"`
		Status                  string `json:"registrationConfig,omitempty"`
		// []types.Tag on the wire, not a map (serializers.go:18065, aws-sdk-go-v2/service/iot@v1.77.4).
		Tags []tags.KV `json:"tags,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	ca, err := h.Backend.RegisterCACertificate(req.CACertificate, "ACTIVE", tags.MapFromKV(req.Tags))
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCertificateArn: ca.CertificateARN,
		keyCertificateID:  ca.CertificateID,
	})
}

func (h *Handler) handleDescribeCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	ca, err := h.Backend.DescribeCACertificate(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"certificateDescription": ca})
}

func (h *Handler) handleListCACertificates(c *echo.Context) error {
	certs := h.Backend.ListCACertificates()
	summaries := make([]map[string]any, len(certs))
	for i, ca := range certs {
		summaries[i] = map[string]any{
			keyCertificateID:  ca.CertificateID,
			keyCertificateArn: ca.CertificateARN,
			keyStatus:         ca.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyCertificates: summaries})
}

func (h *Handler) handleUpdateCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	var req struct {
		NewStatus string `json:"newStatus"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateCACertificate(id, req.NewStatus); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	if err := h.Backend.DeleteCACertificate(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListCertificatesByCA(c *echo.Context) error {
	// /cacertificates/{caCertificateId}/certificates
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	caID := strings.TrimSuffix(trimmed, "/certificates")
	certs := h.Backend.ListCertificatesByCA(caID)
	summaries := make([]map[string]any, len(certs))
	for i, cert := range certs {
		summaries[i] = map[string]any{
			keyCertificateID:  cert.CertificateID,
			keyCertificateArn: cert.ARN,
			keyStatus:         cert.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyCertificates: summaries})
}

func (h *Handler) handleCancelCertificateTransfer(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/cancel-certificate-transfer/")
	if err := h.Backend.CancelCertificateTransfer(certID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// resolveBatch3CertOps handles certificate-related misc ops.
func resolveBatch3CertOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/provisioning-claim") &&
		method == http.MethodPost:

		return opCreateProvisioningClaim
	case path == "/keys-and-certificate" && method == http.MethodPost:

		return opCreateKeysAndCertificate
	case strings.HasPrefix(path, "/certificates/") &&
		strings.HasSuffix(path, "/transfer") &&
		method == http.MethodPatch:

		return opTransferCertificate
	case strings.HasPrefix(path, "/reject-certificate-transfer/") && method == http.MethodPatch:

		return opRejectCertificateTransfer
	}

	return unknownOperation
}

func (h *Handler) handleCreateProvisioningClaim(c *echo.Context) error {
	// /provisioning-templates/{templateName}/provisioning-claim
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	templateName := strings.TrimSuffix(trimmed, "/provisioning-claim")
	certPEM, publicKey, privateKey, err := h.Backend.CreateProvisioningClaim(templateName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCertificatePem: certPEM,
		"keyPair": map[string]string{
			"PublicKey":  publicKey,
			"PrivateKey": privateKey,
		},
	})
}

func (h *Handler) handleCreateKeysAndCertificate(c *echo.Context) error {
	setAsActive := c.Request().URL.Query().Get("setAsActive") == keyBoolTrue
	cert, publicKey, privateKey, err := h.Backend.CreateKeysAndCertificate(setAsActive)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"certificateArn":  cert.ARN,
		"certificateId":   cert.CertificateID,
		keyCertificatePem: cert.PEM,
		"keyPair": map[string]string{
			"PublicKey":  publicKey,
			"PrivateKey": privateKey,
		},
	})
}

func (h *Handler) handleTransferCertificate(c *echo.Context) error {
	// PATCH /certificates/{certId}/transfer?targetAwsAccount=...
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/certificates/")
	certID := strings.TrimSuffix(trimmed, "/transfer")
	targetAccount := c.Request().URL.Query().Get("targetAwsAccount")

	var body struct {
		TransferMessage string `json:"transferMessage"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	cert, lookupErr := h.Backend.DescribeCertificate(certID)
	if lookupErr != nil {
		return h.handleError(c, lookupErr)
	}

	if transferErr := h.Backend.TransferCertificate(certID, targetAccount, body.TransferMessage); transferErr != nil {
		return respondErr(c, transferErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"transferredCertificateArn": cert.ARN})
}

func (h *Handler) handleRejectCertificateTransfer(c *echo.Context) error {
	// PATCH /reject-certificate-transfer/{certId}
	certID := strings.TrimPrefix(c.Request().URL.Path, "/reject-certificate-transfer/")

	var body struct {
		RejectReason string `json:"rejectReason"`
	}
	if err := readBody(c, &body); err != nil {
		return err
	}

	if err := h.Backend.RejectCertificateTransfer(certID, body.RejectReason); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListOutgoingCertificates(c *echo.Context) error {
	certs := h.Backend.ListOutgoingCertificates()

	return c.JSON(http.StatusOK, map[string]any{"outgoingCertificates": certs})
}

func (h *Handler) dispatchCACertOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opRegisterCACertificate:
		return true, h.handleRegisterCACertificate(c)
	case opDescribeCACertificate:
		return true, h.handleDescribeCACertificate(c)
	case opListCACertificates:
		return true, h.handleListCACertificates(c)
	case opUpdateCACertificate:
		return true, h.handleUpdateCACertificate(c)
	case opDeleteCACertificate:
		return true, h.handleDeleteCACertificate(c)
	case opListCertificatesByCA:
		return true, h.handleListCertificatesByCA(c)
	case opCancelCertificateTransfer:
		return true, h.handleCancelCertificateTransfer(c)
	}

	return false, nil
}

func (h *Handler) dispatchCertClaimOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreateProvisioningClaim:
		return true, h.handleCreateProvisioningClaim(c)
	case opCreateKeysAndCertificate:
		return true, h.handleCreateKeysAndCertificate(c)
	case opTransferCertificate:
		return true, h.handleTransferCertificate(c)
	case opRejectCertificateTransfer:
		return true, h.handleRejectCertificateTransfer(c)
	}

	return false, nil
}

// resolveCertTransferPathOps resolves the CancelCertificateTransfer path.
func resolveCertTransferPathOps(path, method string) string {
	if strings.HasPrefix(path, "/cancel-certificate-transfer/") && method == http.MethodPatch {
		return opCancelCertificateTransfer
	}

	return unknownOperation
}
