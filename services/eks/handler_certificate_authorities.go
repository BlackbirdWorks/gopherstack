package eks

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const keyCertificateAuthority = "certificateAuthority"

// dispatchCertificateAuthorityOps handles the EKS Hybrid Nodes
// CertificateAuthority CRUD+activate family (gopherstack-lruaw).
func (h *Handler) dispatchCertificateAuthorityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateCertificateAuthority:
		return true, h.handleCreateCertificateAuthority(c, route.clusterName, body)
	case opActivateCertificateAuthority:
		return true, h.handleActivateCertificateAuthority(c, route.clusterName, route.nodegroupName, body)
	case opDeleteCertificateAuthority:
		return true, h.handleDeleteCertificateAuthority(c, route.clusterName, route.nodegroupName)
	case opDescribeCertificateAuthority:
		return true, h.handleDescribeCertificateAuthority(c, route.clusterName, route.nodegroupName)
	case opListCertificateAuthorities:
		return true, h.handleListCertificateAuthorities(c, route.clusterName)
	}

	return false, nil
}

// parseCertificateAuthorityRoute returns the route for
// /clusters/{name}/certificate-authorities[/{id}[/activate]] paths.
func parseCertificateAuthorityRoute(method, clusterName string, parts []string) eksRoute {
	const certificateAuthorityParts = 2

	if len(parts) == certificateAuthorityParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateCertificateAuthority, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListCertificateAuthorities, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	tail := parts[2]

	if id, ok := strings.CutSuffix(tail, "/activate"); ok {
		if method == http.MethodPost {
			return eksRoute{operation: opActivateCertificateAuthority, clusterName: clusterName, nodegroupName: id}
		}

		return eksRoute{operation: opUnknown}
	}

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeCertificateAuthority, clusterName: clusterName, nodegroupName: tail}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteCertificateAuthority, clusterName: clusterName, nodegroupName: tail}
	}

	return eksRoute{operation: opUnknown}
}

// certificateAuthoritySummaryToJSON builds the types.CertificateAuthoritySummary
// shape (deserializers.go's awsRestjson1_deserializeDocumentCertificateAuthoritySummary) --
// used by Create/Activate/Delete's "certificateAuthority" key and every
// entry of List's "certificateAuthorities" array. Unlike the full
// CertificateAuthority shape (certificateAuthorityToJSON), the real summary
// has no data/rollbackAvailable/validity/scheduledEvents members.
func certificateAuthoritySummaryToJSON(ca *CertificateAuthority) map[string]any {
	m := map[string]any{
		"id":                 ca.ID,
		"signingStatus":      ca.SigningStatus,
		"distributionStatus": ca.DistributionStatus,
		"createdBy":          ca.CreatedBy,
		keyCreatedAt:         ca.CreatedAt.Unix(),
	}

	if ca.ActivatedAt != nil {
		m["activatedAt"] = ca.ActivatedAt.Unix()
		m["activatedBy"] = ca.ActivatedBy
	}

	return m
}

// certificateAuthorityToJSON builds the full types.CertificateAuthority
// shape (deserializers.go's awsRestjson1_deserializeDocumentCertificateAuthority),
// used only by DescribeCertificateAuthority's response.
func certificateAuthorityToJSON(ca *CertificateAuthority) map[string]any {
	m := certificateAuthoritySummaryToJSON(ca)

	m["data"] = ca.Data
	m["rollbackAvailable"] = ca.RollbackAvailable
	m["validity"] = map[string]any{
		"notBefore": ca.NotBefore.Unix(),
		"notAfter":  ca.NotAfter.Unix(),
	}

	return m
}

// certificateAuthorityIdempotencyFingerprintBody synthesizes the payload
// withIdempotency fingerprints for the CertificateAuthority write ops.
// Every one of Create/Activate/DeleteCertificateAuthorityInput carries
// ClientRequestToken as its ONLY body member (verified: serializers.go's
// awsRestjson1_serializeOpDocument<Op>Input for all three writes exactly one
// key, "clientRequestToken") -- unlike e.g. CreateFargateProfile, whose body
// also carries fargateProfileName/selectors/etc. idempotencyFingerprint
// drops "clientRequestToken" before hashing, so fingerprinting the real
// request body here would always canonicalize to "{}" regardless of which
// cluster or certificate authority the request actually names (those
// identifiers live only in the URL). Left unfixed, a client that reused one
// ClientRequestToken across two different clusters or certificate
// authorities would incorrectly replay the FIRST call's response for the
// second, entirely different, request instead of performing it. Folding the
// route's own clusterName/id into the fingerprinted payload closes that.
func certificateAuthorityIdempotencyFingerprintBody(clusterName, id string) []byte {
	payload := map[string]string{keyClusterName: clusterName}
	if id != "" {
		payload["certificateAuthorityId"] = id
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}

	return raw
}

type createCertificateAuthorityBody struct {
	ClientRequestToken string `json:"clientRequestToken"`
}

func (h *Handler) handleCreateCertificateAuthority(c *echo.Context, clusterName string, body []byte) error {
	var in createCertificateAuthorityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
		}
	}

	fp := certificateAuthorityIdempotencyFingerprintBody(clusterName, "")

	return h.withIdempotency(c, opCreateCertificateAuthority, in.ClientRequestToken, fp, func() (int, any, error) {
		ca, upd, err := h.Backend.CreateCertificateAuthority(clusterName)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{
			keyCertificateAuthority: certificateAuthoritySummaryToJSON(ca),
			keyUpdate:               updateToJSON(upd),
		}, nil
	})
}

type activateCertificateAuthorityBody struct {
	ClientRequestToken string `json:"clientRequestToken"`
}

func (h *Handler) handleActivateCertificateAuthority(c *echo.Context, clusterName, id string, body []byte) error {
	var in activateCertificateAuthorityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
		}
	}

	fp := certificateAuthorityIdempotencyFingerprintBody(clusterName, id)

	return h.withIdempotency(c, opActivateCertificateAuthority, in.ClientRequestToken, fp, func() (int, any, error) {
		ca, upd, err := h.Backend.ActivateCertificateAuthority(clusterName, id)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{
			keyCertificateAuthority: certificateAuthoritySummaryToJSON(ca),
			keyUpdate:               updateToJSON(upd),
		}, nil
	})
}

// handleDeleteCertificateAuthority -- DeleteCertificateAuthorityInput is the
// only op in this service whose ClientRequestToken travels as a query
// parameter, not a JSON body (serializers.go: DELETE carries no request
// document; awsRestjson1_serializeOpHttpBindingsDeleteCertificateAuthorityInput
// uses encoder.SetQuery("clientRequestToken"), not the body encoder every
// other op uses).
func (h *Handler) handleDeleteCertificateAuthority(c *echo.Context, clusterName, id string) error {
	token := c.QueryParam("clientRequestToken")
	fp := certificateAuthorityIdempotencyFingerprintBody(clusterName, id)

	return h.withIdempotency(c, opDeleteCertificateAuthority, token, fp, func() (int, any, error) {
		ca, upd, err := h.Backend.DeleteCertificateAuthority(clusterName, id)
		if err != nil {
			return 0, nil, err
		}

		return http.StatusOK, map[string]any{
			keyCertificateAuthority: certificateAuthoritySummaryToJSON(ca),
			keyUpdate:               updateToJSON(upd),
		}, nil
	})
}

func (h *Handler) handleDescribeCertificateAuthority(c *echo.Context, clusterName, id string) error {
	ca, err := h.Backend.DescribeCertificateAuthority(clusterName, id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyCertificateAuthority: certificateAuthorityToJSON(ca)})
}

func (h *Handler) handleListCertificateAuthorities(c *echo.Context, clusterName string) error {
	cas, err := h.Backend.ListCertificateAuthorities(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]map[string]any, len(cas))
	for i, ca := range cas {
		summaries[i] = certificateAuthoritySummaryToJSON(ca)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(summaries, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("certificateAuthorities", p))
}
