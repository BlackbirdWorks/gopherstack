package eks

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	opAssumeRoleForPodIdentity = "AssumeRoleForPodIdentity"

	// PodIdentityAudience is the audience of pod identity tokens and the value of the response audience field.
	PodIdentityAudience = "pods.eks.amazonaws.com"

	podIdentitySessionMax = 64
	jwtParts              = 3
	serviceAccountSubject = "system:serviceaccount:"
)

var (
	// ErrInvalidToken is InvalidTokenException (400) from the eks-auth API.
	ErrInvalidToken = awserr.New("InvalidTokenException", awserr.ErrInvalidParameter)
	// ErrExpiredToken is ExpiredTokenException (400) from the eks-auth API.
	ErrExpiredToken = awserr.New("ExpiredTokenException", awserr.ErrInvalidParameter)
	// ErrAccessDenied is AccessDeniedException; the eks-auth API documents HTTP 400.
	ErrAccessDenied = awserr.New("AccessDeniedException", awserr.ErrInvalidParameter)
	// ErrServiceUnavailable is ServiceUnavailableException (503).
	ErrServiceUnavailable = awserr.New("ServiceUnavailableException", awserr.ErrConflict)

	podIdentityTokenRE = regexp.MustCompile(`^[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+\.[A-Za-z0-9\-_=]+$`)
	sessionNameCleanRE = regexp.MustCompile(`[^\w+=,.@-]`)
)

// PodIdentityTag is one session tag attached to pod identity credentials.
type PodIdentityTag struct{ Key, Value string }

// PodIdentityRoleRequest asks the STS backend for the credentials of a pod identity role.
// An empty CallerARN is the first hop (trust is checked for pods.eks.amazonaws.com);
// otherwise it is a role-chaining hop from CallerARN.
type PodIdentityRoleRequest struct {
	RoleARN     string
	SessionName string
	CallerARN   string
	ExternalID  string
	Policy      string
	Tags        []PodIdentityTag
}

// PodIdentityCredentials are the temporary credentials STS issued.
type PodIdentityCredentials struct {
	Expiration      time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	AssumedRoleARN  string
	AssumedRoleID   string
}

// PodIdentityRoleAssumer issues the credentials returned by AssumeRoleForPodIdentity.
type PodIdentityRoleAssumer interface {
	AssumePodIdentityRole(req PodIdentityRoleRequest) (PodIdentityCredentials, error)
}

// PodIdentityResult is the AssumeRoleForPodIdentity output.
type PodIdentityResult struct {
	Creds          PodIdentityCredentials
	AssociationARN string
	AssociationID  string
	Namespace      string
	ServiceAccount string
}

// SetPodIdentityRoleAssumer sets how pod identity credentials are issued; call before serving traffic.
func (b *InMemoryBackend) SetPodIdentityRoleAssumer(a PodIdentityRoleAssumer) {
	b.mu.Lock("SetPodIdentityRoleAssumer")
	defer b.mu.Unlock()

	b.podIdentityAssumer = a
}

// SetPodIdentityRoleAssumer wires a into every region's backend.
func (h *Handler) SetPodIdentityRoleAssumer(a PodIdentityRoleAssumer) {
	for _, bk := range h.RegionBackends() {
		bk.SetPodIdentityRoleAssumer(a)
	}
}

type podIdentityClaims struct {
	Subject    string   `json:"sub"`
	Issuer     string   `json:"iss"`
	Audience   audience `json:"aud"`
	Expiry     int64    `json:"exp"`
	NotBefore  int64    `json:"nbf"`
	Kubernetes struct {
		Namespace      string `json:"namespace"`
		ServiceAccount struct {
			Name string `json:"name"`
		} `json:"serviceaccount"`
		Pod struct {
			Name string `json:"name"`
			UID  string `json:"uid"`
		} `json:"pod"`
	} `json:"kubernetes.io"`
}

// audience decodes the JWT aud claim, which may be a string or an array.
type audience []string

func (a *audience) UnmarshalJSON(raw []byte) error {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		*a = audience{one}

		return nil
	}

	var many []string

	if err := json.Unmarshal(raw, &many); err != nil {
		return err
	}

	*a = many

	return nil
}

func verifyPodIdentityToken(token, issuer string, pub *rsa.PublicKey, now time.Time) (*podIdentityClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != jwtParts {
		return nil, errMsgf(ErrInvalidToken, "The token is not a valid JWT.")
	}

	sig, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[2], "="))
	if err != nil {
		return nil, errMsgf(ErrInvalidToken, "The token signature is malformed.")
	}

	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig) != nil {
		return nil, errMsgf(ErrInvalidToken, "The token signature is invalid.")
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil, errMsgf(ErrInvalidToken, "The token payload is malformed.")
	}

	var c podIdentityClaims
	if json.Unmarshal(raw, &c) != nil {
		return nil, errMsgf(ErrInvalidToken, "The token payload is malformed.")
	}

	switch {
	case c.Issuer != issuer:
		return nil, errMsgf(ErrInvalidToken, "The token was not issued by this cluster.")
	case !slices.Contains(c.Audience, PodIdentityAudience):
		return nil, errMsgf(ErrInvalidToken, "The token audience must be %s.", PodIdentityAudience)
	case c.Expiry != 0 && !now.Before(time.Unix(c.Expiry, 0)):
		return nil, errMsgf(ErrExpiredToken, "The token is expired.")
	case c.NotBefore != 0 && now.Before(time.Unix(c.NotBefore, 0)):
		return nil, errMsgf(ErrInvalidToken, "The token is not valid yet.")
	}

	if !strings.HasPrefix(c.Subject, serviceAccountSubject) {
		return nil, errMsgf(ErrInvalidToken, "The token is not a service account token.")
	}

	return &c, nil
}

func podIdentitySessionName(cluster, pod string) string {
	if pod == "" {
		pod = "pod"
	}

	name := sessionNameCleanRE.ReplaceAllString("eks-"+cluster+"-"+pod, "-")
	id := uuid.NewString()

	return name[:min(len(name), podIdentitySessionMax-len(id)-1)] + "-" + id
}

func podIdentitySessionTags(c *Cluster, claims *podIdentityClaims, ns, sa string) []PodIdentityTag {
	tags := []PodIdentityTag{
		{"eks-cluster-arn", c.ARN}, {"eks-cluster-name", c.Name},
		{"kubernetes-namespace", ns}, {"kubernetes-service-account", sa},
	}

	if claims.Kubernetes.Pod.Name != "" {
		tags = append(tags, PodIdentityTag{"kubernetes-pod-name", claims.Kubernetes.Pod.Name})
	}

	if claims.Kubernetes.Pod.UID != "" {
		tags = append(tags, PodIdentityTag{"kubernetes-pod-uid", claims.Kubernetes.Pod.UID})
	}

	return tags
}

// AssumeRoleForPodIdentity validates a pod's service-account token against the cluster, resolves the matching
// pod identity association and returns STS credentials for its role (and target role, when set).
func (b *InMemoryBackend) AssumeRoleForPodIdentity(clusterName, token string) (*PodIdentityResult, error) {
	if !podIdentityTokenRE.MatchString(token) {
		return nil, errMsgf(ErrValidation, "token does not match the required pattern.")
	}

	b.mu.RLock("AssumeRoleForPodIdentity")
	c, ok := b.clusters.Get(clusterName)
	assumer := b.podIdentityAssumer
	b.mu.RUnlock()

	if !ok {
		return nil, errMsgf(ErrNotFound, "No cluster found for name: %s.", clusterName)
	}

	key, err := b.signingKey(c.OIDCIssuer)
	if err != nil {
		return nil, err
	}

	claims, err := verifyPodIdentityToken(token, c.OIDCIssuer, &key.PublicKey, time.Now())
	if err != nil {
		return nil, err
	}

	ns, sa := claims.Kubernetes.Namespace, claims.Kubernetes.ServiceAccount.Name
	if want := serviceAccountSubject + ns + ":" + sa; ns == "" || sa == "" || claims.Subject != want {
		return nil, errMsgf(ErrInvalidToken, "The token subject does not match its service account claims.")
	}

	assoc, found := b.podIdentityFor(clusterName, ns, sa)
	if !found {
		return nil, errMsgf(ErrAccessDenied, "No pod identity association exists for service account %s/%s.", ns, sa)
	}

	if assumer == nil {
		return nil, errMsgf(ErrServiceUnavailable, "No STS backend is available to issue credentials.")
	}

	creds, err := assumePodIdentity(assumer, c, claims, assoc)
	if err != nil {
		return nil, errMsgf(ErrAccessDenied, "Unable to assume the pod identity role: %v", err)
	}

	return &PodIdentityResult{
		Creds: creds, AssociationARN: assoc.ARN, AssociationID: assoc.AssociationID, Namespace: ns, ServiceAccount: sa,
	}, nil
}

func assumePodIdentity(
	a PodIdentityRoleAssumer, c *Cluster, claims *podIdentityClaims, assoc PodIdentityAssociation,
) (PodIdentityCredentials, error) {
	session := podIdentitySessionName(c.Name, claims.Kubernetes.Pod.Name)

	var tags []PodIdentityTag
	if !assoc.DisableSessionTags {
		tags = podIdentitySessionTags(c, claims, assoc.Namespace, assoc.ServiceAccount)
	}

	first := PodIdentityRoleRequest{RoleARN: assoc.RoleARN, SessionName: session, Tags: tags}
	if assoc.TargetRoleARN == "" {
		first.Policy = assoc.Policy

		return a.AssumePodIdentityRole(first)
	}

	hop, err := a.AssumePodIdentityRole(first)
	if err != nil {
		return PodIdentityCredentials{}, err
	}

	return a.AssumePodIdentityRole(PodIdentityRoleRequest{
		RoleARN: assoc.TargetRoleARN, SessionName: session, CallerARN: hop.AssumedRoleARN,
		ExternalID: assoc.ExternalID, Policy: assoc.Policy, Tags: tags,
	})
}

func (b *InMemoryBackend) podIdentityFor(cluster, ns, sa string) (PodIdentityAssociation, bool) {
	b.mu.RLock("podIdentityFor")
	defer b.mu.RUnlock()

	for _, a := range b.podIdentityAssociationsByCluster.Get(cluster) {
		if a.Namespace == ns && a.ServiceAccount == sa && a.RoleARN != "" {
			return *a, true
		}
	}

	return PodIdentityAssociation{}, false
}

func (h *Handler) handleAssumeRoleForPodIdentity(c *echo.Context, clusterName string, body []byte) error {
	var in struct {
		Token string `json:"token"`
	}

	if err := json.Unmarshal(body, &in); err != nil || in.Token == "" {
		return h.handleError(c, errMsgf(ErrValidation, "token is required."))
	}

	res, err := h.Backend.AssumeRoleForPodIdentity(clusterName, in.Token)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"assumedRoleUser": map[string]string{"arn": res.Creds.AssumedRoleARN, "assumeRoleId": res.Creds.AssumedRoleID},
		"audience":        PodIdentityAudience,
		"credentials": map[string]any{
			"accessKeyId":     res.Creds.AccessKeyID,
			"secretAccessKey": res.Creds.SecretAccessKey,
			"sessionToken":    res.Creds.SessionToken,
			"expiration":      res.Creds.Expiration.Unix(),
		},
		"podIdentityAssociation": map[string]string{
			"associationArn": res.AssociationARN, "associationId": res.AssociationID,
		},
		"subject": map[string]string{"namespace": res.Namespace, "serviceAccount": res.ServiceAccount},
	})
}
