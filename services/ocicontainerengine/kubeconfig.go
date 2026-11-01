package ocicontainerengine

import (
	"encoding/base64"
	"net/http"
	"strings"
)

const (
	endpointLegacy  = "LEGACY_KUBERNETES"
	endpointPublic  = "PUBLIC_ENDPOINT"
	endpointPrivate = "PRIVATE_ENDPOINT"
	endpointVcnHost = "VCN_HOSTNAME"
	tokenVersion2   = "2.0.0"
)

// KubeconfigIn is the CreateKubeconfig request body.
type KubeconfigIn struct {
	TokenVersion *string `json:"tokenVersion"`
	Expiration   *int    `json:"expiration"`
	Endpoint     string  `json:"endpoint"`
}

func pickEndpoint(c *Cluster, want string) (string, *APIError) {
	ep := c.Endpoints

	switch want {
	case endpointLegacy:
		return ep.Kubernetes, nil
	case endpointPublic:
		return ep.PublicEndpoint, nil
	case endpointPrivate:
		return ep.PrivateEndpoint, nil
	case endpointVcnHost:
		return ep.VcnHostnameEndpoint, nil
	case "":
		for _, v := range []string{ep.Kubernetes, ep.PublicEndpoint, ep.PrivateEndpoint} {
			if v != "" {
				return v, nil
			}
		}

		return "", nil
	}

	return "", errParam("endpoint must be one of " + strings.Join(
		[]string{endpointLegacy, endpointPublic, endpointPrivate, endpointVcnHost}, ", "))
}

// CreateKubeconfig renders a kubeconfig for the cluster. The emulator authenticates with a static bearer
// token instead of the `oci ce cluster generate-token` exec plugin real OKE kubeconfigs use.
func (b *InMemoryBackend) CreateKubeconfig(id string, in KubeconfigIn) ([]byte, error) {
	b.mu.Lock("CreateKubeconfig")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return nil, aerr
	}

	if in.TokenVersion != nil && *in.TokenVersion != tokenVersion2 {
		return nil, errParam("tokenVersion must be " + tokenVersion2)
	}

	if in.Expiration != nil && *in.Expiration <= 0 {
		return nil, errParam("expiration must be a positive number of seconds")
	}

	if c.State != stateActive && c.State != stateUpdating {
		return nil, errState("Cluster " + id + " is in state " + c.State + "; a kubeconfig requires ACTIVE")
	}

	server, aerr := pickEndpoint(c, in.Endpoint)
	if aerr != nil {
		return nil, aerr
	}

	if server == "" {
		return nil, errParam("Cluster " + id + " has no " + def(in.Endpoint, "reachable") + " endpoint")
	}

	return renderKubeconfig(c, server), nil
}

func renderKubeconfig(c *Cluster, server string) []byte {
	tail := ocidTail(c.ID, longTail)
	ca := base64.StdEncoding.EncodeToString([]byte(c.CAPEM))

	var sb strings.Builder

	sb.WriteString("apiVersion: v1\nkind: Config\nclusters:\n- cluster:\n")
	sb.WriteString("    certificate-authority-data: " + ca + "\n")
	sb.WriteString("    server: https://" + server + "\n")
	sb.WriteString("  name: cluster-" + tail + "\ncontexts:\n- context:\n")
	sb.WriteString("    cluster: cluster-" + tail + "\n    user: user-" + tail + "\n")
	sb.WriteString("  name: context-" + tail + "\ncurrent-context: context-" + tail + "\npreferences: {}\nusers:\n")
	sb.WriteString("- name: user-" + tail + "\n  user:\n    token: " + c.Token + "\n")

	return []byte(sb.String())
}

func (h *Handler) createKubeconfig(q *req) error {
	var in KubeconfigIn
	if len(q.body) > 0 {
		if err := q.decode(&in); err != nil {
			return err
		}
	}

	out, err := h.Backend.CreateKubeconfig(q.param("clusterId"), in)
	if err != nil {
		return err
	}

	q.header("Content-Type", "application/yaml")
	q.w.WriteHeader(http.StatusOK)
	_, _ = q.w.Write(out)

	return nil
}
