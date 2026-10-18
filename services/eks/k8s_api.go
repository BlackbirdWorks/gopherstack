package eks

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	maxK8sBody        = 8 << 20
	k8sFieldManager   = "gopherstack-eks"
	managedByLabel    = "app.kubernetes.io/managed-by"
	managedByValue    = "gopherstack-eks"
	contentTypeApply  = "application/apply-patch+yaml"
	contentTypeMerge  = "application/merge-patch+json"
	rbacAPIPath       = "/apis/rbac.authorization.k8s.io/v1"
	k8sRequestTimeout = 30 * time.Second
	k8sIdleConns      = 4

	keyAPIVersion = "apiVersion"
	keyKind       = "kind"
	keyMetadata   = "metadata"
	keyLabels     = "labels"
	keyCode       = "code"
	keyMessage    = "message"
	arnSvcIAM     = "iam"
	arnSvcSTS     = "sts"
)

var (
	errK8sNotFound = errors.New("kubernetes object not found")
	errK8sStatus   = errors.New("kubernetes API returned an error status")
)

// k8sAPI is a minimal admin client for a cluster's k3s API server.
type k8sAPI struct {
	client *http.Client
	tr     *http.Transport
	base   string
	token  string
}

func newK8sAPI(addr, token string, caPEM []byte) (*k8sAPI, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%w: cluster CA is not a certificate", errNotReady)
	}

	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		MaxIdleConnsPerHost: k8sIdleConns,
		IdleConnTimeout:     time.Minute,
	}

	return &k8sAPI{
		base:   "https://" + addr,
		token:  token,
		tr:     tr,
		client: &http.Client{Transport: tr, Timeout: k8sRequestTimeout},
	}, nil
}

func (a *k8sAPI) close() { a.tr.CloseIdleConnections() }

func (a *k8sAPI) do(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Accept", "application/json")

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}

	defer func() { _ = resp.Body.Close() }()

	out, err := io.ReadAll(io.LimitReader(resp.Body, maxK8sBody))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return out, fmt.Errorf("%w: %s", errK8sNotFound, path)
	case resp.StatusCode >= http.StatusMultipleChoices:
		return out, fmt.Errorf("%w: %s %s returned %d: %.200s", errK8sStatus, method, path, resp.StatusCode, out)
	}

	return out, nil
}

func (a *k8sAPI) getJSON(ctx context.Context, path string, out any) error {
	raw, err := a.do(ctx, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}

	return nil
}

// apply server-side-applies obj (JSON is valid YAML) at path, forcing field ownership.
func (a *k8sAPI) apply(ctx context.Context, path string, obj any) error {
	raw, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}

	q := url.Values{"fieldManager": {k8sFieldManager}, "force": {"true"}}
	_, err = a.do(ctx, http.MethodPatch, path+"?"+q.Encode(), contentTypeApply, raw)

	return err
}

func (a *k8sAPI) mergePatch(ctx context.Context, path string, patch any) error {
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}

	_, err = a.do(ctx, http.MethodPatch, path, contentTypeMerge, raw)

	return err
}

func (a *k8sAPI) remove(ctx context.Context, path string) error {
	_, err := a.do(ctx, http.MethodDelete, path, "", nil)
	if errors.Is(err, errK8sNotFound) {
		return nil
	}

	return err
}

type k8sMeta struct {
	Labels    map[string]string `json:"labels"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
}

type k8sCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type k8sTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

type k8sNode struct {
	Metadata k8sMeta `json:"metadata"`
	Spec     struct {
		Taints []k8sTaint `json:"taints"`
	} `json:"spec"`
	Status struct {
		Conditions []k8sCondition `json:"conditions"`
	} `json:"status"`
}

func (n *k8sNode) ready() bool {
	for _, c := range n.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status == "True"
		}
	}

	return false
}

func (a *k8sAPI) listNodes(ctx context.Context) ([]k8sNode, error) {
	var list struct {
		Items []k8sNode `json:"items"`
	}

	if err := a.getJSON(ctx, "/api/v1/nodes", &list); err != nil {
		return nil, err
	}

	return list.Items, nil
}

func (a *k8sAPI) listManaged(ctx context.Context, collection string) ([]k8sMeta, error) {
	var list struct {
		Items []struct {
			Metadata k8sMeta `json:"metadata"`
		} `json:"items"`
	}

	q := url.Values{"labelSelector": {managedByLabel + "=" + managedByValue}}
	if err := a.getJSON(ctx, collection+"?"+q.Encode(), &list); err != nil {
		return nil, err
	}

	out := make([]k8sMeta, len(list.Items))
	for i, it := range list.Items {
		out[i] = it.Metadata
	}

	return out, nil
}

func (a *k8sAPI) deploymentAvailable(ctx context.Context, namespace, name string) (bool, error) {
	var d struct {
		Status struct {
			AvailableReplicas int `json:"availableReplicas"`
		} `json:"status"`
	}

	err := a.getJSON(ctx, "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+name, &d)
	if err != nil {
		if errors.Is(err, errK8sNotFound) {
			return false, nil
		}

		return false, err
	}

	return d.Status.AvailableReplicas > 0, nil
}

// getConfigMapData returns a ConfigMap's data map; a missing ConfigMap yields an empty map.
func (a *k8sAPI) getConfigMapData(ctx context.Context, namespace, name string) (map[string]string, error) {
	var cm struct {
		Data map[string]string `json:"data"`
	}

	err := a.getJSON(ctx, "/api/v1/namespaces/"+namespace+"/configmaps/"+name, &cm)
	if err != nil {
		if errors.Is(err, errK8sNotFound) {
			return map[string]string{}, nil
		}

		return nil, err
	}

	return cm.Data, nil
}
