package azurearm

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	aksStateCreating  = "Creating"
	aksStateUpdating  = "Updating"
	aksStateStarting  = "Starting"
	aksStateCanceled  = "Canceled"
	aksCodeNoOp       = "OperationNotAllowed"
	aksCodeRotateFail = "ClusterCertificateRotationFailed"
	aksAbortWait      = 30 * time.Second
	aksSecretMaxLen   = 256
)

// aksOp is the long-running operation a cluster is busy with; abort cancels it.
type aksOp struct {
	cancel  context.CancelFunc
	done    chan struct{}
	aborted atomic.Bool
}

func (o *aksOp) wasAborted() bool { return o.aborted.Load() }

// beginOp marks the cluster busy with an operation in state. The returned context is cancelled by abort; end must
// be called once the operation has finished and reports whether it was aborted.
func (p *ContainerServiceProvider) beginOp(
	ctx context.Context, c *storedAKSCluster, state string,
) (context.Context, func() bool) {
	ctx, cancel := context.WithCancel(ctx)
	op := &aksOp{cancel: cancel, done: make(chan struct{})}
	key := aksKey(c.ResourceGroup, c.Name)

	p.mu.Lock("beginOp")
	p.ops[key] = op
	c.ProvState = state
	p.mu.Unlock()

	return ctx, func() bool {
		p.mu.Lock("endOp")
		delete(p.ops, key)

		c.ProvState = ""
		if op.wasAborted() {
			c.ProvState = aksStateCanceled
		}

		c.Modified = p.now().UTC()
		p.mu.Unlock()

		close(op.done)
		cancel()

		return op.wasAborted()
	}
}

// abortLatest cancels the cluster's running operation and waits for it to wind down; the cluster then reports
// Canceled. With nothing running the answer is the 409 the API documents.
func (p *ContainerServiceProvider) abortLatest(ctx context.Context, id ResourceID) error {
	name := id.LeafName()

	p.mu.RLock("abortLatest")
	_, ok := p.clusters[aksKey(id.ResourceGroup, name)]
	op := p.ops[aksKey(id.ResourceGroup, name)]
	p.mu.RUnlock()

	if !ok {
		return aksNotFound(id.ResourceGroup, name)
	}

	if op == nil {
		return netErr(http.StatusConflict, aksCodeNoOp,
			"There is no running operation to abort on managed cluster '%s': it completed before it could be "+
				"canceled.", name)
	}

	op.aborted.Store(true)
	op.cancel()

	wait := time.NewTimer(aksAbortWait)
	defer wait.Stop()

	select {
	case <-op.done:
	case <-ctx.Done():
	case <-wait.C:
	}

	return nil
}

// rotateCertificates implements rotateClusterCertificates. With the engine the node's cluster CAs are replaced
// (k3s certificate rotate-ca) and everything reissued under them; otherwise the emulated CA and token are
// replaced, so kubeconfigs fetched before no longer match.
func (p *ContainerServiceProvider) rotateCertificates(ctx context.Context, id ResourceID) error {
	p.opMu.Lock("rotateCertificates")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.LeafName())
	if !ok {
		return aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if c.Stopped {
		return aksStoppedErr(c.Name)
	}

	if p.eng.enabled() {
		opCtx, end := p.beginOp(ctx, c, aksStateUpdating)
		err := p.eng.rotate(opCtx, aksKey(c.ResourceGroup, c.Name))

		if aborted := end(); aborted {
			return nil
		}

		return err
	}

	ca, err := k3sengine.NewCA("gopherstack-aks-" + c.Name + "-ca")
	if err != nil {
		return netErr(
			http.StatusInternalServerError,
			aksCodeRotateFail,
			"Generating the new cluster CA failed: %v",
			err,
		)
	}

	p.mu.Lock("rotateCertificates")
	defer p.mu.Unlock()

	c.CACert, c.Token, c.Modified = string(ca.CertPEM), aksRandomToken(), p.now().UTC()

	return nil
}

// rotate replaces the live node's cluster CAs; the node comes back serving the new one.
func (e *aksEngine) rotate(ctx context.Context, key string) error {
	e.mu.Lock("rotate")
	lv, ok := e.live[key]
	core := e.core

	if !ok || !lv.ready || core == nil {
		e.mu.Unlock()

		return netErr(http.StatusConflict, aksCodeStarting,
			"The API server of the managed cluster is not running, so its certificates cannot be rotated.")
	}

	lv.ready = false
	id, addr := lv.inst.ContainerID, lv.addr
	e.mu.Unlock()

	got, err := core.RotateCertificates(ctx, id, addr)

	e.mu.Lock("rotate")
	defer e.mu.Unlock()

	if e.live[key] != lv {
		return nil
	}

	if err != nil {
		lv.ready = errors.Is(err, container.ErrExecUnsupported) || errors.Is(err, k3sengine.ErrRotateFailed)
		if ctx.Err() != nil {
			return nil
		}

		return netErr(
			http.StatusInternalServerError,
			aksCodeRotateFail,
			"Rotating the cluster certificates failed: %v",
			err,
		)
	}

	lv.ca, lv.ready = got, true

	return nil
}

// resetAADProfile implements resetAADProfile for legacy (non-managed) AAD integration.
func (p *ContainerServiceProvider) resetAADProfile(id ResourceID, body map[string]any) error {
	p.opMu.Lock("resetAADProfile")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.LeafName())
	if !ok {
		return aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if c.Stopped {
		return aksStoppedErr(c.Name)
	}

	if body == nil {
		return aksBad("The request body must be a ManagedClusterAADProfile.")
	}

	if acaBool(body, "managed") || acaBool(mapOf(c.Props["aadProfile"]), "managed") {
		return netErr(http.StatusBadRequest, aksCodeNoOp,
			"Resetting the AAD profile is only allowed for legacy AAD integration; managed cluster '%s' uses "+
				"AKS-managed Azure AD.", c.Name)
	}

	for _, k := range []string{"clientAppID", "serverAppID", "tenantID"} {
		if v := strOf(body, k); len(v) > aksSecretMaxLen {
			return aksBad("%s is too long.", k)
		}
	}

	prof := acaClone(body)
	if prof == nil {
		prof = map[string]any{}
	}

	p.mu.Lock("resetAADProfile")
	defer p.mu.Unlock()

	c.Props["aadProfile"] = prof
	c.Modified = p.now().UTC()

	return nil
}

// resetServicePrincipalProfile implements resetServicePrincipalProfile; only clusters running on a service
// principal accept it.
func (p *ContainerServiceProvider) resetServicePrincipalProfile(id ResourceID, body map[string]any) error {
	p.opMu.Lock("resetServicePrincipalProfile")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.LeafName())
	if !ok {
		return aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if c.Stopped {
		return aksStoppedErr(c.Name)
	}

	if strOf(body, "clientId") == "" {
		return aksBad("servicePrincipalProfile.clientId is required.")
	}

	if cur := mapOf(
		c.Props["servicePrincipalProfile"],
	); cur == nil ||
		strings.EqualFold(strOf(cur, "clientId"), "msi") {
		return netErr(http.StatusBadRequest, aksCodeNoOp,
			"Resetting the service principal profile is not allowed: managed cluster '%s' does not use a service "+
				"principal.", c.Name)
	}

	p.mu.Lock("resetServicePrincipalProfile")
	defer p.mu.Unlock()

	sp := acaClone(mapOf(c.Props["servicePrincipalProfile"]))
	sp["clientId"] = strOf(body, "clientId")

	if secret := strOf(body, "secret"); secret != "" {
		sp["secret"] = secret
	}

	c.Props["servicePrincipalProfile"] = sp
	c.Modified = p.now().UTC()

	return nil
}

// SetClock replaces the provider's time source; call it before the provider serves requests.
func (p *ContainerServiceProvider) SetClock(now func() time.Time) { p.now = now }
