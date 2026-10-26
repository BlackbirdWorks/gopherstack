package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

func ctxOf(rq *request) lbCtx {
	return lbCtx{project: rq.rt.project, scope: rq.rt.scope, loc: rq.rt.loc}
}

type lbOpts[T any] struct {
	del      func(b *InMemoryBackend, c lbCtx, name string) (*computeapi.Operation, error)
	get      handlerFn
	agg      []string
	scopes   []string
	patch    bool
	put      bool
	noInsert bool
}

func registerLB[T any](h *Handler, sp lbSpec[T], o lbOpts[T]) {
	list := func(b *InMemoryBackend, p, s, l string) []entry { return lbList(b, sp, p, s, l) }

	for i, scope := range o.scopes {
		aggKind := ""
		if i == 0 {
			aggKind = sp.kind + "AggregatedList"
		}

		h.scopedList(sp.coll, scope, sp.kind+"List", aggKind, sp.coll, o.agg, list)

		get := o.get
		if get == nil {
			get = func(h *Handler, rq *request) (any, error) { return lbGet(h.Backend, sp, ctxOf(rq), rq.rt.name) }
		}

		h.reg(sp.coll, scope, verbGet, get)
		h.reg(sp.coll, scope, verbDelete, func(h *Handler, rq *request) (any, error) {
			if o.del != nil {
				return opResult(o.del(h.Backend, ctxOf(rq), rq.rt.name))
			}

			return opResult(lbDelete(h.Backend, sp, ctxOf(rq), rq.rt.name, nil))
		})

		if !o.noInsert {
			h.reg(sp.coll, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
				in, raw, err := decode[T](rq)
				if err != nil {
					return nil, err
				}

				return opResult(lbInsert(h.Backend, sp, ctxOf(rq), in, raw))
			})
		}

		if o.patch {
			h.reg(sp.coll, scope, verbPatch, func(h *Handler, rq *request) (any, error) {
				_, raw, err := decode[T](rq)
				if err != nil {
					return nil, err
				}

				return opResult(lbPatch(h.Backend, sp, ctxOf(rq), rq.rt.name, raw))
			})
		}

		if o.put {
			h.reg(sp.coll, scope, verbUpdate, func(h *Handler, rq *request) (any, error) {
				in, raw, err := decode[T](rq)
				if err != nil {
					return nil, err
				}

				return opResult(lbReplace(h.Backend, sp, ctxOf(rq), rq.rt.name, in, raw))
			})
		}
	}
}

// regSetter registers an action that edits one field of a stored resource from a small request body.
func regSetter[T, R any](h *Handler, sp lbSpec[T], scopes []string, verb string, apply func(cur *T, r *R)) {
	for _, scope := range scopes {
		h.reg(sp.coll, scope, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[R](rq)
			if err != nil {
				return nil, err
			}

			return opResult(lbMutate(h.Backend, sp, ctxOf(rq), rq.rt.name, verb, nil,
				func(cur *T) (*T, *APIError) {
					apply(cur, in)

					return cur, nil
				}))
		})
	}
}

func globalAndRegions() []string { return append(regionScopes(), scopeGlobal) }

func (h *Handler) registerLBOps() {
	h.registerBackendOps()
	h.registerURLMapOps()
	h.registerProxyOps()
	h.registerCertOps()
	h.registerRuleOps()
	h.registerNEGOps()
}

func (h *Handler) registerBackendOps() {
	both := []string{scopeGlobal, scopeRegion}

	registerLB(h, bsSpec(), lbOpts[computeapi.BackendService]{
		scopes: both, agg: globalAndRegions(), patch: true, put: true,
	})

	for _, scope := range both {
		h.reg(collBackendSvc, scope, "getHealth", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.ResourceGroupReference](rq)
			if err != nil {
				return nil, err
			}

			return h.Backend.backendServiceHealth(ctxOf(rq), rq.rt.name, in.Group)
		})
	}

	h.reg(collBackendSvc, scopeGlobal, "addSignedUrlKey", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.SignedUrlKey](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.addBackendServiceKey(ctxOf(rq), rq.rt.name, in))
	})
	h.reg(collBackendSvc, scopeGlobal, "deleteSignedUrlKey", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteBackendServiceKey(ctxOf(rq), rq.rt.name, rq.q.Get("keyName")))
	})

	registerLB(h, bucketSpec(), lbOpts[computeapi.BackendBucket]{
		scopes: []string{scopeGlobal}, agg: nil, patch: true, put: true,
	})
	h.reg(collBackendBucket, scopeGlobal, "addSignedUrlKey", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.SignedUrlKey](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.addBackendBucketKey(ctxOf(rq), rq.rt.name, in))
	})
	h.reg(collBackendBucket, scopeGlobal, "deleteSignedUrlKey", func(h *Handler, rq *request) (any, error) {
		return opResult(h.Backend.deleteBackendBucketKey(ctxOf(rq), rq.rt.name, rq.q.Get("keyName")))
	})
}

func (h *Handler) registerURLMapOps() {
	both := []string{scopeGlobal, scopeRegion}

	registerLB(h, urlMapSpec(), lbOpts[computeapi.UrlMap]{
		scopes: both, agg: globalAndRegions(), patch: true, put: true,
	})

	for _, scope := range both {
		h.reg(collURLMaps, scope, "validate", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.UrlMapsValidateRequest](rq)
			if err != nil {
				return nil, err
			}

			return h.Backend.validateURLMap(ctxOf(rq), in)
		})
	}
}

func (h *Handler) registerProxyOps() {
	both := []string{scopeGlobal, scopeRegion}
	global := []string{scopeGlobal}

	registerLB(h, httpProxySpec(), lbOpts[computeapi.TargetHttpProxy]{scopes: both, agg: globalAndRegions()})
	regSetter(h, httpProxySpec(), both, "setUrlMap", func(cur *computeapi.TargetHttpProxy, r *computeapi.UrlMapReference) {
		cur.UrlMap = r.UrlMap
	})

	registerLB(h, httpsProxySpec(), lbOpts[computeapi.TargetHttpsProxy]{scopes: both, agg: globalAndRegions()})
	regSetter(h, httpsProxySpec(), both, "setUrlMap", func(cur *computeapi.TargetHttpsProxy, r *computeapi.UrlMapReference) {
		cur.UrlMap = r.UrlMap
	})
	regSetter(h, httpsProxySpec(), both, "setSslCertificates", func(cur *computeapi.TargetHttpsProxy, r *computeapi.TargetHttpsProxiesSetSslCertificatesRequest) {
		cur.SslCertificates = r.SslCertificates
	})
	regSetter(h, httpsProxySpec(), global, "setSslPolicy", func(cur *computeapi.TargetHttpsProxy, r *computeapi.SslPolicyReference) {
		cur.SslPolicy = r.SslPolicy
	})
	regSetter(h, httpsProxySpec(), global, "setQuicOverride", func(cur *computeapi.TargetHttpsProxy, r *computeapi.TargetHttpsProxiesSetQuicOverrideRequest) {
		cur.QuicOverride = r.QuicOverride
	})

	registerLB(h, tcpProxySpec(), lbOpts[computeapi.TargetTcpProxy]{scopes: both, agg: globalAndRegions()})
	regSetter(h, tcpProxySpec(), both, "setBackendService", func(cur *computeapi.TargetTcpProxy, r *computeapi.TargetTcpProxiesSetBackendServiceRequest) {
		cur.Service = r.Service
	})
	regSetter(h, tcpProxySpec(), both, "setProxyHeader", func(cur *computeapi.TargetTcpProxy, r *computeapi.TargetTcpProxiesSetProxyHeaderRequest) {
		cur.ProxyHeader = r.ProxyHeader
	})

	registerLB(h, sslProxySpec(), lbOpts[computeapi.TargetSslProxy]{scopes: global})
	regSetter(h, sslProxySpec(), global, "setBackendService", func(cur *computeapi.TargetSslProxy, r *computeapi.TargetSslProxiesSetBackendServiceRequest) {
		cur.Service = r.Service
	})
	regSetter(h, sslProxySpec(), global, "setProxyHeader", func(cur *computeapi.TargetSslProxy, r *computeapi.TargetSslProxiesSetProxyHeaderRequest) {
		cur.ProxyHeader = r.ProxyHeader
	})
	regSetter(h, sslProxySpec(), global, "setSslCertificates", func(cur *computeapi.TargetSslProxy, r *computeapi.TargetSslProxiesSetSslCertificatesRequest) {
		cur.SslCertificates = r.SslCertificates
	})
	regSetter(h, sslProxySpec(), global, "setSslPolicy", func(cur *computeapi.TargetSslProxy, r *computeapi.SslPolicyReference) {
		cur.SslPolicy = r.SslPolicy
	})
}

func (h *Handler) registerCertOps() {
	registerLB(h, sslCertSpec(), lbOpts[computeapi.SslCertificate]{
		scopes: []string{scopeGlobal, scopeRegion}, agg: globalAndRegions(),
	})
	registerLB(h, sslPolicySpec(), lbOpts[computeapi.SslPolicy]{
		scopes: []string{scopeGlobal}, patch: true,
		get: func(h *Handler, rq *request) (any, error) {
			if rq.rt.name == "listAvailableFeatures" {
				return &computeapi.SslPoliciesListAvailableFeaturesResponse{Features: append([]string{}, sslFeatures...)}, nil
			}

			return lbGet(h.Backend, sslPolicySpec(), ctxOf(rq), rq.rt.name)
		},
	})
}

func (h *Handler) registerRuleOps() {
	both := []string{scopeGlobal, scopeRegion}

	registerLB(h, fwdRuleSpec(), lbOpts[computeapi.ForwardingRule]{scopes: both, agg: globalAndRegions()})

	for _, scope := range both {
		h.reg(collFwdRules, scope, "setTarget", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.TargetReference](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.setRuleTarget(ctxOf(rq), rq.rt.name, in.Target))
		})
		h.reg(collFwdRules, scope, "setLabels", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[labelsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.setRuleLabels(ctxOf(rq), rq.rt.name, *in))
		})
	}
}

func (h *Handler) registerNEGOps() {
	scopes := []string{scopeZone, scopeGlobal, scopeRegion}

	registerLB(h, negSpec(), lbOpts[computeapi.NetworkEndpointGroup]{
		scopes: scopes, agg: append(append(zoneScopes(), regionScopes()...), scopeGlobal),
		del: (*InMemoryBackend).deleteNEG,
	})

	for _, scope := range []string{scopeZone, scopeGlobal} {
		h.reg(collNEG, scope, "attachNetworkEndpoints", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.NetworkEndpointGroupsAttachEndpointsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.attachEndpoints(ctxOf(rq), rq.rt.name, in.NetworkEndpoints))
		})
		h.reg(collNEG, scope, "detachNetworkEndpoints", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.NetworkEndpointGroupsDetachEndpointsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.detachEndpoints(ctxOf(rq), rq.rt.name, in.NetworkEndpoints))
		})
		h.reg(collNEG, scope, "listNetworkEndpoints", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[computeapi.NetworkEndpointGroupsListEndpointsRequest](rq)
			if err != nil {
				return nil, err
			}

			entries, lerr := h.Backend.listEndpoints(ctxOf(rq), rq.rt.name, in.HealthStatus == "SHOW")
			if lerr != nil {
				return nil, lerr
			}

			return listBody(rq, "compute#networkEndpointGroupsListNetworkEndpoints", entries)
		})
	}
}
