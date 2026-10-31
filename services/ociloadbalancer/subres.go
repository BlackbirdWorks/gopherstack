package ociloadbalancer

import (
	"slices"
	"strings"
)

func equalFold(a, b string) bool { return strings.EqualFold(a, b) }

func subresourceOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateListener: (*Handler).createListener,
		opUpdateListener: (*Handler).updateListener,
		opDeleteListener: deleteByName(
			"DeleteListener",
			"listenerName",
			func(lb *LoadBalancer) map[string]*Listener { return lb.Listeners },
		),
		opListListenerRules: (*Handler).listListenerRules,

		opCreateHostname: (*Handler).createHostname,
		opUpdateHostname: (*Handler).updateHostname,
		opGetHostname:    getByName("name", func(lb *LoadBalancer) map[string]*Hostname { return lb.Hostnames }),
		opListHostnames:  listAll(func(lb *LoadBalancer) map[string]*Hostname { return lb.Hostnames }),
		opDeleteHostname: deleteByName(
			"DeleteHostname",
			"name",
			func(lb *LoadBalancer) map[string]*Hostname { return lb.Hostnames },
		),

		opCreatePRS: (*Handler).createPathRouteSet,
		opUpdatePRS: (*Handler).updatePathRouteSet,
		opGetPRS: getByName(
			"pathRouteSetName",
			func(lb *LoadBalancer) map[string]*PathRouteSet { return lb.PathRouteSets },
		),
		opListPRS: listAll(func(lb *LoadBalancer) map[string]*PathRouteSet { return lb.PathRouteSets }),
		opDeletePRS: deleteByName(
			"DeletePathRouteSet",
			"pathRouteSetName",
			func(lb *LoadBalancer) map[string]*PathRouteSet { return lb.PathRouteSets },
		),

		opCreateRP: (*Handler).createRoutingPolicy,
		opUpdateRP: (*Handler).updateRoutingPolicy,
		opGetRP: getByName(
			"routingPolicyName",
			func(lb *LoadBalancer) map[string]*RoutingPolicy { return lb.RoutingPolicies },
		),
		opListRP: listAll(func(lb *LoadBalancer) map[string]*RoutingPolicy { return lb.RoutingPolicies }),
		opDeleteRP: deleteByName(
			"DeleteRoutingPolicy",
			"routingPolicyName",
			func(lb *LoadBalancer) map[string]*RoutingPolicy { return lb.RoutingPolicies },
		),

		opCreateRS: (*Handler).createRuleSet,
		opUpdateRS: (*Handler).updateRuleSet,
		opGetRS:    getByName("ruleSetName", func(lb *LoadBalancer) map[string]*RuleSet { return lb.RuleSets }),
		opListRS:   listAll(func(lb *LoadBalancer) map[string]*RuleSet { return lb.RuleSets }),
		opDeleteRS: deleteByName(
			"DeleteRuleSet",
			"ruleSetName",
			func(lb *LoadBalancer) map[string]*RuleSet { return lb.RuleSets },
		),

		opCreateSuite: (*Handler).createSuite,
		opUpdateSuite: (*Handler).updateSuite,
		opGetSuite: getByName(
			"name",
			func(lb *LoadBalancer) map[string]*SSLCipherSuite { return lb.SSLCipherSuites },
		),
		opListSuites: listAll(func(lb *LoadBalancer) map[string]*SSLCipherSuite { return lb.SSLCipherSuites }),
		opDeleteSuite: deleteByName(
			"DeleteSSLCipherSuite",
			"name",
			func(lb *LoadBalancer) map[string]*SSLCipherSuite { return lb.SSLCipherSuites },
		),

		opCreateCert: (*Handler).createCertificate,
		opListCerts:  listAll(func(lb *LoadBalancer) map[string]*Certificate { return lb.Certificates }),
		opDeleteCert: (*Handler).deleteCertificate,
	}
}

// checkListenerKey requires the listener's certificate bundle to hold a private key to terminate TLS with.
func (b *InMemoryBackend) checkListenerKey(lb *LoadBalancer, l *Listener) *APIError {
	if l.SSL == nil {
		return nil
	}

	if _, ok := lb.Certificates[l.SSL.CertificateName]; ok && b.certKey(lb.ID, l.SSL.CertificateName) == nil {
		return errParam("certificate " + l.SSL.CertificateName + " has no private key to terminate TLS with")
	}

	return nil
}

func (h *Handler) createListener(q *req) error {
	return mutateBody(h, q, "CreateListener", func(b *InMemoryBackend, lb *LoadBalancer, in *listenerIn) *APIError {
		if _, dup := lb.Listeners[in.Name]; dup {
			return errConflict("listener " + in.Name + " already exists")
		}

		l, err := buildListener(in.Name, in)
		if err != nil {
			return err
		}

		if err = b.checkListenerKey(lb, l); err != nil {
			return err
		}

		lb.Listeners[l.Name] = l

		return nil
	})
}

func (h *Handler) updateListener(q *req) error {
	name := q.param("listenerName")

	return mutateBody(h, q, "UpdateListener", func(b *InMemoryBackend, lb *LoadBalancer, in *listenerIn) *APIError {
		if _, err := lookup(lb.Listeners, name); err != nil {
			return err
		}

		l, err := buildListener(name, in)
		if err != nil {
			return err
		}

		if err = b.checkListenerKey(lb, l); err != nil {
			return err
		}

		lb.Listeners[name] = l

		return nil
	})
}

type listenerRuleSummary struct {
	RuleSetName string `json:"ruleSetName"`
	Rule        Rule   `json:"rule"`
}

func (h *Handler) listListenerRules(q *req) error {
	return readSub(h, q, func(lb *LoadBalancer) ([]listenerRuleSummary, *APIError) {
		l, err := lookup(lb.Listeners, q.param("listenerName"))
		if err != nil {
			return nil, err
		}

		out := []listenerRuleSummary{}

		for _, rsn := range l.RuleSets {
			if rs := lb.RuleSets[rsn]; rs != nil {
				for _, r := range rs.Items {
					out = append(out, listenerRuleSummary{Rule: r, RuleSetName: rsn})
				}
			}
		}

		return out, nil
	})
}

func (h *Handler) createHostname(q *req) error {
	return mutateBody(h, q, "CreateHostname", func(_ *InMemoryBackend, lb *LoadBalancer, in *hostnameIn) *APIError {
		if err := checkName("hostname", in.Name); err != nil {
			return err
		}

		if _, dup := lb.Hostnames[in.Name]; dup {
			return errConflict("hostname " + in.Name + " already exists")
		}

		if err := checkHostname(in.Hostname); err != nil {
			return err
		}

		lb.Hostnames[in.Name] = &Hostname{Name: in.Name, Hostname: in.Hostname}

		return nil
	})
}

func (h *Handler) updateHostname(q *req) error {
	name := q.param("name")

	return mutateBody(h, q, "UpdateHostname", func(_ *InMemoryBackend, lb *LoadBalancer, in *hostnameIn) *APIError {
		hn, err := lookup(lb.Hostnames, name)
		if err != nil {
			return err
		}

		if in.Hostname == "" {
			return nil
		}

		if err = checkHostname(in.Hostname); err != nil {
			return err
		}

		hn.Hostname = in.Hostname

		return nil
	})
}

func (h *Handler) createPathRouteSet(q *req) error {
	return mutateBody(
		h,
		q,
		"CreatePathRouteSet",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *pathRouteSetIn) *APIError {
			if _, dup := lb.PathRouteSets[in.Name]; dup {
				return errConflict("path route set " + in.Name + " already exists")
			}

			ps, err := buildPathRouteSet(in.Name, in)
			if err != nil {
				return err
			}

			lb.PathRouteSets[in.Name] = ps

			return nil
		},
	)
}

func (h *Handler) updatePathRouteSet(q *req) error {
	name := q.param("pathRouteSetName")

	return mutateBody(
		h,
		q,
		"UpdatePathRouteSet",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *pathRouteSetIn) *APIError {
			if _, err := lookup(lb.PathRouteSets, name); err != nil {
				return err
			}

			ps, err := buildPathRouteSet(name, in)
			if err != nil {
				return err
			}

			lb.PathRouteSets[name] = ps

			return nil
		},
	)
}

func (h *Handler) createRoutingPolicy(q *req) error {
	return mutateBody(
		h,
		q,
		"CreateRoutingPolicy",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *routingPolicyIn) *APIError {
			if _, dup := lb.RoutingPolicies[in.Name]; dup {
				return errConflict("routing policy " + in.Name + " already exists")
			}

			rp, err := buildRoutingPolicy(in.Name, in, "")
			if err != nil {
				return err
			}

			lb.RoutingPolicies[in.Name] = rp

			return nil
		},
	)
}

func (h *Handler) updateRoutingPolicy(q *req) error {
	name := q.param("routingPolicyName")

	return mutateBody(
		h,
		q,
		"UpdateRoutingPolicy",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *routingPolicyIn) *APIError {
			cur, err := lookup(lb.RoutingPolicies, name)
			if err != nil {
				return err
			}

			rp, err := buildRoutingPolicy(name, in, cur.ConditionLanguageVersion)
			if err != nil {
				return err
			}

			lb.RoutingPolicies[name] = rp

			return nil
		},
	)
}

func (h *Handler) createRuleSet(q *req) error {
	return mutateBody(h, q, "CreateRuleSet", func(_ *InMemoryBackend, lb *LoadBalancer, in *ruleSetIn) *APIError {
		if _, dup := lb.RuleSets[in.Name]; dup {
			return errConflict("rule set " + in.Name + " already exists")
		}

		rs, err := buildRuleSet(in.Name, in)
		if err != nil {
			return err
		}

		lb.RuleSets[in.Name] = rs

		return nil
	})
}

func (h *Handler) updateRuleSet(q *req) error {
	name := q.param("ruleSetName")

	return mutateBody(h, q, "UpdateRuleSet", func(_ *InMemoryBackend, lb *LoadBalancer, in *ruleSetIn) *APIError {
		if _, err := lookup(lb.RuleSets, name); err != nil {
			return err
		}

		rs, err := buildRuleSet(name, in)
		if err != nil {
			return err
		}

		lb.RuleSets[name] = rs

		return nil
	})
}

func (h *Handler) createSuite(q *req) error {
	return mutateBody(
		h,
		q,
		"CreateSSLCipherSuite",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *cipherSuiteIn) *APIError {
			if _, dup := lb.SSLCipherSuites[in.Name]; dup {
				return errConflict("SSL cipher suite " + in.Name + " already exists")
			}

			cs, err := buildCipherSuite(in.Name, in)
			if err != nil {
				return err
			}

			lb.SSLCipherSuites[in.Name] = cs

			return nil
		},
	)
}

func (h *Handler) updateSuite(q *req) error {
	name := q.param("name")

	return mutateBody(
		h,
		q,
		"UpdateSSLCipherSuite",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *cipherSuiteIn) *APIError {
			if _, err := lookup(lb.SSLCipherSuites, name); err != nil {
				return err
			}

			cs, err := buildCipherSuite(name, in)
			if err != nil {
				return err
			}

			lb.SSLCipherSuites[name] = cs

			return nil
		},
	)
}

func (h *Handler) createCertificate(q *req) error {
	return mutateBody(h, q, "CreateCertificate", func(b *InMemoryBackend, lb *LoadBalancer, in *certIn) *APIError {
		return b.createCertificate(lb, in)
	})
}

func (h *Handler) deleteCertificate(q *req) error {
	name := q.param("certificateName")

	return mutateDelete(h, q, "DeleteCertificate", func(b *InMemoryBackend, lb *LoadBalancer) *APIError {
		return b.deleteCertificate(lb, name)
	})
}

func backendSetOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateBS: (*Handler).createBackendSet,
		opUpdateBS: (*Handler).updateBackendSet,
		opGetBS: getByName(
			"backendSetName",
			func(lb *LoadBalancer) map[string]*BackendSet { return lb.BackendSets },
		),
		opListBS: listAll(func(lb *LoadBalancer) map[string]*BackendSet { return lb.BackendSets }),
		opDeleteBS: deleteByName(
			"DeleteBackendSet",
			"backendSetName",
			func(lb *LoadBalancer) map[string]*BackendSet { return lb.BackendSets },
		),
		opGetHC:    (*Handler).getHealthChecker,
		opUpdateHC: (*Handler).updateHealthChecker,
		opCreateBE: (*Handler).createBackend,
		opGetBE:    (*Handler).getBackend,
		opListBE:   (*Handler).listBackends,
		opUpdateBE: (*Handler).updateBackend,
		opDeleteBE: (*Handler).deleteBackend,
	}
}

func (h *Handler) createBackendSet(q *req) error {
	return mutateBody(h, q, "CreateBackendSet", func(_ *InMemoryBackend, lb *LoadBalancer, in *backendSetIn) *APIError {
		if _, dup := lb.BackendSets[in.Name]; dup {
			return errConflict("backend set " + in.Name + " already exists")
		}

		bs, err := buildBackendSet(in.Name, in)
		if err != nil {
			return err
		}

		lb.BackendSets[in.Name] = bs

		return nil
	})
}

func (h *Handler) updateBackendSet(q *req) error {
	name := q.param("backendSetName")

	return mutateBody(h, q, "UpdateBackendSet", func(_ *InMemoryBackend, lb *LoadBalancer, in *backendSetIn) *APIError {
		if _, err := lookup(lb.BackendSets, name); err != nil {
			return err
		}

		bs, err := buildBackendSet(name, in)
		if err != nil {
			return err
		}

		lb.BackendSets[name] = bs

		return nil
	})
}

func (h *Handler) getHealthChecker(q *req) error {
	return readSub(h, q, func(lb *LoadBalancer) (HealthChecker, *APIError) {
		bs, err := lookup(lb.BackendSets, q.param("backendSetName"))
		if err != nil {
			return HealthChecker{}, err
		}

		return bs.HealthChecker, nil
	})
}

func (h *Handler) updateHealthChecker(q *req) error {
	name := q.param("backendSetName")

	return mutateBody(
		h,
		q,
		"UpdateHealthChecker",
		func(_ *InMemoryBackend, lb *LoadBalancer, in *healthCheckerIn) *APIError {
			bs, err := lookup(lb.BackendSets, name)
			if err != nil {
				return err
			}

			hc, err := buildHealthChecker(in)
			if err != nil {
				return err
			}

			bs.HealthChecker = hc

			return nil
		},
	)
}

func findBackend(bs *BackendSet, name string) int {
	return slices.IndexFunc(bs.Backends, func(b Backend) bool { return b.Name == name })
}

func (h *Handler) createBackend(q *req) error {
	name := q.param("backendSetName")

	return mutateBody(h, q, "CreateBackend", func(_ *InMemoryBackend, lb *LoadBalancer, in *backendIn) *APIError {
		bs, err := lookup(lb.BackendSets, name)
		if err != nil {
			return err
		}

		be, err := buildBackend(*in)
		if err != nil {
			return err
		}

		if findBackend(bs, be.Name) >= 0 {
			return errConflict("backend " + be.Name + " already exists")
		}

		bs.Backends = append(bs.Backends, be)

		return nil
	})
}

func (h *Handler) getBackend(q *req) error {
	return readSub(h, q, func(lb *LoadBalancer) (Backend, *APIError) {
		bs, err := lookup(lb.BackendSets, q.param("backendSetName"))
		if err != nil {
			return Backend{}, err
		}

		i := findBackend(bs, q.param("backendName"))
		if i < 0 {
			return Backend{}, errNotFound(q.param("backendName"))
		}

		return bs.Backends[i], nil
	})
}

func (h *Handler) listBackends(q *req) error {
	return readSub(h, q, func(lb *LoadBalancer) ([]Backend, *APIError) {
		bs, err := lookup(lb.BackendSets, q.param("backendSetName"))
		if err != nil {
			return nil, err
		}

		return bs.Backends, nil
	})
}

type updateBackendIn struct {
	Weight         *int  `json:"weight"`
	Backup         *bool `json:"backup"`
	Drain          *bool `json:"drain"`
	Offline        *bool `json:"offline"`
	MaxConnections *int  `json:"maxConnections"`
}

func (h *Handler) updateBackend(q *req) error {
	set, name := q.param("backendSetName"), q.param("backendName")

	return mutateBody(h, q, "UpdateBackend", func(_ *InMemoryBackend, lb *LoadBalancer, in *updateBackendIn) *APIError {
		bs, err := lookup(lb.BackendSets, set)
		if err != nil {
			return err
		}

		i := findBackend(bs, name)
		if i < 0 {
			return errNotFound(name)
		}

		if in.Weight == nil || in.Backup == nil || in.Drain == nil || in.Offline == nil {
			return errMissing("weight, backup, drain and offline are required")
		}

		if *in.Weight < 1 || (in.MaxConnections != nil && *in.MaxConnections < 1) {
			return errParam("weight and maxConnections must be at least 1")
		}

		bs.Backends[i].Weight, bs.Backends[i].Backup = *in.Weight, *in.Backup
		bs.Backends[i].Drain, bs.Backends[i].Offline = *in.Drain, *in.Offline
		bs.Backends[i].MaxConnections = in.MaxConnections

		return nil
	})
}

func (h *Handler) deleteBackend(q *req) error {
	set, name := q.param("backendSetName"), q.param("backendName")

	return mutateDelete(h, q, "DeleteBackend", func(_ *InMemoryBackend, lb *LoadBalancer) *APIError {
		bs, err := lookup(lb.BackendSets, set)
		if err != nil {
			return err
		}

		i := findBackend(bs, name)
		if i < 0 {
			return errNotFound(name)
		}

		bs.Backends = slices.Delete(bs.Backends, i, i+1)

		return nil
	})
}
