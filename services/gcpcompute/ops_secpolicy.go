package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

var secPolicyPatchDrop = []string{ //nolint:gochecknoglobals // read-only lookup table
	"rules", "type", jsonLabels, "labelFingerprint", "associations", "id", jsonKind, jsonSelfLink, jsonCreated,
	"region", "parent", "shortName",
}

func rulePriority(q map[string][]string) (int64, *APIError) {
	vals := q["priority"]
	if len(vals) == 0 {
		return 0, nil
	}

	p, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		return 0, errField("priority", vals[0], "")
	}

	return p, nil
}

func ruleIndex(p *computeapi.SecurityPolicy, prio int64) (int, *APIError) {
	i := slices.IndexFunc(p.Rules, func(r *computeapi.SecurityPolicyRule) bool { return r.Priority == prio })
	if i < 0 {
		return 0, errNotFound(trimURL(p.SelfLink) + "/rules/" + strconv.FormatInt(prio, 10))
	}

	return i, nil
}

// maskedBody returns the body keys a patch applies; names in the updateMask but absent from the body are cleared.
func maskedBody(raw map[string]any, mask string, drop []string) map[string]any {
	out := map[string]any{}

	for k, v := range raw {
		if !slices.Contains(drop, k) {
			out[k] = v
		}
	}

	for path := range strings.SplitSeq(mask, ",") {
		top, _, _ := strings.Cut(strings.TrimSpace(path), ".")
		if _, set := raw[top]; top != "" && !set && !slices.Contains(drop, top) {
			out[top] = nil
		}
	}

	return out
}

func (b *InMemoryBackend) mutateSecPolicy(
	c lbCtx, name, verb string, edit func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError),
) (*computeapi.Operation, error) {
	return lbMutate(b, secPolicySpec(), c, name, verb, nil, edit)
}

func (b *InMemoryBackend) validateSecPolicyEdit(
	c lbCtx, name string, edit func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError),
) (*computeapi.Operation, error) {
	b.mu.Lock("ValidateSecurityPolicy")
	defer b.mu.Unlock()

	cur, ok := b.secPolicies[key(c.project, scopeGlobal, name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, scopeGlobal, "", collSecPolicy, name))
	}

	next, err := edit(clone(cur))
	if err == nil {
		err = prepSecurityPolicy(b, c, next, nil)
	}

	if err != nil {
		return nil, err
	}

	return b.newOpLocked(
		opPrefix+collSecPolicy+".addRule",
		lbTargetOf(c, collSecPolicy, name, cur.Id),
		b.timing.Done,
	), nil
}

func (b *InMemoryBackend) addSecPolicyRule(
	c lbCtx, name string, rule *computeapi.SecurityPolicyRule, validateOnly bool,
) (*computeapi.Operation, error) {
	edit := func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError) {
		cur.Rules = append(cur.Rules, clone(rule))

		return cur, nil
	}

	if validateOnly {
		return b.validateSecPolicyEdit(c, name, edit)
	}

	return b.mutateSecPolicy(c, name, "addRule", edit)
}

func (b *InMemoryBackend) getSecPolicyRule(c lbCtx, name string, prio int64) (*computeapi.SecurityPolicyRule, error) {
	p, err := lbGet(b, secPolicySpec(), c, name)
	if err != nil {
		return nil, err
	}

	i, ierr := ruleIndex(p, prio)
	if ierr != nil {
		return nil, ierr
	}

	return p.Rules[i], nil
}

func (b *InMemoryBackend) patchSecPolicyRule(
	c lbCtx, name string, prio int64, raw map[string]any, mask string,
) (*computeapi.Operation, error) {
	return b.mutateSecPolicy(
		c,
		name,
		"patchRule",
		func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError) {
			i, err := ruleIndex(cur, prio)
			if err != nil {
				return nil, err
			}

			next, merr := mergeInto(cur.Rules[i], maskedBody(raw, mask, []string{"priority", jsonKind}))
			if merr != nil {
				return nil, merr
			}

			next.Priority = prio
			cur.Rules[i] = next

			return cur, nil
		},
	)
}

func (b *InMemoryBackend) removeSecPolicyRule(c lbCtx, name string, prio int64) (*computeapi.Operation, error) {
	return b.mutateSecPolicy(
		c,
		name,
		"removeRule",
		func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError) {
			i, err := ruleIndex(cur, prio)
			if err != nil {
				return nil, err
			}

			if prio == defaultRulePrio {
				return nil, errField("priority", strconv.FormatInt(prio, 10), "The default rule cannot be removed.")
			}

			cur.Rules = slices.Delete(cur.Rules, i, i+1)

			return cur, nil
		},
	)
}

func (b *InMemoryBackend) patchSecPolicy(
	c lbCtx, name string, raw map[string]any, mask string,
) (*computeapi.Operation, error) {
	return b.mutateSecPolicy(
		c,
		name,
		"patch",
		func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError) {
			if fp, ok := raw[jsonFingerp].(string); ok && fp != "" && fp != cur.Fingerprint {
				return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
			}

			return mergeInto(cur, maskedBody(raw, mask, append([]string{jsonFingerp, jsonName}, secPolicyPatchDrop...)))
		},
	)
}

func (b *InMemoryBackend) setSecPolicyLabels(c lbCtx, name string, req labelsRequest) (*computeapi.Operation, error) {
	return b.mutateSecPolicy(
		c,
		name,
		"setLabels",
		func(cur *computeapi.SecurityPolicy) (*computeapi.SecurityPolicy, *APIError) {
			labels, fp, err := applyLabels(cur.Labels, cur.LabelFingerprint, req)
			if err != nil {
				return nil, err
			}

			cur.Labels, cur.LabelFingerprint = labels, fp

			return cur, nil
		},
	)
}

func (h *Handler) registerSecurityOps() {
	g := scopeGlobal
	sp := secPolicySpec()

	registerLB(h, sp, lbOpts[computeapi.SecurityPolicy]{scopes: []string{g}})

	h.reg(collSecPolicy, g, verbPatch, func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.SecurityPolicy](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.patchSecPolicy(ctxOf(rq), rq.rt.name, raw, rq.q.Get("updateMask")))
	})
	h.reg(collSecPolicy, g, "addRule", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.SecurityPolicyRule](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.addSecPolicyRule(ctxOf(rq), rq.rt.name, in, rq.q.Get("validateOnly") == "true"))
	})
	h.reg(collSecPolicy, g, "getRule", func(h *Handler, rq *request) (any, error) {
		prio, err := rulePriority(rq.q)
		if err != nil {
			return nil, err
		}

		return h.Backend.getSecPolicyRule(ctxOf(rq), rq.rt.name, prio)
	})
	h.reg(collSecPolicy, g, "patchRule", func(h *Handler, rq *request) (any, error) {
		_, raw, err := decode[computeapi.SecurityPolicyRule](rq)
		if err != nil {
			return nil, err
		}

		prio, perr := rulePriority(rq.q)
		if perr != nil {
			return nil, perr
		}

		return opResult(h.Backend.patchSecPolicyRule(ctxOf(rq), rq.rt.name, prio, raw, rq.q.Get("updateMask")))
	})
	h.reg(collSecPolicy, g, "removeRule", func(h *Handler, rq *request) (any, error) {
		prio, err := rulePriority(rq.q)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.removeSecPolicyRule(ctxOf(rq), rq.rt.name, prio))
	})
	h.reg(collSecPolicy, g, "setLabels", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[labelsRequest](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.setSecPolicyLabels(ctxOf(rq), rq.rt.name, *in))
	})

	regSetter(h, bsSpec(), []string{g}, "setSecurityPolicy",
		func(cur *computeapi.BackendService, r *computeapi.SecurityPolicyReference) {
			cur.SecurityPolicy = r.SecurityPolicy
		})
}
