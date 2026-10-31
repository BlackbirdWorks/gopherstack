package azurearm

import (
	"crypto/subtle"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

const (
	apimItemDepth      = 2
	apimActionDepth    = 3
	apimProductAPIsLen = 3
	apimAPIChildLen    = 3
	apimOpPolicyLen    = 5
	apimAPIChildIdx    = 2
	apimOpPolicyIdx    = 4
	apimMaxShortID     = 80
)

// apimReq is one control-plane request below Microsoft.ApiManagement/service/{name}.
type apimReq struct {
	Body    map[string]any
	Query   url.Values
	Method  string
	Sub     string
	RG      string
	Svc     string
	IfMatch string
	Rest    []string
}

// apimResp is the answer: Body nil means an empty body.
type apimResp struct {
	Body    any
	Etag    string
	OpID    string
	Status  int
	Retry   bool
	OpAsync bool
}

// apimKind describes one family of child entities.
type apimKind struct {
	norm  func(s *apimService, parent, id string, in map[string]any, prev *apimEntity) (map[string]any, error)
	view  func(map[string]any) map[string]any
	del   func(s *apimService, parent, id string, q url.Values) error
	store func(s *apimService) map[string]*apimEntity
	typ   string
}

func apimIDCapped(typ string) bool {
	return typ == apimCBackends || typ == "apis/operations"
}

func entKey(parent, id string) string {
	if parent == "" {
		return strings.ToLower(id)
	}

	return strings.ToLower(parent) + "/" + strings.ToLower(id)
}

func apimKinds() []apimKind {
	return []apimKind{
		apiKind(), opKind(), productKind(), subKind(), namedKind(), backendKind(),
		policyKind(apimCPolicies), policyKind("apis/policies"), policyKind("apis/operations/policies"),
		{typ: "products/apis"},
	}
}

func apiKind() apimKind {
	return apimKind{
		typ: apimCApis, norm: normAPI, del: delAPI,
		store: func(s *apimService) map[string]*apimEntity { return s.APIs },
	}
}

func opKind() apimKind {
	return apimKind{
		typ: "apis/operations", norm: normOperation, del: delOperation,
		store: func(s *apimService) map[string]*apimEntity { return s.Ops },
	}
}

func productKind() apimKind {
	return apimKind{
		typ: apimCProducts, norm: normProduct, del: delProduct,
		store: func(s *apimService) map[string]*apimEntity { return s.Products },
	}
}

func subKind() apimKind {
	return apimKind{
		typ: sbSubscriptionsType, norm: normSubscription, view: viewSubscription,
		store: func(s *apimService) map[string]*apimEntity { return s.Subs },
	}
}

func namedKind() apimKind {
	return apimKind{
		typ: "namedValues", norm: normNamedValue, view: viewNamedValue,
		store: func(s *apimService) map[string]*apimEntity { return s.Named },
	}
}

func backendKind() apimKind {
	return apimKind{
		typ: apimCBackends, norm: normBackend, view: viewBackend,
		store: func(s *apimService) map[string]*apimEntity { return s.Backends },
	}
}

func policyKind(typ string) apimKind {
	return apimKind{
		typ: typ, norm: normPolicy,
		store: func(s *apimService) map[string]*apimEntity { return s.Policies },
	}
}

// Do serves one control-plane request.
func (p *APIMProvider) Do(req apimReq) (apimResp, error) {
	if req.Method == http.MethodGet || req.Method == http.MethodHead {
		p.mu.RLock("Do")
		defer p.mu.RUnlock()
	} else {
		p.mu.Lock("Do")
		defer p.mu.Unlock()
	}

	if len(req.Rest) == 0 {
		return p.doService(req)
	}

	if isAPIMOperationResult(req.Rest) && req.Method == http.MethodGet {
		return p.operationResult(req)
	}

	s, ok := p.svcs[apimKey(req.Sub, req.RG, req.Svc)]
	if !ok {
		return apimResp{}, apimNotFound("API Management service")
	}

	if req.Method != http.MethodGet && req.Method != http.MethodHead && s.state(p.now()) != provisioningStateSucceeded {
		return apimResp{}, apimBusy()
	}

	return p.route(s, req)
}

func (p *APIMProvider) doService(req apimReq) (apimResp, error) {
	key := apimKey(req.Sub, req.RG, req.Svc)

	switch req.Method {
	case http.MethodGet, http.MethodHead:
		s, ok := p.svcs[key]
		if !ok {
			return apimResp{}, apimNotFound("API Management service")
		}

		return apimResp{Status: http.StatusOK, Body: s.body(p.now()), Etag: apimEtag(s.Etag)}, nil
	case http.MethodPut, http.MethodPatch:
		s, created, err := p.writeService(req.Sub, req.RG, req.Svc, req.Body, req.Method == http.MethodPatch)
		if err != nil {
			return apimResp{}, err
		}

		if created || req.Method == http.MethodPatch {
			return p.asyncWrite(s, req.Method, created), nil
		}

		return apimResp{Status: http.StatusOK, Body: s.body(p.now()), Etag: apimEtag(s.Etag), Retry: true}, nil
	case http.MethodDelete:
		s, ok := p.svcs[key]
		if !ok {
			return apimResp{Status: http.StatusNoContent}, nil
		}

		delete(p.svcs, key)

		return apimResp{
			Status: http.StatusAccepted, Retry: true,
			OpID: p.startOp(key, http.MethodDelete, s.Location, p.now().Add(p.fastActivate)),
		}, nil
	}

	return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
}

func (p *APIMProvider) route(s *apimService, req apimReq) (apimResp, error) {
	r := req.Rest

	switch strings.ToLower(r[0]) {
	case apimCApis:
		return p.routeAPI(s, req)
	case apimCPolicies:
		return p.scoped(s, req, policyKind(apimCPolicies), "", 0)
	case apimCProducts:
		if len(r) >= apimProductAPIsLen && strings.EqualFold(r[2], apimCApis) {
			return p.productAPIs(s, req)
		}

		return p.collection(s, req, productKind(), "", 0)
	case sbSubscriptionsType:
		return p.collection(s, req, subKind(), "", 0)
	case "namedvalues":
		return p.collection(s, req, namedKind(), "", 0)
	case apimCBackends:
		return p.collection(s, req, backendKind(), "", 0)
	}

	return apimResp{}, apimNotFound(cosmosFieldResource)
}

func (p *APIMProvider) routeAPI(s *apimService, req apimReq) (apimResp, error) {
	r := req.Rest
	if len(r) < apimAPIChildLen {
		return p.collection(s, req, apiKind(), "", 0)
	}

	switch {
	case strings.EqualFold(r[2], nsOperations) && len(r) >= apimOpPolicyLen && strings.EqualFold(r[4], apimCPolicies):
		return p.scoped(
			s,
			req,
			policyKind("apis/operations/policies"),
			"apis/"+r[1]+"/operations/"+r[3],
			apimOpPolicyIdx,
		)
	case strings.EqualFold(r[2], nsOperations):
		return p.collection(s, req, opKind(), r[1], apimAPIChildIdx)
	case strings.EqualFold(r[2], apimCPolicies):
		return p.scoped(s, req, policyKind("apis/policies"), "apis/"+r[1], apimAPIChildIdx)
	}

	return p.collection(s, req, apiKind(), "", 0)
}

// collection serves the list and item operations of a kind whose collection segment sits at index at (the
// segments before it identify the parent api).
func (p *APIMProvider) collection(s *apimService, req apimReq, k apimKind, parent string, at int) (apimResp, error) {
	r := req.Rest
	path := strings.Join(r[:at+1], "/")

	if parent != "" {
		if _, ok := s.APIs[entKey("", parent)]; !ok {
			return apimResp{}, apimNotFound(apimCAPI)
		}
	}

	switch len(r) - at {
	case 1:
		if req.Method != http.MethodGet {
			return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
		}

		return apimResp{Status: http.StatusOK, Body: map[string]any{fieldValue: p.listKind(s, k, parent, path)}}, nil
	case apimItemDepth:
		return p.item(s, req, k, parent, r[at+1], path+"/"+r[at+1])
	case apimActionDepth:
		return p.action(s, req, k, r[at+1], r[at+2])
	}

	return apimResp{}, apimNotFound(cosmosFieldResource)
}

// scoped serves a policy document at the given scope; its collection and item segments follow the scope.
func (p *APIMProvider) scoped(s *apimService, req apimReq, k apimKind, scope string, at int) (apimResp, error) {
	r := req.Rest

	if err := p.policyScopeExists(s, r); err != nil {
		return apimResp{}, err
	}

	path := strings.Join(r[:at+1], "/")

	switch len(r) - at {
	case 1:
		if req.Method != http.MethodGet {
			return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
		}

		var out []map[string]any
		if ent, ok := s.Policies[entKey(scope, apimPolicyID)]; ok {
			out = append(out, s.entityBody(k, path+"/"+apimPolicyID, ent, req.Query))
		}

		return apimResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
	case apimItemDepth:
		if !strings.EqualFold(r[at+1], apimPolicyID) {
			return apimResp{}, apimInvalid("The policy identifier must be %q.", apimPolicyID)
		}

		return p.item(s, req, k, scope, apimPolicyID, path+"/"+apimPolicyID)
	}

	return apimResp{}, apimNotFound(cosmosFieldResource)
}

func (p *APIMProvider) policyScopeExists(s *apimService, r []string) error {
	if len(r) >= 2 && strings.EqualFold(r[0], apimCApis) {
		if _, ok := s.APIs[entKey("", r[1])]; !ok {
			return apimNotFound(apimCAPI)
		}

		if len(r) >= 4 && strings.EqualFold(r[2], nsOperations) {
			if _, ok := s.Ops[entKey(r[1], r[3])]; !ok {
				return apimNotFound("operation")
			}
		}
	}

	return nil
}

func (p *APIMProvider) listKind(s *apimService, k apimKind, parent, path string) []map[string]any {
	ents := make([]*apimEntity, 0)

	for _, e := range k.store(s) {
		if strings.EqualFold(e.Parent, parent) {
			ents = append(ents, e)
		}
	}

	sort.Slice(ents, func(i, j int) bool { return strings.ToLower(ents[i].ID) < strings.ToLower(ents[j].ID) })

	out := make([]map[string]any, 0, len(ents))
	for _, e := range ents {
		out = append(out, s.entityBody(k, path+"/"+e.ID, e, nil))
	}

	return out
}

func (s *apimService) entityBody(k apimKind, path string, e *apimEntity, q url.Values) map[string]any {
	props := e.Props
	if k.view != nil {
		props = k.view(props)
	}

	if q != nil && q.Get(apimCFormat) == apimCRawxml && props != nil && k.view == nil {
		props = cloneAnyMap(props)
		props[apimCFormat] = apimCRawxml
	}

	return map[string]any{
		cosmosFieldID:   s.armBase() + "/" + path,
		fieldName:       e.ID,
		fieldType:       apimARMType + "/" + k.typ,
		fieldProperties: props,
	}
}

func checkIfMatch(have, want string) error {
	if want == "" || want == "*" || have == want {
		return nil
	}

	return apimErr(http.StatusPreconditionFailed, apimCodePrecondition,
		"The entity has been modified; the If-Match header does not match its current ETag.")
}

func (p *APIMProvider) item(
	s *apimService, req apimReq, k apimKind, parent, id, path string,
) (apimResp, error) {
	if !apimChildIDRE.MatchString(id) || (apimIDCapped(k.typ) && len(id) > apimMaxShortID) {
		return apimResp{}, apimInvalid("The identifier %q is not valid.", id)
	}

	store := k.store(s)
	key := entKey(parent, id)
	prev := store[key]

	switch req.Method {
	case http.MethodGet, http.MethodHead:
		if prev == nil {
			return apimResp{}, apimNotFound(apimCEntity)
		}

		return apimResp{
			Status: http.StatusOK,
			Body:   s.entityBody(k, path, prev, req.Query),
			Etag:   apimEtag(prev.Etag),
		}, nil
	case http.MethodDelete:
		return p.deleteItem(s, req, k, parent, id)
	case http.MethodPut, http.MethodPatch:
		return p.writeItem(s, req, k, parent, id, path)
	}

	return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
}

func (p *APIMProvider) deleteItem(s *apimService, req apimReq, k apimKind, parent, id string) (apimResp, error) {
	store := k.store(s)
	key := entKey(parent, id)

	prev := store[key]
	if prev == nil {
		return apimResp{Status: http.StatusNoContent}, nil
	}

	if req.IfMatch == "" {
		return apimResp{}, apimErr(http.StatusPreconditionFailed, apimCodePrecondition,
			"The If-Match header is required to delete this entity; use If-Match: * to match any ETag.")
	}

	if err := checkIfMatch(apimEtag(prev.Etag), req.IfMatch); err != nil {
		return apimResp{}, err
	}

	if k.del != nil {
		if err := k.del(s, parent, id, req.Query); err != nil {
			return apimResp{}, err
		}
	}

	delete(store, key)
	s.bump()

	return apimResp{Status: http.StatusOK}, nil
}

func (p *APIMProvider) writeItem(s *apimService, req apimReq, k apimKind, parent, id, path string) (apimResp, error) {
	store := k.store(s)
	key := entKey(parent, id)
	prev := store[key]
	patch := req.Method == http.MethodPatch

	if prev == nil && patch {
		return apimResp{}, apimNotFound(apimCEntity)
	}

	if prev != nil {
		if err := checkIfMatch(apimEtag(prev.Etag), req.IfMatch); err != nil {
			return apimResp{}, err
		}
	}

	in := cloneAnyMap(propsOf(req.Body))
	if patch {
		merged := cloneAnyMap(prev.Props)
		maps.Copy(merged, in)

		in = merged
	}

	props, err := k.norm(s, parent, id, in, prev)
	if err != nil {
		return apimResp{}, err
	}

	ent := &apimEntity{ID: id, Parent: parent, Props: props, Etag: 1}
	if prev != nil {
		ent.ID, ent.Etag = prev.ID, prev.Etag+1
	}

	store[key] = ent
	s.bump()

	status := http.StatusOK
	if prev == nil {
		status = http.StatusCreated
	}

	return apimResp{Status: status, Body: s.entityBody(k, path, ent, nil), Etag: apimEtag(ent.Etag)}, nil
}

// action serves POST listSecrets, regeneratePrimaryKey, regenerateSecondaryKey (subscriptions) and listValue
// (named values).
func (p *APIMProvider) action(s *apimService, req apimReq, k apimKind, id, verb string) (apimResp, error) {
	if req.Method != http.MethodPost {
		return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
	}

	ent := k.store(s)[entKey("", id)]
	if ent == nil {
		return apimResp{}, apimNotFound(apimCEntity)
	}

	switch {
	case k.typ == sbSubscriptionsType && strings.EqualFold(verb, "listSecrets"):
		return apimResp{Status: http.StatusOK, Etag: apimEtag(ent.Etag), Body: map[string]any{
			redisPropPrimaryKey:   ent.Props[redisPropPrimaryKey],
			redisPropSecondaryKey: ent.Props[redisPropSecondaryKey],
		}}, nil
	case k.typ == sbSubscriptionsType && strings.EqualFold(verb, "regeneratePrimaryKey"):
		ent.Props = withProp(ent.Props, redisPropPrimaryKey, newAPIMKey())
		s.bump()

		return apimResp{Status: http.StatusNoContent}, nil
	case k.typ == sbSubscriptionsType && strings.EqualFold(verb, "regenerateSecondaryKey"):
		ent.Props = withProp(ent.Props, redisPropSecondaryKey, newAPIMKey())
		s.bump()

		return apimResp{Status: http.StatusNoContent}, nil
	case k.typ == "namedValues" && strings.EqualFold(verb, "listValue"):
		return apimResp{
			Status: http.StatusOK, Etag: apimEtag(ent.Etag), Body: map[string]any{fieldValue: ent.Props[fieldValue]},
		}, nil
	}

	return apimResp{}, apimNotFound(cosmosFieldResource)
}

func (p *APIMProvider) productAPIs(s *apimService, req apimReq) (apimResp, error) {
	r := req.Rest
	if _, ok := s.Products[entKey("", r[1])]; !ok {
		return apimResp{}, apimNotFound("product")
	}

	prefix := strings.ToLower(r[1]) + "/"

	if len(r) == apimProductAPIsLen {
		if req.Method != http.MethodGet {
			return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
		}

		out := make([]map[string]any, 0)

		for _, e := range p.listKind(s, apiKind(), "", apimCApis) {
			if s.ProductAPIs[prefix+strings.ToLower(nameField(e))] {
				out = append(out, e)
			}
		}

		return apimResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
	}

	return p.productAPILink(s, req, prefix)
}

func (p *APIMProvider) productAPILink(s *apimService, req apimReq, prefix string) (apimResp, error) {
	aid := req.Rest[3]
	linkKey := prefix + strings.ToLower(aid)
	api := s.APIs[entKey("", aid)]

	switch req.Method {
	case http.MethodHead:
		if !s.ProductAPIs[linkKey] {
			return apimResp{}, apimNotFound("link")
		}

		return apimResp{Status: http.StatusNoContent}, nil
	case http.MethodPut:
		if api == nil {
			return apimResp{}, apimNotFound(apimCAPI)
		}

		status := http.StatusCreated
		if s.ProductAPIs[linkKey] {
			status = http.StatusOK
		}

		s.ProductAPIs[linkKey] = true
		s.bump()

		return apimResp{Status: status, Body: s.entityBody(apiKind(), "apis/"+api.ID, api, nil)}, nil
	case http.MethodDelete:
		if !s.ProductAPIs[linkKey] {
			return apimResp{Status: http.StatusNoContent}, nil
		}

		delete(s.ProductAPIs, linkKey)
		s.bump()

		return apimResp{Status: http.StatusOK}, nil
	}

	return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
}

func keysEqual(a, b string) bool {
	return a != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func withProp(props map[string]any, k string, v any) map[string]any {
	out := cloneAnyMap(props)
	out[k] = v

	return out
}
