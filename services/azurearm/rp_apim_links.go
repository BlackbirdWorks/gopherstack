package azurearm

import (
	"net/http"
	"strings"
)

const (
	apimLinkProductGroup = "productGroups"
	apimLinkGroupUser    = "groupUsers"
	apimLinkMinSegs      = 3
	apimLinkItemSegs     = 4
)

// apimLinkSpec describes an association served under a parent entity: products/{p}/groups/{g} and
// groups/{g}/users/{u}, with the child contract as the body (learn.microsoft.com/rest/api/apimanagement/
// product-group and group-user).
type apimLinkSpec struct {
	parents    func(*apimService) map[string]*apimEntity
	child      apimKind
	ns         string
	childColl  string
	parentName string
	childName  string
}

func apimLinkKey(ns, parent, child string) string {
	return ns + "|" + strings.ToLower(parent) + "/" + strings.ToLower(child)
}

func apimProductGroupLink() apimLinkSpec {
	g, _ := apimGenericKind(apimGroupsType)

	return apimLinkSpec{
		parents: func(s *apimService) map[string]*apimEntity { return s.Products },
		child:   g, ns: apimLinkProductGroup, childColl: apimGroupsType, parentName: "product", childName: "group",
	}
}

func apimGroupUserLink() apimLinkSpec {
	g, _ := apimGenericKind(apimGroupsType)
	u, _ := apimGenericKind(apimUsersType)

	return apimLinkSpec{
		parents: func(s *apimService) map[string]*apimEntity { return g.store(s) },
		child:   u, ns: apimLinkGroupUser, childColl: apimUsersType, parentName: "group", childName: "user",
	}
}

func (p *APIMProvider) linked(s *apimService, req apimReq, l apimLinkSpec) (apimResp, error) {
	r := req.Rest
	if _, ok := l.parents(s)[entKey("", r[1])]; !ok {
		return apimResp{}, apimNotFound(l.parentName)
	}

	if len(r) == apimLinkMinSegs {
		if req.Method != http.MethodGet {
			return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
		}

		out := make([]map[string]any, 0)

		for _, e := range p.listKind(s, l.child, "", l.childColl) {
			if s.Links[apimLinkKey(l.ns, r[1], nameField(e))] {
				out = append(out, e)
			}
		}

		return apimResp{Status: http.StatusOK, Body: map[string]any{fieldValue: out}}, nil
	}

	if len(r) != apimLinkItemSegs {
		return apimResp{}, apimNotFound(cosmosFieldResource)
	}

	return p.linkItem(s, req, l)
}

func (p *APIMProvider) linkItem(s *apimService, req apimReq, l apimLinkSpec) (apimResp, error) {
	r := req.Rest
	key := apimLinkKey(l.ns, r[1], r[3])
	child := l.child.store(s)[entKey("", r[3])]

	switch req.Method {
	case http.MethodHead:
		if !s.Links[key] {
			return apimResp{}, apimNotFound("link")
		}

		return apimResp{Status: http.StatusNoContent}, nil
	case http.MethodPut:
		if child == nil {
			return apimResp{}, apimNotFound(l.childName)
		}

		status := http.StatusCreated
		if s.Links[key] {
			status = http.StatusOK
		}

		s.Links[key] = true
		s.bump()

		return apimResp{Status: status, Body: s.entityBody(l.child, l.childColl+"/"+child.ID, child, nil)}, nil
	case http.MethodDelete:
		if !s.Links[key] {
			return apimResp{Status: http.StatusNoContent}, nil
		}

		delete(s.Links, key)
		s.bump()

		return apimResp{Status: http.StatusOK}, nil
	}

	return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
}

// dropLinks removes every association that names the entity of the given kind and identifier.
func (s *apimService) dropLinks(ns, id string, asParent bool) {
	lid := strings.ToLower(id)

	for k := range s.Links {
		rest, ok := strings.CutPrefix(k, ns+"|")
		if !ok {
			continue
		}

		parent, child, _ := strings.Cut(rest, "/")
		if (asParent && parent == lid) || (!asParent && child == lid) {
			delete(s.Links, k)
		}
	}
}
