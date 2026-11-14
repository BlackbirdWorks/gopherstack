package azurearm

import (
	"crypto/sha1" //nolint:gosec // certificate thumbprints are SHA-1 by definition
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"net/http"
	"sort"
	"strings"
	"time"

	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	acaDaprType   = "daprComponents"
	acaCertType   = "certificates"
	acaJavaType   = "javaComponents"
	acaActSecrets = "listsecrets"

	acaFieldComponentType = "componentType"
	acaFieldSecrets       = "secrets"
	acaFieldSecretRef     = "secretRef"
	acaFieldKeyVault      = "certificateKeyVaultProperties"
)

// storedACASub is a child of a managed environment: a Dapr component, a certificate or a Java component.
type storedACASub struct {
	Created  time.Time         `json:"created"`
	Modified time.Time         `json:"modified"`
	Tags     map[string]string `json:"tags,omitempty"`
	Props    map[string]any    `json:"props"`
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Location string            `json:"location,omitempty"`
}

func acaSubKey(typ, name string) string { return strings.ToLower(typ + "/" + name) }

func acaSubType(k acaKind) string {
	switch k {
	case acaKindDapr:
		return acaDaprType
	case acaKindCert:
		return acaCertType
	case acaKindJava:
		return acaJavaType
	case acaKindOther, acaKindEnv, acaKindStorage, acaKindApp, acaKindRevision, acaKindReplica, acaKindJob,
		acaKindExecution, acaKindAuth, acaKindSource, acaKindPool:
	}

	return ""
}

func isACASubKind(k acaKind) bool { return acaSubType(k) != "" }

func (p *ContainerAppsProvider) putSub(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putSub")
	defer p.opMu.Unlock()

	t := acaTargetOf(id)
	typ, name := acaSubType(t.kind), t.names[1]

	if !acaNameValid(name, 1, acaMaxEnvNameLen) {
		return nil, acaBad("The name '%s' is invalid.", name)
	}

	props := acaClone(mapOf(body[fieldProperties]))

	p.mu.Lock("putSub")
	defer p.mu.Unlock()

	env, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	now := p.now().UTC()
	rec := &storedACASub{Name: name, Type: typ, Created: now, Modified: now, Tags: stringTags(body[fieldTags])}

	var err error

	switch t.kind {
	case acaKindDapr:
		err = acaCheckDaprComponent(name, props)
	case acaKindCert:
		rec.Location = normLocation(strOf(body, fieldLocation))
		if rec.Location == "" {
			rec.Location = env.Location
		}

		props, err = acaCheckCert(props, now)
	case acaKindJava:
		err = acaCheckJava(env, name, props)
	case acaKindOther, acaKindEnv, acaKindStorage, acaKindApp, acaKindRevision, acaKindReplica, acaKindJob,
		acaKindExecution, acaKindAuth, acaKindSource, acaKindPool:
	}

	if err != nil {
		return nil, err
	}

	rec.Props = props

	if env.Subs == nil {
		env.Subs = map[string]*storedACASub{}
	}

	if old, exists := env.Subs[acaSubKey(typ, name)]; exists {
		rec.Created = old.Created
	}

	env.Subs[acaSubKey(typ, name)] = rec
	env.Modified = now

	return p.subBody(id, env, rec), nil
}

func acaCheckDaprComponent(name string, props map[string]any) error {
	if strOf(props, acaFieldComponentType) == "" {
		return acaBad("Dapr component '%s' requires properties.componentType.", name)
	}

	if strOf(props, "version") == "" {
		return acaBad("Dapr component '%s' requires properties.version.", name)
	}

	secrets := map[string]bool{}

	for _, s := range acaList(props[acaFieldSecrets]) {
		sm := mapOf(s)
		if strOf(sm, fieldName) == "" || (strOf(sm, fieldValue) == "") == (strOf(sm, "keyVaultUrl") == "") {
			return acaBad("Each Dapr component secret needs a name and exactly one of value or keyVaultUrl.")
		}

		secrets[strings.ToLower(strOf(sm, fieldName))] = true
	}

	for _, m := range acaList(props[acaFieldMetadata]) {
		mm := mapOf(m)
		ref := strOf(mm, acaFieldSecretRef)

		if strOf(mm, fieldName) == "" || (strOf(mm, fieldValue) == "") == (ref == "") {
			return acaBad("Each Dapr component metadata entry needs a name and exactly one of value or secretRef.")
		}

		if ref != "" && !secrets[strings.ToLower(ref)] {
			return acaBad("Dapr component metadata '%s' references secret '%s', which is not defined in secrets.",
				strOf(mm, fieldName), ref)
		}
	}

	return nil
}

func acaCheckJava(env *storedACAEnv, name string, props map[string]any) error {
	if _, ok := canonicalIn([]string{"SpringBootAdmin", "SpringCloudConfig", "SpringCloudEureka"},
		strOf(props, acaFieldComponentType)); !ok {
		return acaBad("Java component '%s' needs a componentType of SpringBootAdmin, SpringCloudConfig or "+
			"SpringCloudEureka.", name)
	}

	props[acaFieldComponentType], _ = canonicalIn([]string{"SpringBootAdmin", "SpringCloudConfig", "SpringCloudEureka"},
		strOf(props, acaFieldComponentType))

	for _, c := range acaList(props["configurations"]) {
		if strOf(mapOf(c), "propertyName") == "" {
			return acaBad("Each Java component configuration needs a propertyName.")
		}
	}

	if sc := mapOf(props["scale"]); sc != nil {
		lo, hi := intOf(sc["minReplicas"]), intOf(sc["maxReplicas"])
		if lo < 0 || (sc["maxReplicas"] != nil && hi < lo) {
			return acaBad("Java component scale needs minReplicas >= 0 and maxReplicas >= minReplicas.")
		}
	}

	if err := acaCheckJavaBinds(env, props); err != nil {
		return err
	}

	delete(props, fieldProvisioningState)

	return nil
}

func acaCheckCert(props map[string]any, now time.Time) (map[string]any, error) {
	out := map[string]any{}

	if kv := mapOf(props[acaFieldKeyVault]); kv != nil {
		if strOf(kv, "keyVaultUrl") == "" {
			return nil, acaBad("certificateKeyVaultProperties needs a keyVaultUrl.")
		}

		out[acaFieldKeyVault] = kv
		out[fieldProvisioningState] = provisioningStateSucceeded

		return out, nil
	}

	raw, err := base64.StdEncoding.DecodeString(strOf(props, fieldValue))
	if err != nil || len(raw) == 0 {
		return nil, acaBad("A certificate needs either certificateKeyVaultProperties or a base64 PFX or PEM value.")
	}

	leaf, err := acaParseCert(raw, strOf(props, "password"))
	if err != nil {
		return nil, acaBad("Failed to parse the certificate: %v.", err)
	}

	spki := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	thumb := sha1.Sum(leaf.Raw) //nolint:gosec // see import

	out["subjectName"] = leaf.Subject.CommonName
	out["issuer"] = leaf.Issuer.CommonName
	out["issueDate"] = leaf.NotBefore.UTC().Format(time.RFC3339)
	out["expirationDate"] = leaf.NotAfter.UTC().Format(time.RFC3339)
	out["thumbprint"] = strings.ToUpper(hex.EncodeToString(thumb[:]))
	out["publicKeyHash"] = base64.StdEncoding.EncodeToString(spki[:])
	out["valid"] = !now.Before(leaf.NotBefore) && now.Before(leaf.NotAfter)
	out[fieldProvisioningState] = provisioningStateSucceeded

	sans := make([]any, 0, len(leaf.DNSNames))
	for _, n := range leaf.DNSNames {
		sans = append(sans, n)
	}

	out["subjectAlternativeNames"] = sans

	return out, nil
}

func acaParseCert(raw []byte, password string) (*x509.Certificate, error) {
	if block, _ := pem.Decode(raw); block != nil && block.Type == "CERTIFICATE" {
		return x509.ParseCertificate(block.Bytes)
	}

	_, leaf, _, err := pkcs12.DecodeChain(raw, password)

	return leaf, err
}

func (p *ContainerAppsProvider) subBody(id ResourceID, env *storedACAEnv, s *storedACASub) map[string]any {
	props := acaClone(s.Props)
	props[fieldProvisioningState] = provisioningStateSucceeded

	if s.Type == acaDaprType {
		for _, sec := range acaList(props[acaFieldSecrets]) {
			delete(mapOf(sec), fieldValue)
		}
	}

	sid := acaResourceID(id.SubscriptionID, id.ResourceGroup, []string{acaEnvType, s.Type},
		[]string{env.Name, s.Name})

	out := map[string]any{
		"id": sid.ARMID(), fieldName: s.Name, fieldType: namespaceMicrosoftApp + "/" + acaEnvType + "/" + s.Type,
		fieldProperties: props, acaFieldSystemData: map[string]any{},
	}

	if s.Type == acaCertType {
		out[fieldLocation], out[fieldTags] = s.Location, tagsOrEmpty(s.Tags)
	}

	return out
}

func (p *ContainerAppsProvider) subGet(id ResourceID, t acaTarget) (map[string]any, error) {
	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s, ok := e.Subs[acaSubKey(acaSubType(t.kind), t.names[1])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return p.subBody(id, e, s), nil
}

func (p *ContainerAppsProvider) listSubs(id ResourceID, t acaTarget) ([]map[string]any, error) {
	e, ok := p.envs[acaKey(id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	typ := acaSubType(t.kind)
	keys := make([]string, 0, len(e.Subs))

	for k, s := range e.Subs {
		if s.Type == typ {
			keys = append(keys, k)
		}
	}

	out := make([]map[string]any, 0, len(keys))

	for _, k := range sortedKeysOf(keys) {
		out = append(out, p.subBody(id, e, e.Subs[k]))
	}

	return out, nil
}

func (p *ContainerAppsProvider) deleteSub(id ResourceID, t acaTarget) error {
	p.opMu.Lock("deleteSub")
	defer p.opMu.Unlock()

	p.mu.Lock("deleteSub")
	defer p.mu.Unlock()

	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return ErrResourceNotFound
	}

	key := acaSubKey(acaSubType(t.kind), t.names[1])
	if _, ok = e.Subs[key]; !ok {
		return ErrResourceNotFound
	}

	if t.kind == acaKindJava {
		for _, o := range e.Subs {
			for _, b := range acaList(o.Props["serviceBinds"]) {
				if strings.EqualFold(strOf(mapOf(b), "serviceId"), id.ARMID()) {
					return netErr(
						http.StatusConflict,
						acaCodeInvalid,
						"Java component '%s' is bound to Java component '%s' and cannot be deleted.",
						t.names[1],
						o.Name,
					)
				}
			}
		}
	}

	delete(e.Subs, key)

	return nil
}

// patchSub applies PATCH: certificates take tags only, Java components merge their properties.
func (p *ContainerAppsProvider) patchSub(id ResourceID, t acaTarget, patch map[string]any) (map[string]any, error) {
	if t.kind == acaKindCert {
		return p.patchCertTags(id, t, stringTags(patch[fieldTags]))
	}

	p.mu.RLock("patchSub")
	cur, err := p.subGet(id, t)
	p.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	acaMergePatch(cur, patch)

	return p.putSub(id, cur)
}

func (p *ContainerAppsProvider) patchCertTags(
	id ResourceID,
	t acaTarget,
	tags map[string]string,
) (map[string]any, error) {
	p.mu.Lock("patchCertTags")
	defer p.mu.Unlock()

	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s, ok := e.Subs[acaSubKey(acaCertType, t.names[1])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s.Tags, s.Modified = tags, p.now().UTC()

	return p.subBody(id, e, s), nil
}

func (p *ContainerAppsProvider) listDaprSecrets(id ResourceID) (map[string]any, error) {
	p.mu.RLock("listDaprSecrets")
	defer p.mu.RUnlock()

	t := acaTargetOf(id)
	if t.kind != acaKindDapr || t.listing {
		return nil, ErrResourceNotFound
	}

	e, ok := p.envs[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	s, ok := e.Subs[acaSubKey(acaDaprType, t.names[1])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	out := make([]any, 0)

	for _, sec := range acaList(s.Props[acaFieldSecrets]) {
		sm := mapOf(sec)
		out = append(out, map[string]any{fieldName: strOf(sm, fieldName), fieldValue: strOf(sm, fieldValue)})
	}

	return map[string]any{fieldValue: out}, nil
}

func sortedKeysOf(keys []string) []string {
	sort.Strings(keys)

	return keys
}

func acaCheckJavaBinds(env *storedACAEnv, props map[string]any) error {
	for _, b := range acaList(props["serviceBinds"]) {
		bm := mapOf(b)
		target := strOf(bm, "serviceId")

		if strOf(bm, fieldName) == "" || target == "" {
			return acaBad("Each Java component service bind needs a name and a serviceId.")
		}

		tid, err := ParseGenericResourcePath(target)
		tt := acaTargetOf(tid)

		if err != nil || tt.kind != acaKindJava || len(tt.names) != 2 || !strings.EqualFold(tt.names[0], env.Name) ||
			env.Subs[acaSubKey(acaJavaType, tt.names[1])] == nil {
			return acaBad("Java component service bind '%s' targets '%s', which is not a Java component of this "+
				"environment.", strOf(bm, fieldName), target)
		}
	}

	return nil
}
