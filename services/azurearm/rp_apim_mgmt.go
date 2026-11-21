package azurearm

import (
	"crypto/sha1" //nolint:gosec // the certificate thumbprint is a SHA-1 fingerprint by definition
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"software.sslmate.com/src/go-pkcs12"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	apimVerbError         = "error"
	apimCorrNone          = "None"
	apimCertificatesType  = "certificates"
	apimDiagnosticsType   = "diagnostics"
	apimGlobalSchemasType = "schemas"
	apimAPIDiagType       = "apis/diagnostics"
	apimAPISchemasType    = "apis/schemas"
	apimAPIReleasesType   = "apis/releases"
	apimMaxCertificates   = 100
	apimMaxCertBytes      = 1 << 20
	apimMaxSchemaBytes    = 4 << 20
	apimMaxNotes          = 500
	apimMaxLoggedHeaders  = 100
	apimMaxDiagBytes      = 8192
	apimMaxSchemas        = 1000
	apimMasterSub         = "master"
	apimRevMarker         = ";rev="
	apimMaxRevision       = 1000
	apimCurrent           = "isCurrent"
	apimRevision          = "apiRevision"
	apimRevCreated        = "createdDateTime"
	apimRevUpdated        = "updatedDateTime"
)

func apimMgmtKinds() []apimKind {
	mk := func(typ string, norm func(*apimService, string, string, map[string]any, *apimEntity) (map[string]any, error),
		view func(map[string]any) map[string]any,
	) apimKind {
		return apimKind{typ: typ, norm: norm, view: view, store: func(s *apimService) map[string]*apimEntity {
			if s.Generic == nil {
				s.Generic = map[string]map[string]*apimEntity{}
			}

			if s.Generic[typ] == nil {
				s.Generic[typ] = map[string]*apimEntity{}
			}

			return s.Generic[typ]
		}}
	}

	return []apimKind{
		mk(apimCertificatesType, normCertificate, dropSecrets("data", "password", "publicKey", "privateKey")),
		mk(apimDiagnosticsType, normDiagnostic, nil),
		mk(apimAPIDiagType, normDiagnostic, nil),
		mk(apimGlobalSchemasType, normGlobalSchema, nil),
		mk(apimAPISchemasType, normAPISchema, nil),
		mk(apimAPIReleasesType, normRelease, nil),
	}
}

func apimMgmtKind(typ string) apimKind {
	for _, k := range apimMgmtKinds() {
		if k.typ == typ {
			return k
		}
	}

	return apimKind{}
}

// seedAPIMBuiltins creates the entities a new service starts with: the built-in groups, the all-access master
// subscription and, on the classic tiers, the Starter and Unlimited products.
func seedAPIMBuiltins(s *apimService, now time.Time) {
	groups := apimMgmtGroups()
	if s.Generic == nil {
		s.Generic = map[string]map[string]*apimEntity{}
	}

	if s.Generic[apimGroupsType] == nil {
		s.Generic[apimGroupsType] = map[string]*apimEntity{}
	}

	for _, g := range groups {
		s.Generic[apimGroupsType][entKey("", g.id)] = &apimEntity{ID: g.id, Etag: 1, Props: map[string]any{
			apimCDisplayName: g.name, webDescription: g.desc, fieldType: "system", "builtIn": true,
		}}
	}

	stamp := now.UTC().Format(time.RFC3339)
	scope, _ := s.normScope("/")

	s.Subs[entKey("", apimMasterSub)] = &apimEntity{ID: apimMasterSub, Etag: 1, Props: map[string]any{
		apimCDisplayName: "Built-in all-access subscription", acrPropScope: scope, sqlPropState: apimStateActive,
		redisPropPrimaryKey: newAPIMKey(), redisPropSecondaryKey: newAPIMKey(), "allowTracing": true,
		apimCCreatedDate: stamp, apimCStartDate: stamp,
	}}

	if s.SKU == apimSKUConsumption {
		return
	}

	for _, p := range []struct {
		id, name, desc string
		policy         string
		approval       bool
	}{
		{
			id: "starter", name: "Starter",
			desc: "Subscribers will be able to run 5 calls/minute up to a maximum of 100 calls/week.",
			policy: `<policies><inbound><rate-limit calls="5" renewal-period="60" />` +
				`<quota calls="100" renewal-period="604800" />` +
				`<base /></inbound><backend><base /></backend><outbound><base /></outbound></policies>`,
		},
		{
			id: "unlimited", name: "Unlimited", approval: true,
			desc: "Subscribers have completely unlimited access to the API. Administrator approval is required.",
		},
	} {
		s.Products[entKey("", p.id)] = &apimEntity{ID: p.id, Etag: 1, Props: map[string]any{
			apimCDisplayName:          p.name,
			webDescription:            p.desc,
			sqlPropState:              "published",
			apimCSubscriptionRequired: true,
			"approvalRequired":        p.approval,
			"subscriptionsLimit":      1,
		}}

		if p.policy != "" {
			s.Policies[entKey("products/"+p.id, apimPolicyID)] = &apimEntity{
				ID: apimPolicyID, Etag: 1, Props: map[string]any{apimCFormat: apimFormatXML, fieldValue: p.policy},
			}
		}

		for _, g := range []string{"administrators", "developers"} {
			s.Links[apimLinkKey(apimLinkProductGroup, p.id, g)] = true
		}
	}
}

type apimBuiltinGroup struct{ id, name, desc string }

func apimMgmtGroups() []apimBuiltinGroup {
	return []apimBuiltinGroup{
		{
			"administrators", "Administrators",
			"Administrators is a built-in group containing the admin email account provided at the time of " +
				"service creation. Its membership is managed by the system.",
		},
		{
			"developers",
			"Developers",
			"Developers is a built-in group. Its membership is managed by the system. Signed-in users fall into this group.",
		},
		{
			"guests", "Guests",
			"Guests is a built-in group. Its membership is managed by the system. " +
				"Unauthenticated users visiting the developer portal fall into this group.",
		},
	}
}

func delBuiltinSub(_ *apimService, _, id string, _ url.Values) error {
	if strings.EqualFold(id, apimMasterSub) {
		return apimInvalid("The built-in all-access subscription cannot be deleted.")
	}

	return nil
}

func delSystemGroup(s *apimService, _, id string, _ url.Values) error {
	if g := s.Generic[apimGroupsType][entKey("", id)]; g != nil && strOf(g.Props, fieldType) == "system" {
		return apimInvalid("The system group '%s' cannot be deleted.", id)
	}

	return nil
}

func apimParseCert(raw []byte, password string) (*x509.Certificate, error) {
	if _, cert, _, err := pkcs12.DecodeChain(raw, password); err == nil && cert != nil {
		return cert, nil
	}

	if cert, err := x509.ParseCertificate(raw); err == nil {
		return cert, nil
	}

	if blk, _ := pem.Decode(raw); blk != nil && blk.Type == "CERTIFICATE" {
		return x509.ParseCertificate(blk.Bytes)
	}

	return nil, apimInvalid("The certificate data is not a valid PFX (with its password) or X.509 certificate.")
}

func normCertificate(s *apimService, _, _ string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if prev == nil && len(s.Generic[apimCertificatesType]) >= apimMaxCertificates {
		return nil, apimErr(
			http.StatusConflict,
			apimCodeConflict,
			"The service already has %d certificates.",
			apimMaxCertificates,
		)
	}

	if _, has := out["keyVault"]; has {
		return nil, apimInvalid("Key Vault certificates are not supported; upload the certificate data.")
	}

	data, err := optString(out, "data", 1, apimMaxCertBytes, true, "certificate data")
	if err != nil {
		return nil, err
	}

	raw, derr := base64.StdEncoding.DecodeString(data)
	if derr != nil {
		return nil, apimInvalid("The certificate data is not valid Base64.")
	}

	pw, err := optString(out, "password", 0, apimMaxName, false, "certificate password")
	if err != nil {
		return nil, err
	}

	cert, err := apimParseCert(raw, pw)
	if err != nil {
		return nil, err
	}

	_, hasKey := apimTLSFromPFX(raw, pw)
	out["privateKey"] = hasKey

	sum := sha1.Sum(cert.Raw) //nolint:gosec // see import

	out["subject"] = cert.Subject.String()
	out["thumbprint"] = strings.ToUpper(hex.EncodeToString(sum[:]))
	out["expirationDate"] = cert.NotAfter.UTC().Format(time.RFC3339)
	out["publicKey"] = base64.StdEncoding.EncodeToString(cert.RawSubjectPublicKeyInfo)

	return out, nil
}

func apimEnum(in map[string]any, key string, allowed ...string) error {
	if v, has := in[key]; has && v != nil {
		s, _ := v.(string)
		if !containsStr(allowed, s) {
			return apimInvalid("The %s '%v' is not valid; allowed values are %s.", key, v, strings.Join(allowed, ", "))
		}
	}

	return nil
}

func containsStr(list []string, v string) bool {
	return slices.Contains(list, v)
}

func normDiagnostic(s *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	logger, err := optString(out, "loggerId", 1, apimMaxURL, true, "diagnostic loggerId")
	if err != nil {
		return nil, err
	}

	id := logger[strings.LastIndex(logger, "/")+1:]
	if !strings.Contains(strings.ToLower(logger), "/loggers/") || s.Generic[apimLoggersType][entKey("", id)] == nil {
		return nil, apimInvalid("The loggerId '%s' does not reference an existing logger.", logger)
	}

	if err = normDiagnosticFields(out); err != nil {
		return nil, err
	}

	for _, side := range []string{"frontend", "backend"} {
		if err = normPipelineSettings(mapOf(out[side])); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func normDiagnosticFields(out map[string]any) error {
	for key, allowed := range map[string][]string{
		"alwaysLog": {"allErrors"}, "verbosity": {"verbose", "information", apimVerbError},
		"httpCorrelationProtocol": {apimCorrNone, "Legacy", "W3C"}, "operationNameFormat": {"Name", "Url"},
	} {
		if err := apimEnum(out, key, allowed...); err != nil {
			return err
		}
	}

	for _, b := range []string{"logClientIp", "metrics"} {
		if _, err := optBool(out, b, false); err != nil {
			return err
		}
	}

	sm := mapOf(out["sampling"])
	if sm == nil {
		return nil
	}

	if err := apimEnum(sm, "samplingType", "fixed"); err != nil {
		return err
	}

	if p, has := sm["percentage"]; has {
		f, ok := p.(float64)
		if !ok || f < 0 || f > 100 {
			return apimInvalid("The sampling percentage must be a number from 0 to 100.")
		}
	}

	return nil
}

func normPipelineSettings(side map[string]any) error {
	for _, dir := range []string{apimKindRequest, apimKindResponse} {
		d := mapOf(side[dir])
		if d == nil {
			continue
		}

		if h := anySlice(mapOf(d)["headers"]); len(h) > apimMaxLoggedHeaders {
			return apimInvalid("At most %d headers can be logged.", apimMaxLoggedHeaders)
		}

		if b := mapOf(d["body"]); b != nil {
			if n, has := b["bytes"]; has {
				f, ok := n.(float64)
				if !ok || f < 0 || f > apimMaxDiagBytes {
					return apimInvalid("The logged body bytes must be 0 to %d.", apimMaxDiagBytes)
				}
			}
		}
	}

	return nil
}

func apimJSONOK(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case string:
		return jsontext.Value(t).IsValid()
	}

	b, err := json.Marshal(v, jsonx.V1())

	return err == nil && len(b) <= apimMaxSchemaBytes
}

func normGlobalSchema(s *apimService, _, _ string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	typ, err := apimOneOf(out, "schemaType", "schema type", apimFormatXML, "json")
	if err != nil {
		return nil, err
	}

	if prev == nil && len(s.Generic[apimGlobalSchemasType]) >= apimMaxSchemas {
		return nil, apimErr(
			http.StatusConflict,
			apimCodeConflict,
			"The service already has %d schemas.",
			apimMaxSchemas,
		)
	}

	if _, err = optString(out, webDescription, 0, apimMaxText, false, "schema description"); err != nil {
		return nil, err
	}

	val, doc := out[fieldValue], out["document"]

	switch {
	case val == nil && doc == nil:
		return nil, apimInvalid("The schema needs a value or a document.")
	case typ == apimFormatXML:
		str, ok := val.(string)
		if !ok || len(str) > apimMaxSchemaBytes || apimXMLOK(str) != nil {
			return nil, apimInvalid("An xml schema value must be a well-formed XML string.")
		}
	case !apimJSONOK(firstAny(val, doc)):
		return nil, apimInvalid("A json schema must be valid JSON.")
	}

	return out, nil
}

func firstAny(a, b any) any {
	if a != nil {
		return a
	}

	return b
}

func normAPISchema(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	ct, err := optString(out, apimCContentTypeX, 1, apimMaxName, true, "schema contentType")
	if err != nil {
		return nil, err
	}

	mt, _, merr := mime.ParseMediaType(ct)
	if merr != nil || !strings.Contains(mt, "/") {
		return nil, apimInvalid("The schema contentType '%s' is not a valid media type.", ct)
	}

	doc := mapOf(out["document"])
	if len(doc) == 0 {
		return nil, apimInvalid("The schema document is required.")
	}

	body := firstAny(firstAny(doc[fieldValue], doc["components"]), doc["definitions"])

	switch {
	case body == nil:
		return nil, apimInvalid("The schema document needs a value, components or definitions.")
	case strings.HasSuffix(mt, "+xml") || mt == "application/xml":
		if str, ok := body.(string); !ok || apimXMLOK(str) != nil {
			return nil, apimInvalid("An XML schema document must be well-formed XML.")
		}
	case strings.HasSuffix(mt, "json") && !apimJSONOK(body):
		return nil, apimInvalid("A JSON schema document must be valid JSON.")
	}

	return out, nil
}

// apiBaseID strips the ;rev=N suffix of a revision identifier.
func apiBaseID(id string) string {
	base, _, _ := strings.Cut(id, apimRevMarker)

	return base
}

func apiRevisionOf(id string) (string, bool, error) {
	_, rev, has := strings.Cut(id, apimRevMarker)
	if !has {
		return "1", false, nil
	}

	n, err := strconv.Atoi(rev)
	if err != nil || n < 1 || n > apimMaxRevision {
		return "", true, apimInvalid("The revision of '%s' must be a number from 1 to %d.", id, apimMaxRevision)
	}

	return strconv.Itoa(n), true, nil
}

func normRelease(s *apimService, parent, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	ref, err := optString(out, "apiId", 1, apimMaxURL, true, "release apiId")
	if err != nil {
		return nil, err
	}

	target := ref[strings.LastIndex(ref, "/")+1:]
	if !strings.EqualFold(apiBaseID(target), apiBaseID(parent)) || s.APIs[entKey("", target)] == nil {
		return nil, apimInvalid("The apiId '%s' must reference an existing revision of the API '%s'.", ref, parent)
	}

	if _, err = optString(out, "notes", 0, apimMaxNotes, false, "release notes"); err != nil {
		return nil, err
	}

	stamp := apimNow(s).UTC().Format(time.RFC3339)
	out[apimCCreatedDate], out["updatedDateTime"] = stamp, stamp

	for _, a := range s.APIs {
		if strings.EqualFold(apiBaseID(a.ID), apiBaseID(parent)) {
			a.Props[apimCurrent] = strings.EqualFold(a.ID, target)
			a.Etag++
		}
	}

	return out, nil
}

func apimNow(s *apimService) time.Time {
	if s.now != nil {
		return s.now()
	}

	return time.Now()
}

// apiRevisions lists the revisions of an API as ApiRevisionContract values.
func (s *apimService) apiRevisions(id string) []map[string]any {
	var out []map[string]any

	for _, a := range s.APIs {
		if !strings.EqualFold(apiBaseID(a.ID), apiBaseID(id)) {
			continue
		}

		cur, _ := a.Props[apimCurrent].(bool)
		out = append(out, map[string]any{
			"apiId": "/apis/" + a.ID, apimRevision: strOf(a.Props, apimRevision), "isOnline": true, apimCurrent: cur,
			apimFieldDesc: strOf(a.Props, "apiRevisionDescription"), "createdDateTime": strOf(a.Props, apimRevCreated),
			"updatedDateTime": strOf(a.Props, apimRevUpdated),
		})
	}

	sort.Slice(out, func(i, j int) bool {
		ri, _ := strconv.Atoi(strOf(out[i], apimRevision))
		rj, _ := strconv.Atoi(strOf(out[j], apimRevision))

		return ri < rj
	})

	return out
}

func (p *APIMProvider) apiRevisionList(s *apimService, req apimReq) (apimResp, error) {
	if req.Method != http.MethodGet {
		return apimResp{}, apimErr(http.StatusMethodNotAllowed, apimCMethodNotAllowed, "Method not allowed.")
	}

	if _, ok := s.APIs[entKey("", req.Rest[1])]; !ok {
		return apimResp{}, apimNotFound(apimCAPI)
	}

	return apimResp{Status: http.StatusOK, Body: map[string]any{fieldValue: s.apiRevisions(req.Rest[1])}}, nil
}

func apimXMLOK(doc string) error {
	dec := xml.NewDecoder(strings.NewReader(doc))
	dec.Strict = true

	seen := false

	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		if _, ok := tok.(xml.StartElement); ok {
			seen = true
		}

		if _, ok := tok.(xml.Directive); ok {
			return errAPIMFragmentRoot
		}
	}

	if !seen {
		return errAPIMFragmentRoot
	}

	return nil
}
