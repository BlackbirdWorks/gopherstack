package cognitoidp

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
)

const (
	nsDSig       = "http://www.w3.org/2000/09/xmldsig#"
	nsExcC14N    = "http://www.w3.org/2001/10/xml-exc-c14n#"
	algEnveloped = "http://www.w3.org/2000/09/xmldsig#enveloped-signature"
	algRSASHA256 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
	algRSASHA384 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha384"
	algRSASHA512 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512"
	algDigest256 = "http://www.w3.org/2001/04/xmlenc#sha256"
	algDigest384 = "http://www.w3.org/2001/04/xmldsig-more#sha384"
	algDigest512 = "http://www.w3.org/2001/04/xmlenc#sha512"
	xmlNSPrefix  = "xmlns"
	prefixXML    = "xml"
	maxXMLDepth  = 64
	maxXMLNodes  = 20000
	attrNameID   = "ID"
)

var errSAMLSignature = errors.New("saml signature")

func sigErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSAMLSignature, fmt.Sprintf(format, args...))
}

type xattr struct {
	prefix, local, value string
}

type xnode struct {
	parent   *xnode
	prefix   string
	local    string
	text     string
	attrs    []xattr
	nsDecls  map[string]string
	children []*xnode
	isText   bool
}

var errXML = errors.New("invalid xml")

func xmlErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errXML, fmt.Sprintf(format, args...))
}

type xmlBuilder struct {
	root, cur *xnode
	depth     int
}

// parseXML builds a namespace-aware tree preserving prefixes; DOCTYPE and entity declarations are rejected.
func parseXML(data []byte) (*xnode, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	dec.Strict = true

	var b xmlBuilder

	for count := 0; ; count++ {
		tok, err := dec.RawToken()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, xmlErrorf("%v", err)
		}

		if count > maxXMLNodes {
			return nil, xmlErrorf("document is too large")
		}

		if err = b.add(tok); err != nil {
			return nil, err
		}
	}

	if b.root == nil || b.cur != nil {
		return nil, xmlErrorf("document is incomplete")
	}

	return b.root, nil
}

func (b *xmlBuilder) add(tok xml.Token) error {
	switch t := tok.(type) {
	case xml.StartElement:
		return b.start(t)
	case xml.EndElement:
		if b.cur == nil || b.cur.prefix != t.Name.Space || b.cur.local != t.Name.Local {
			return xmlErrorf("end tag does not match")
		}

		b.cur = b.cur.parent
		b.depth--
	case xml.CharData:
		if b.cur != nil {
			b.cur.children = append(b.cur.children, &xnode{parent: b.cur, isText: true, text: string(t)})
		}
	case xml.Directive:
		return xmlErrorf("directives are not allowed")
	}

	return nil
}

func (b *xmlBuilder) start(t xml.StartElement) error {
	b.depth++
	if b.depth > maxXMLDepth {
		return xmlErrorf("document is nested too deeply")
	}

	n := newXNode(t, b.cur)

	switch {
	case b.cur != nil:
		b.cur.children = append(b.cur.children, n)
	case b.root != nil:
		return xmlErrorf("multiple root elements")
	default:
		b.root = n
	}

	b.cur = n

	return nil
}

func newXNode(t xml.StartElement, parent *xnode) *xnode {
	n := &xnode{parent: parent, prefix: t.Name.Space, local: t.Name.Local}

	for _, a := range t.Attr {
		switch {
		case a.Name.Space == xmlNSPrefix:
			if n.nsDecls == nil {
				n.nsDecls = map[string]string{}
			}

			n.nsDecls[a.Name.Local] = a.Value
		case a.Name.Space == "" && a.Name.Local == xmlNSPrefix:
			if n.nsDecls == nil {
				n.nsDecls = map[string]string{}
			}

			n.nsDecls[""] = a.Value
		default:
			n.attrs = append(n.attrs, xattr{prefix: a.Name.Space, local: a.Name.Local, value: a.Value})
		}
	}

	return n
}

func (n *xnode) lookupNS(prefix string) (string, bool) {
	for cur := n; cur != nil; cur = cur.parent {
		if uri, ok := cur.nsDecls[prefix]; ok {
			return uri, true
		}
	}

	return "", false
}

func (n *xnode) ns() string {
	uri, _ := n.lookupNS(n.prefix)

	return uri
}

func (n *xnode) is(ns, local string) bool { return !n.isText && n.local == local && n.ns() == ns }

func (n *xnode) attr(local string) string {
	for _, a := range n.attrs {
		if a.prefix == "" && a.local == local {
			return a.value
		}
	}

	return ""
}

func (n *xnode) child(ns, local string) *xnode {
	for _, c := range n.children {
		if c.is(ns, local) {
			return c
		}
	}

	return nil
}

func (n *xnode) childrenNamed(ns, local string) []*xnode {
	var out []*xnode

	for _, c := range n.children {
		if c.is(ns, local) {
			out = append(out, c)
		}
	}

	return out
}

func (n *xnode) textContent() string {
	var sb strings.Builder

	for _, c := range n.children {
		if c.isText {
			sb.WriteString(c.text)
		}
	}

	return strings.TrimSpace(sb.String())
}

func (n *xnode) walk(fn func(*xnode)) {
	fn(n)

	for _, c := range n.children {
		if !c.isText {
			c.walk(fn)
		}
	}
}

// canonicalize renders n as Exclusive XML Canonicalization 1.0 (no comments), omitting skip.
func canonicalize(n, skip *xnode, inclusive []string) []byte {
	var buf bytes.Buffer

	writeCanonical(&buf, n, skip, inclusive, map[string]string{})

	return buf.Bytes()
}

func writeCanonical(buf *bytes.Buffer, n, skip *xnode, inclusive []string, rendered map[string]string) {
	if n == skip {
		return
	}

	if n.isText {
		buf.WriteString(escapeC14NText(n.text))

		return
	}

	decls, child := namespaceDeclarations(n, inclusive, rendered)
	qn := qname(n.prefix, n.local)
	buf.WriteString("<" + qn)

	for _, d := range decls {
		buf.WriteString(d)
	}

	for _, a := range sortedAttrs(n) {
		buf.WriteString(" " + qname(a.prefix, a.local) + `="` + escapeC14NAttr(a.value) + `"`)
	}

	buf.WriteString(">")

	for _, c := range n.children {
		writeCanonical(buf, c, skip, inclusive, child)
	}

	buf.WriteString("</" + qn + ">")
}

// namespaceDeclarations returns the declarations n emits under exclusive C14N and the map its children inherit.
func namespaceDeclarations(n *xnode, inclusive []string, rendered map[string]string) ([]string, map[string]string) {
	used := map[string]bool{n.prefix: true}

	for _, a := range n.attrs {
		if a.prefix != "" && a.prefix != prefixXML {
			used[a.prefix] = true
		}
	}

	for _, p := range inclusive {
		if p == "#default" {
			p = ""
		}

		used[p] = true
	}

	prefixes := make([]string, 0, len(used))
	for p := range used {
		prefixes = append(prefixes, p)
	}

	sort.Strings(prefixes)

	child := maps0(rendered)
	decls := make([]string, 0, len(prefixes))

	for _, p := range prefixes {
		if d, ok := declarationFor(n, p, rendered); ok {
			decls = append(decls, d.text)
			child[p] = d.uri
		}
	}

	return decls, child
}

type nsDecl struct{ text, uri string }

func declarationFor(n *xnode, prefix string, rendered map[string]string) (nsDecl, bool) {
	if prefix == prefixXML {
		return nsDecl{}, false
	}

	uri, ok := n.lookupNS(prefix)

	if prefix == "" && (!ok || uri == "") {
		if rendered[""] != "" {
			return nsDecl{text: ` xmlns=""`}, true
		}

		return nsDecl{}, false
	}

	if !ok || (hasKey(rendered, prefix) && rendered[prefix] == uri) {
		return nsDecl{}, false
	}

	return nsDecl{text: nsDeclText(prefix, uri), uri: uri}, true
}

func sortedAttrs(n *xnode) []xattr {
	attrs := slices.Clone(n.attrs)
	sort.SliceStable(attrs, func(i, j int) bool {
		ni, nj := attrNS(n, attrs[i]), attrNS(n, attrs[j])
		if ni != nj {
			return ni < nj
		}

		return attrs[i].local < attrs[j].local
	})

	return attrs
}

func maps0(m map[string]string) map[string]string {
	out := make(map[string]string, len(m)+1)
	maps.Copy(out, m)

	return out
}

func hasKey(m map[string]string, k string) bool {
	_, ok := m[k]

	return ok
}

func attrNS(n *xnode, a xattr) string {
	if a.prefix == "" {
		return ""
	}

	if a.prefix == prefixXML {
		return "http://www.w3.org/XML/1998/namespace"
	}

	uri, _ := n.lookupNS(a.prefix)

	return uri
}

func qname(prefix, local string) string {
	if prefix == "" {
		return local
	}

	return prefix + ":" + local
}

func nsDeclText(prefix, uri string) string {
	if prefix == "" {
		return ` xmlns="` + escapeC14NAttr(uri) + `"`
	}

	return " xmlns:" + prefix + `="` + escapeC14NAttr(uri) + `"`
}

func escapeC14NText(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\r", "&#xD;").Replace(s)
}

func escapeC14NAttr(s string) string {
	return strings.NewReplacer(
		"&", "&amp;", "<", "&lt;", `"`, "&quot;", "\t", "&#x9;", "\n", "&#xA;", "\r", "&#xD;",
	).Replace(s)
}

func hashFor(alg string) (crypto.Hash, bool) {
	switch alg {
	case algRSASHA256, algDigest256:
		return crypto.SHA256, true
	case algRSASHA384, algDigest384:
		return crypto.SHA384, true
	case algRSASHA512, algDigest512:
		return crypto.SHA512, true
	default:
		return 0, false
	}
}

func digest(h crypto.Hash, data []byte) []byte {
	switch h {
	case crypto.SHA384:
		sum := sha512.Sum384(data)

		return sum[:]
	case crypto.SHA512:
		sum := sha512.Sum512(data)

		return sum[:]
	default:
		sum := sha256.Sum256(data)

		return sum[:]
	}
}

// verifiedElements returns every element whose enveloped XML-DSig signature verifies against keys.
// Only SHA-2 RSA signatures over exclusive C14N are accepted; the signed element must carry a unique ID.
func verifiedElements(root *xnode, keys []*rsa.PublicKey) ([]*xnode, error) {
	ids := map[string]int{}

	root.walk(func(n *xnode) {
		if id := n.attr(attrNameID); id != "" {
			ids[id]++
		}
	})

	var sigs []*xnode

	root.walk(func(n *xnode) {
		if n.is(nsDSig, "Signature") {
			sigs = append(sigs, n)
		}
	})

	var out []*xnode

	for _, sig := range sigs {
		signed := sig.parent
		if signed == nil {
			continue
		}

		if err := verifySignature(sig, signed, ids, keys); err != nil {
			return nil, err
		}

		out = append(out, signed)
	}

	if len(out) == 0 {
		return nil, sigErrorf("document is not signed")
	}

	return out, nil
}

func verifySignature(sig, signed *xnode, ids map[string]int, keys []*rsa.PublicKey) error {
	info := sig.child(nsDSig, "SignedInfo")
	valueNode := sig.child(nsDSig, "SignatureValue")

	if info == nil || valueNode == nil {
		return sigErrorf("signature is malformed")
	}

	incl, err := checkCanonMethod(info.child(nsDSig, "CanonicalizationMethod"))
	if err != nil {
		return err
	}

	sigAlg := info.child(nsDSig, "SignatureMethod")
	if sigAlg == nil {
		return sigErrorf("signature method is missing")
	}

	sigHash, ok := hashFor(sigAlg.attr("Algorithm"))
	if !ok || !strings.Contains(sigAlg.attr("Algorithm"), "rsa-sha") {
		return sigErrorf("unsupported signature algorithm %q", sigAlg.attr("Algorithm"))
	}

	if err = checkReference(info, signed, ids); err != nil {
		return err
	}

	sigBytes, err := base64.StdEncoding.DecodeString(stripSpace(valueNode.textContent()))
	if err != nil {
		return sigErrorf("signature value is not base64")
	}

	signedInfo := canonicalize(info, nil, incl)
	sum := digest(sigHash, signedInfo)

	for _, k := range keys {
		if rsa.VerifyPKCS1v15(k, sigHash, sum, sigBytes) == nil {
			return nil
		}
	}

	return sigErrorf("signature does not verify against the identity provider certificates")
}

// checkCanonMethod requires exclusive C14N without comments and returns its InclusiveNamespaces prefixes.
func checkCanonMethod(m *xnode) ([]string, error) {
	if m == nil || m.attr("Algorithm") != nsExcC14N {
		return nil, sigErrorf("only exclusive canonicalization is supported")
	}

	return inclusivePrefixes(m), nil
}

func inclusivePrefixes(m *xnode) []string {
	for _, c := range m.children {
		if !c.isText && c.local == "InclusiveNamespaces" {
			return strings.Fields(c.attr("PrefixList"))
		}
	}

	return nil
}

// referenceTransforms validates the transform list and returns the exclusive-C14N prefix list.
func referenceTransforms(ref *xnode) ([]string, error) {
	var incl []string

	enveloped := false

	if tr := ref.child(nsDSig, "Transforms"); tr != nil {
		for _, t := range tr.childrenNamed(nsDSig, "Transform") {
			switch t.attr("Algorithm") {
			case algEnveloped:
				enveloped = true
			case nsExcC14N:
				incl = inclusivePrefixes(t)
			default:
				return nil, sigErrorf("unsupported transform %q", t.attr("Algorithm"))
			}
		}
	}

	if !enveloped {
		return nil, sigErrorf("enveloped-signature transform is required")
	}

	return incl, nil
}

func checkReference(info, signed *xnode, ids map[string]int) error {
	refs := info.childrenNamed(nsDSig, "Reference")
	if len(refs) != 1 {
		return sigErrorf("exactly one signature reference is required")
	}

	ref := refs[0]
	id := signed.attr(attrNameID)

	if id == "" || ref.attr("URI") != "#"+id || ids[id] != 1 {
		return sigErrorf("signature reference does not identify the signed element")
	}

	incl, err := referenceTransforms(ref)
	if err != nil {
		return err
	}

	var sigNode *xnode

	for _, c := range signed.childrenNamed(nsDSig, "Signature") {
		if c.child(nsDSig, "SignedInfo") == info {
			sigNode = c
		}
	}

	if sigNode == nil {
		return sigErrorf("signature is not enveloped by the signed element")
	}

	return checkDigest(ref, canonicalize(signed, sigNode, incl))
}

func checkDigest(ref *xnode, canon []byte) error {
	dm := ref.child(nsDSig, "DigestMethod")
	dv := ref.child(nsDSig, "DigestValue")

	if dm == nil || dv == nil {
		return sigErrorf("digest is missing")
	}

	h, ok := hashFor(dm.attr("Algorithm"))
	if !ok || !strings.Contains(dm.attr("Algorithm"), "sha") {
		return sigErrorf("unsupported digest algorithm %q", dm.attr("Algorithm"))
	}

	want, err := base64.StdEncoding.DecodeString(stripSpace(dv.textContent()))
	if err != nil {
		return sigErrorf("digest value is not base64")
	}

	if subtle.ConstantTimeCompare(digest(h, canon), want) != 1 {
		return sigErrorf("digest mismatch")
	}

	return nil
}

func stripSpace(s string) string {
	return strings.Map(func(r rune) rune {
		if r == ' ' || r == '\n' || r == '\r' || r == '\t' {
			return -1
		}

		return r
	}, s)
}
