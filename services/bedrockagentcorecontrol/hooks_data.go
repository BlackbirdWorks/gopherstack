package bedrockagentcorecontrol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	versionIDLen      = 32
	scanInitial       = 1 << 16
	scanMax           = 1 << 24
	maxInlineExamples = 1000
	draftVersion      = "DRAFT"
	mainBranch        = "mainline"
	failedCreate      = "CREATE_FAILED"
)

func exampleList(r *Resource) []any {
	l, _ := r.Aux2()["examples"].([]any)

	return l
}

func (b *InMemoryBackend) newExample(e any) (map[string]any, error) {
	m, ok := e.(map[string]any)
	if !ok {
		return nil, invalid("each example must be a JSON object")
	}

	out := make(map[string]any, len(m)+1)
	maps.Copy(out, m)

	if strOf(out, "exampleId") == "" {
		out["exampleId"] = awsid.NewN("ex-", idSuffixLen)
	}

	return out, nil
}

func validateDatasetInput(in args) error {
	src := in.obj("source")
	if src == nil {
		return nil
	}

	kind, ok := exactlyOne(src, []string{"inlineExamples", "s3Source"})
	if !ok {
		return invalid("source must set exactly one of inlineExamples or s3Source")
	}

	if kind == "inlineExamples" {
		n := len(listOf(objOf(src, "inlineExamples"), "examples"))
		if n == 0 || n > maxInlineExamples {
			return invalid("inlineExamples must contain between 1 and %d examples", maxInlineExamples)
		}

		return nil
	}

	if _, _, err := splitS3URI(strOf(objOf(src, "s3Source"), "s3Uri")); err != nil {
		return err
	}

	return nil
}

func splitS3URI(uri string) (string, string, error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	bucket, key, found := strings.Cut(rest, "/")

	if !ok || !found || bucket == "" || key == "" {
		return "", "", invalid("s3Uri %q must look like s3://bucket/key.jsonl", uri)
	}

	return bucket, key, nil
}

// loadExamples resolves a dataset source into example documents; a non-empty reason marks the dataset CREATE_FAILED.
func (b *InMemoryBackend) loadExamples(source map[string]any) ([]any, string) {
	if inline := objOf(source, "inlineExamples"); inline != nil {
		out := make([]any, 0, len(listOf(inline, "examples")))

		for _, e := range listOf(inline, "examples") {
			ex, err := b.newExample(e)
			if err != nil {
				return nil, err.Error()
			}

			out = append(out, ex)
		}

		return out, ""
	}

	bucket, key, _ := splitS3URI(strOf(objOf(source, "s3Source"), "s3Uri"))
	if b.refs == nil {
		return nil, "the S3 object " + bucket + "/" + key + " could not be read"
	}

	data, ok := b.refs.ReadObject(bucket, key)
	if !ok {
		return nil, "the S3 object " + bucket + "/" + key + " does not exist"
	}

	var out []any

	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, scanInitial), scanMax)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		var m any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return nil, fmt.Sprintf("line %d of the S3 object is not valid JSON", len(out)+1)
		}

		ex, err := b.newExample(m)
		if err != nil {
			return nil, err.Error()
		}

		out = append(out, ex)
	}

	return out, ""
}

func (b *InMemoryBackend) decorateDataset(r *Resource, in args) {
	examples, reason := b.loadExamples(in.obj("source"))
	delete(r.Doc, "source")

	r.Doc["datasetVersion"] = draftVersion
	r.Doc["draftStatus"] = draftUnmodified
	r.Doc["exampleCount"] = float64(len(examples))
	r.Aux2()["examples"] = examples
	r.Aux2()["versionSeq"] = float64(0)

	if reason != "" {
		r.Status = failedCreate
		r.Doc["failureReason"] = reason
	}
}

func (b *InMemoryBackend) datasetVersionView(r *Resource, version string) (*Resource, error) {
	if version == "" || version == draftVersion {
		return r, nil
	}

	for _, v := range r.Versions {
		if v.ID == version {
			c := cloneResource(r)
			c.Doc = cloneDoc(v.Doc)
			c.Created = v.Created

			return c, nil
		}
	}

	return nil, notFound("dataset %q has no version %q", r.ID, version)
}

func (b *InMemoryBackend) deleteDatasetVersion(r *Resource, version string) (bool, error) {
	if version == "" || version == draftVersion {
		return false, nil
	}

	if !slices.ContainsFunc(r.Versions, func(v Version) bool { return v.ID == version }) {
		return false, notFound("dataset %q has no version %q", r.ID, version)
	}

	r.Versions = slices.DeleteFunc(r.Versions, func(v Version) bool { return v.ID == version })

	return true, nil
}

func (b *InMemoryBackend) decorateHarness(r *Resource) {
	r.Doc["harnessVersion"] = "1"
	r.Versions = []Version{{ID: "1", Created: r.Created, Doc: cloneDoc(r.Doc)}}
}

func (b *InMemoryBackend) applyHarnessUpdate(r *Resource, patch map[string]any) map[string]any {
	mergeDoc(r, patch)

	n, _ := strconv.Atoi(strOf(r.Doc, "harnessVersion"))
	next := strconv.Itoa(n + 1)
	r.Doc["harnessVersion"] = next
	r.Versions = append(r.Versions, Version{ID: next, Created: b.now(), Doc: cloneDoc(r.Doc)})

	return nil
}

func (b *InMemoryBackend) harnessVersionView(r *Resource, version string) (*Resource, error) {
	if version == "" || version == strOf(r.Doc, "harnessVersion") {
		return r, nil
	}

	for _, v := range r.Versions {
		if v.ID == version {
			c := cloneResource(r)
			c.Doc, c.Updated = cloneDoc(v.Doc), v.Created

			return c, nil
		}
	}

	return nil, notFound("harness %q has no version %q", r.ID, version)
}

func (b *InMemoryBackend) decorateHarnessEndpoint(sc scope, r *Resource) error {
	h, err := b.mustFind(sc, b.kinds[kindHarness], "", r.Parent)
	if err != nil {
		return err
	}

	v := strOf(r.Doc, "targetVersion")
	if v == "" {
		v = strOf(h.Doc, "harnessVersion")
	}

	if !slices.ContainsFunc(h.Versions, func(x Version) bool { return x.ID == v }) {
		return notFound("harness %q has no version %q", h.ID, v)
	}

	r.Doc["targetVersion"], r.Doc["liveVersion"] = v, v
	r.Doc[keyHarnessID] = h.ID

	return nil
}

func (b *InMemoryBackend) validateHarnessEndpointUpdate(r *Resource, patch map[string]any) error {
	v, ok := patch["targetVersion"].(string)
	if !ok {
		return nil
	}

	h, found := b.resources.Get(makeKey(r.Account, r.Region, kindHarness, "", r.Parent))
	if !found || !slices.ContainsFunc(h.Versions, func(x Version) bool { return x.ID == v }) {
		return notFound("harness %q has no version %q", r.Parent, v)
	}

	return nil
}

func validateBundleInput(in args) error {
	if len(in.obj("components")) == 0 {
		return invalid("components must not be empty")
	}

	return nil
}

func (b *InMemoryBackend) decorateBundle(r *Resource, in args) {
	branch := in.str(keyBranchName)
	if branch == "" {
		branch = mainBranch
	}

	vid := awsid.NewN("", versionIDLen)
	meta := map[string]any{keyBranchName: branch, keyParentVersionIDs: []any{}}

	for _, f := range []string{keyCommitMessage, keyCreatedBy} {
		if v, ok := r.Doc[f]; ok {
			meta[f] = v
		}
	}

	for _, f := range []string{keyBranchName, keyCommitMessage, keyCreatedBy} {
		delete(r.Doc, f)
	}

	r.Doc["versionId"], r.Doc["lineageMetadata"] = vid, meta
	r.Versions = []Version{{ID: vid, Branch: branch, Created: r.Created, Doc: cloneDoc(r.Doc)}}
}

func validateBundleUpdate(r *Resource, patch map[string]any) error {
	parents, _ := patch[keyParentVersionIDs].([]any)
	if len(parents) == 0 {
		return invalid("parentVersionIds must not be empty")
	}

	for _, p := range parents {
		id, _ := p.(string)
		if !slices.ContainsFunc(r.Versions, func(v Version) bool { return v.ID == id }) {
			return notFound("configuration bundle %q has no version %q", r.ID, id)
		}
	}

	return nil
}

func (b *InMemoryBackend) applyBundleUpdate(r *Resource, patch map[string]any) map[string]any {
	parents, _ := patch[keyParentVersionIDs].([]any)
	branch, _ := patch[keyBranchName].(string)

	if branch == "" {
		for _, v := range r.Versions {
			if v.ID == parents[0] {
				branch = v.Branch
			}
		}
	}

	meta := map[string]any{keyBranchName: branch, keyParentVersionIDs: parents}

	for _, f := range []string{keyCommitMessage, keyCreatedBy} {
		if v, ok := patch[f]; ok {
			meta[f] = v
		}
	}

	for _, f := range []string{keyParentVersionIDs, keyBranchName, keyCommitMessage, keyCreatedBy} {
		delete(patch, f)
	}

	mergeDoc(r, patch)

	vid := awsid.NewN("", versionIDLen)
	r.Doc["versionId"], r.Doc["lineageMetadata"] = vid, meta

	pids := make([]string, 0, len(parents))
	for _, p := range parents {
		s, _ := p.(string)
		pids = append(pids, s)
	}

	r.Versions = append(
		r.Versions,
		Version{ID: vid, Branch: branch, Parents: pids, Created: b.now(), Doc: cloneDoc(r.Doc)},
	)

	return nil
}

func (b *InMemoryBackend) bundleBranchView(r *Resource, branch string) (*Resource, error) {
	if branch == "" {
		return r, nil
	}

	for _, v := range slices.Backward(r.Versions) {
		if v.Branch == branch {
			c := cloneResource(r)
			c.Doc = cloneDoc(v.Doc)

			return c, nil
		}
	}

	return nil, notFound("configuration bundle %q has no branch %q", r.ID, branch)
}

func (b *InMemoryBackend) validateOnlineEvalInput(sc scope, in map[string]any) error {
	for _, e := range listOf(in, "evaluators") {
		id := strOf(mapOf(e), "evaluatorId")
		if id == "" {
			return invalid("every evaluator reference must carry an evaluatorId")
		}

		if strings.HasPrefix(id, "Builtin.") {
			continue
		}

		if _, ok := b.find(sc, b.kinds[kindEvaluator], "", id); !ok {
			return notFound("evaluator %q not found", id)
		}
	}

	if v, ok := in["executionStatus"].(string); ok && v != "ENABLED" && v != "DISABLED" {
		return invalid("executionStatus must be ENABLED or DISABLED")
	}

	return nil
}

func decorateOnlineEval(r *Resource, in args) {
	status := "DISABLED"
	if enabled, _ := in["enableOnCreate"].(bool); enabled {
		status = "ENABLED"
	}

	delete(r.Doc, "enableOnCreate")
	r.Doc["executionStatus"] = status
}

func (b *InMemoryBackend) evaluatorDeletable(sc scope, r *Resource) error {
	for _, c := range b.scoped(sc, b.kinds[kindOnlineEval], "") {
		for _, e := range listOf(c.Doc, "evaluators") {
			if strOf(mapOf(e), "evaluatorId") == r.ID {
				return conflict("evaluator %q is used by online evaluation config %q", r.ID, c.ID)
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) validateConsentCreate(sc scope, in map[string]any) error {
	if s, ok := in["sources"].([]any); ok {
		if len(s) == 0 {
			return invalid("sources must not be empty")
		}

		for _, e := range s {
			src, _ := e.(map[string]any)
			if _, found := b.gatewayByRef(sc, strOf(src, "identifier")); !found {
				return notFound("consent portal source %q does not exist", strOf(src, "identifier"))
			}
		}
	}

	if idp := objOf(in, "idpConfig"); idp != nil {
		arn := strOf(idp, keyCredProviderARN)
		if r, ok := b.byARN(sc, arn); !ok || r.Kind != kindOAuthProvider {
			return notFound("OAuth2 credential provider %q does not exist", arn)
		}
	}

	return nil
}

func (b *InMemoryBackend) gatewayByRef(sc scope, ref string) (*Resource, bool) {
	return b.find(sc, b.kinds[kindGateway], "", idOf(ref))
}

func (b *InMemoryBackend) validateConnectorInput(sc scope, in args) error {
	cfgs := in.list("credentialProviderConfigurations")
	if len(cfgs) == 0 {
		return invalid("credentialProviderConfigurations must not be empty")
	}

	for _, c := range cfgs {
		cm, _ := c.(map[string]any)

		kind, ok := exactlyOne(cm, []string{"coinbaseCDP", "stripePrivy"})
		if !ok {
			return invalid("each credential provider configuration must set coinbaseCDP or stripePrivy")
		}

		arn := strOf(objOf(cm, kind), keyCredProviderARN)
		if r, found := b.byARN(sc, arn); !found || r.Kind != kindPaymentProvider {
			return notFound("payment credential provider %q does not exist", arn)
		}
	}

	return nil
}

func (b *InMemoryBackend) decorateConsent(r *Resource) {
	r.Doc["portalUrl"] = fmt.Sprintf("https://%s.consent.bedrock-agentcore.%s.amazonaws.com", r.ID, r.Region)
}
