package bedrockagentcorecontrol

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	defaultEndpoint = "DEFAULT"
	modePublic      = "PUBLIC"
	modeVPC         = "VPC"
	modeSandbox     = "SANDBOX"
)

func cloneDoc(doc map[string]any) map[string]any { return maps.Clone(doc) }

func (b *InMemoryBackend) validateRuntimeInput(sc scope, in args) error {
	art := in.obj("agentRuntimeArtifact")
	if art == nil {
		return nil
	}

	container, code := objOf(art, "containerConfiguration"), objOf(art, "codeConfiguration")
	if (container == nil) == (code == nil) {
		return invalid("agentRuntimeArtifact must set exactly one of containerConfiguration or codeConfiguration")
	}

	if container != nil {
		if err := b.validateContainerURI(strOf(container, "containerUri")); err != nil {
			return err
		}
	}

	if code != nil {
		if err := validateCodeConfiguration(code); err != nil {
			return err
		}
	}

	if err := validateNetworkMode(
		in.obj("networkConfiguration"),
		"networkModeConfig",
		modePublic,
		modeVPC,
	); err != nil {
		return err
	}

	if err := validateJWTAuthorizer(in.obj("authorizerConfiguration")); err != nil {
		return err
	}

	if cp := in.obj("capacityProviderConfiguration"); cp != nil {
		if _, ok := b.byARN(sc, strOf(cp, "capacityProviderArn")); !ok {
			return notFound("capacity provider %q not found", strOf(cp, "capacityProviderArn"))
		}
	}

	return nil
}

func (b *InMemoryBackend) validateContainerURI(uri string) error {
	m := ecrURIPattern.FindStringSubmatch(uri)
	if m == nil {
		return invalid("containerUri %q is not a valid Amazon ECR image URI", uri)
	}

	if b.refs != nil && !b.refs.ECRRepositoryExists(m[1], m[2], m[3]) {
		return invalid("ECR repository %q does not exist in %s", m[3], m[2])
	}

	return nil
}

func validateCodeConfiguration(code map[string]any) error {
	if len(listOf(code, "entryPoint")) == 0 {
		return invalid("codeConfiguration.entryPoint must not be empty")
	}

	if strOf(code, "runtime") == "" {
		return invalid("codeConfiguration.runtime is required")
	}

	s3 := objOf(objOf(code, "code"), "s3")
	if strOf(s3, "bucket") == "" || strOf(s3, "prefix") == "" {
		return invalid("codeConfiguration.code.s3 requires bucket and prefix")
	}

	return nil
}

func validateNetworkMode(nc map[string]any, vpcKey string, allowed ...string) error {
	if nc == nil {
		return nil
	}

	mode := strOf(nc, "networkMode")
	if !slices.Contains(allowed, mode) {
		return invalid("networkMode %q must be one of %s", mode, strings.Join(allowed, ", "))
	}

	if mode == modeVPC {
		vpc := objOf(nc, vpcKey)
		if len(listOf(vpc, "subnets")) == 0 || len(listOf(vpc, "securityGroups")) == 0 {
			return invalid("%s requires subnets and securityGroups for VPC mode", vpcKey)
		}
	}

	return nil
}

func validateJWTAuthorizer(ac map[string]any) error {
	if ac == nil {
		return nil
	}

	jwt := objOf(ac, "customJWTAuthorizer")
	if jwt == nil {
		return invalid("authorizerConfiguration must set customJWTAuthorizer")
	}

	u := strOf(jwt, "discoveryUrl")
	if !strings.HasPrefix(u, "https://") || !strings.HasSuffix(u, "/.well-known/openid-configuration") {
		return invalid("discoveryUrl must be an https URL ending in /.well-known/openid-configuration")
	}

	return nil
}

func (b *InMemoryBackend) validateRuntimeUpdate(sc scope, patch map[string]any) error {
	return b.validateRuntimeInput(sc, args(patch))
}

func decorateRuntime(b *InMemoryBackend, r *Resource) {
	r.Doc["agentRuntimeVersion"] = "1"
	r.Versions = []Version{{ID: "1", Created: r.Created, Doc: cloneDoc(r.Doc)}}
	_ = b
}

func (b *InMemoryBackend) createManagedIdentity(sc scope, r *Resource, base string) {
	idk := b.kinds[kindWorkloadIdentity]
	name := awsid.NewN(base+"-", idSuffixLen)

	wi := &Resource{
		Kind: kindWorkloadIdentity, Account: sc.account, Region: sc.region, ID: name,
		Doc:     map[string]any{keyName: name, "allowedResourceOauth2ReturnUrls": []any{}},
		Created: r.Created, Updated: r.Created, SettleAt: r.Created,
	}
	wi.ARN = b.arnOf(idk, sc, "", name)
	b.resources.Put(wi)

	r.Doc["workloadIdentityDetails"] = map[string]any{"workloadIdentityArn": wi.ARN}
	r.Aux2()["managedIdentity"] = name
}

func (b *InMemoryBackend) createDefaultEndpoint(sc scope, r *Resource) {
	ek := b.kinds[kindRuntimeEndpoint]
	ep := &Resource{
		Kind: kindRuntimeEndpoint, Account: sc.account, Region: sc.region, Parent: r.ID, ParentARN: r.ARN,
		ID: defaultEndpoint, Created: r.Created, Updated: r.Created, SettleAt: r.Created, Status: stateReady,
		Doc: map[string]any{
			keyName: defaultEndpoint, "endpointId": awsid.NewN("", idSuffixLen),
			keyDescription: "Default endpoint for the agent runtime", "liveVersion": "1", "targetVersion": "1",
		},
	}
	ep.ARN = b.arnOf(ek, sc, r.ID, defaultEndpoint)
	b.resources.Put(ep)
}

func (b *InMemoryBackend) latestVersion(r *Resource) string {
	v, _ := r.Doc["agentRuntimeVersion"].(string)

	return v
}

func (b *InMemoryBackend) hasVersion(r *Resource, id string) bool {
	return slices.ContainsFunc(r.Versions, func(v Version) bool { return v.ID == id })
}

func (b *InMemoryBackend) applyRuntimeUpdate(r *Resource, patch map[string]any) map[string]any {
	mergeDoc(r, patch)

	n, _ := strconv.Atoi(b.latestVersion(r))
	next := strconv.Itoa(n + 1)
	r.Doc["agentRuntimeVersion"] = next
	r.Versions = append(r.Versions, Version{ID: next, Created: b.now(), Doc: cloneDoc(r.Doc)})

	if ep, ok := b.resources.Get(makeKey(r.Account, r.Region, kindRuntimeEndpoint, r.ID, defaultEndpoint)); ok {
		ep.Doc["liveVersion"], ep.Doc["targetVersion"] = next, next
		ep.Updated = b.now()
	}

	return nil
}

func (b *InMemoryBackend) decorateEndpoint(sc scope, r *Resource, in args) error {
	rt, err := b.mustFind(sc, b.kinds[kindRuntime], "", r.Parent)
	if err != nil {
		return err
	}

	if strings.EqualFold(r.ID, defaultEndpoint) {
		return conflict("endpoint name %q is reserved for the runtime's default endpoint", defaultEndpoint)
	}

	version := in.str("agentRuntimeVersion")
	if version == "" {
		version = b.latestVersion(rt)
	}

	if !b.hasVersion(rt, version) {
		return notFound("agent runtime %q has no version %q", rt.ID, version)
	}

	delete(r.Doc, "agentRuntimeVersion")
	r.Doc["endpointId"] = awsid.NewN("", idSuffixLen)
	r.Doc["liveVersion"], r.Doc["targetVersion"] = version, version

	return nil
}

func (b *InMemoryBackend) validateEndpointUpdate(r *Resource, patch map[string]any) error {
	v, ok := patch["agentRuntimeVersion"].(string)
	if !ok {
		return nil
	}

	rt, found := b.resources.Get(makeKey(r.Account, r.Region, kindRuntime, "", r.Parent))
	if !found || !b.hasVersion(rt, v) {
		return notFound("agent runtime %q has no version %q", r.Parent, v)
	}

	return nil
}

func (b *InMemoryBackend) applyEndpointUpdate(r *Resource, patch map[string]any) map[string]any {
	overrides := map[string]any{}

	if v, ok := patch["agentRuntimeVersion"].(string); ok {
		overrides["liveVersion"] = r.Doc["liveVersion"]
		r.Doc["targetVersion"], r.Doc["liveVersion"] = v, v
		delete(patch, "agentRuntimeVersion")
	}

	mergeDoc(r, patch)

	return overrides
}

func (b *InMemoryBackend) runtimeDeletable(sc scope, r *Resource, in args) error {
	if in.str("version") != "" {
		return nil
	}

	for _, ep := range b.scoped(sc, b.kinds[kindRuntimeEndpoint], r.ID) {
		if ep.ID != defaultEndpoint {
			return conflict("agent runtime %q still has endpoint %q; delete it first", r.ID, ep.ID)
		}
	}

	return nil
}

func (b *InMemoryBackend) runtimeVersionView(r *Resource, version string) (*Resource, error) {
	if version == "" || version == b.latestVersion(r) {
		return r, nil
	}

	for _, v := range r.Versions {
		if v.ID == version {
			c := cloneResource(r)
			c.Doc, c.Updated = cloneDoc(v.Doc), v.Created

			return c, nil
		}
	}

	return nil, notFound("agent runtime %q has no version %q", r.ID, version)
}

func (b *InMemoryBackend) deleteRuntimeVersion(sc scope, r *Resource, version string) (bool, error) {
	if version == "" || version == b.latestVersion(r) {
		return false, nil
	}

	if !b.hasVersion(r, version) {
		return false, notFound("agent runtime %q has no version %q", r.ID, version)
	}

	for _, ep := range b.scoped(sc, b.kinds[kindRuntimeEndpoint], r.ID) {
		if ep.Doc["liveVersion"] == version || ep.Doc["targetVersion"] == version {
			return false, conflict("version %q is in use by endpoint %q", version, ep.ID)
		}
	}

	r.Versions = slices.DeleteFunc(r.Versions, func(v Version) bool { return v.ID == version })

	return true, nil
}

func (b *InMemoryBackend) capacityProviderDeletable(sc scope, r *Resource) error {
	for _, rt := range b.scoped(sc, b.kinds[kindRuntime], "") {
		if strOf(objOf(rt.Doc, "capacityProviderConfiguration"), "capacityProviderArn") == r.ARN {
			return conflict("capacity provider %q is used by agent runtime %q", r.ID, rt.ID)
		}
	}

	return nil
}

func validateCapacityInput(in args) error {
	if in.obj("computeConfiguration") == nil || in.obj("permissionsConfiguration") == nil {
		return invalid("computeConfiguration and permissionsConfiguration are required")
	}

	return nil
}

func validateSandboxInput(k *kindSpec, in args) error {
	if k.Name == kindBrowser {
		return validateNetworkMode(in.obj("networkConfiguration"), "vpcConfig", modePublic, modeVPC)
	}

	return validateNetworkMode(in.obj("networkConfiguration"), "vpcConfig", modePublic, modeSandbox, modeVPC)
}
