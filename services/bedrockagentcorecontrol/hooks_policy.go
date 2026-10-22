package bedrockagentcorecontrol

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	callbackIDLen    = 32
	secretSuffixLen  = 6
	statusPending    = "PENDING_APPROVAL"
	statusApproved   = "APPROVED"
	statusRejected   = "REJECTED"
	statusDeprecated = "DEPRECATED"
	statusDraft      = "DRAFT"
	generateFailed   = "GENERATE_FAILED"
)

func validatePolicyInput(in map[string]any) error {
	def, _ := in["definition"].(map[string]any)
	if def == nil {
		return nil
	}

	kind, ok := exactlyOne(def, []string{"cedar", keyPolicy, "policyGeneration"})
	if !ok {
		return invalid("definition must set exactly one of cedar, policy or policyGeneration")
	}

	switch kind {
	case "cedar":
		if strings.TrimSpace(strOf(objOf(def, "cedar"), "statement")) == "" {
			return invalid("definition.cedar.statement must not be empty")
		}
	case "policyGeneration":
		return invalid(
			"policy generation %q has no generated assets",
			strOf(objOf(def, "policyGeneration"), "policyGenerationId"),
		)
	}

	return nil
}

func (b *InMemoryBackend) policyEngineDeletable(sc scope, r *Resource) error {
	if err := b.requireNoChildren(sc, r, kindPolicy, "policies"); err != nil {
		return err
	}

	for _, g := range b.scoped(sc, b.kinds[kindGateway], "") {
		if strOf(objOf(g.Doc, "policyEngineConfiguration"), keyARN) == r.ARN {
			return conflict("policy engine %q is attached to gateway %q", r.ID, g.ID)
		}
	}

	return nil
}

func decoratePolicyGeneration(r *Resource) {
	r.Status = generateFailed
	r.Doc["statusReasons"] = []any{"Policy generation needs a generative model, which this emulator does not provide"}
	r.Doc["findings"] = ""
}

func validateRecordInput(in args) error {
	if in.obj("descriptors") == nil {
		return nil
	}

	return nil
}

func validateRecordUpdate(r *Resource, _ map[string]any) error {
	if r.Status == statusPending {
		return conflict("registry record %q is pending approval and cannot be edited", r.ID)
	}

	return nil
}

func (b *InMemoryBackend) decorateRecord(sc scope, r *Resource) error {
	reg, err := b.mustFind(sc, b.kinds[kindRegistry], "", r.Parent)
	if err != nil {
		return err
	}

	if _, ok := r.Doc["recordVersion"]; !ok {
		r.Doc["recordVersion"] = "1"
	}

	r.Status = statusDraft
	_ = reg

	return nil
}

func bumpRecordVersion(r *Resource, patch map[string]any) {
	if _, ok := patch["recordVersion"]; !ok {
		r.Doc["recordVersion"] = bumped(strOf(r.Doc, "recordVersion"))
	}

	delete(r.Doc, "triggerSynchronization")
}

func bumped(v string) string {
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
		return strconv.Itoa(n + 1)
	}

	return v + ".1"
}

// submitRecord moves a DRAFT or REJECTED record to PENDING_APPROVAL, or APPROVED when the registry auto-approves.
func (b *InMemoryBackend) submitRecord(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("submitRecord")
	defer b.mu.Unlock()

	k := b.kinds[kindRegistryRecord]

	rec, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	if rec.Status != statusDraft && rec.Status != statusRejected {
		return nil, conflict("registry record %q is %s and cannot be submitted for approval", rec.ID, rec.Status)
	}

	reg, err := b.mustFind(sc, b.kinds[kindRegistry], "", rec.Parent)
	if err != nil {
		return nil, err
	}

	rec.Status = statusPending
	if auto, _ := objOf(reg.Doc, "approvalConfiguration")["autoApproval"].(bool); auto {
		rec.Status = statusApproved
	}

	rec.Updated = b.now()

	return b.render(op, k, rec, rec.Status, nil), nil
}

func recordTransitions() map[string][]string {
	return map[string][]string{
		statusPending:  {statusApproved, statusRejected},
		statusApproved: {statusDeprecated},
	}
}

// setRecordStatus applies a curator decision to a record.
func (b *InMemoryBackend) setRecordStatus(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("setRecordStatus")
	defer b.mu.Unlock()

	k := b.kinds[kindRegistryRecord]

	rec, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	want := in.str(keyStatus)
	if !slices.Contains(recordTransitions()[rec.Status], want) {
		return nil, conflict("registry record %q cannot move from %s to %s", rec.ID, rec.Status, want)
	}

	rec.Status = want
	rec.Doc["statusReason"] = in.str("statusReason")
	rec.Updated = b.now()

	return b.render(op, k, rec, rec.Status, nil), nil
}

func decorateConnector(r *Resource) {
	if _, ok := r.Doc["provisionMode"]; !ok {
		r.Doc["provisionMode"] = "MANUAL"
	}
}

// rotateConnectorCredentials re-keys a payment connector.
func (b *InMemoryBackend) rotateConnectorCredentials(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("rotateConnectorCredentials")
	defer b.mu.Unlock()

	k := b.kinds[kindPaymentConnector]

	c, err := b.resolve(sc, k, in)
	if err != nil {
		return nil, err
	}

	if isTransitional(b.visible(c)) {
		return nil, conflict("payment connector %q is %s", c.ID, b.visible(c))
	}

	if strOf(c.Doc, "type") != "CoinbaseCDP" {
		return nil, invalid("credential rotation is only available for CoinbaseCDP connectors")
	}

	if len(listOf(objOf(in.obj("credentialsToRotate"), "coinbaseCDP"), "secrets")) == 0 {
		return nil, invalid("credentialsToRotate.coinbaseCDP.secrets must name at least one secret")
	}

	c.Doc["credentialsUpdatedAt"] = b.now().UTC().Format(time.RFC3339Nano)
	b.settle(c, stateUpdating)

	return b.render(op, k, c, stateUpdating, nil), nil
}

func (b *InMemoryBackend) validateProviderInput(k *kindSpec, in args) error {
	switch k.Name {
	case kindAPIKeyProvider:
		if in.str(keyAPIKey) == "" && in.obj("apiKeySecretConfig") == nil {
			return invalid("either apiKey or apiKeySecretConfig is required")
		}
	case kindOAuthProvider:
		return validateOAuthInput(in)
	case kindPaymentProvider:
		cfg := in.obj("providerConfigurationInput")

		kind, ok := exactlyOne(cfg, []string{"coinbaseCdpConfiguration", "stripePrivyConfiguration"})
		if !ok || mapOf(cfg[kind]) == nil {
			return invalid("providerConfigurationInput must set exactly one provider configuration object")
		}
	}

	return b.checkSecretRefs(in)
}

func validateOAuthInput(in args) error {
	cfg := in.obj("oauth2ProviderConfigInput")
	if len(cfg) != 1 {
		return invalid("oauth2ProviderConfigInput must set exactly one provider configuration")
	}

	for _, v := range cfg {
		if mapOf(v) == nil {
			return invalid("the OAuth2 provider configuration must be an object")
		}
	}

	return nil
}

func (b *InMemoryBackend) checkSecretRefs(in map[string]any) error {
	if b.refs == nil {
		return nil
	}

	var bad string

	walkSecretRefs(in, func(id string) {
		if bad == "" && secretPattern.MatchString(id) && !b.refs.SecretExists(id) {
			bad = id
		}
	})

	if bad != "" {
		return invalid("secret %q does not exist", bad)
	}

	return nil
}

func walkSecretRefs(v any, f func(id string)) {
	switch t := v.(type) {
	case map[string]any:
		for k, c := range t {
			if strings.HasSuffix(k, "SecretConfig") || strings.HasSuffix(k, "KeyConfig") {
				if id := strOf(mapOf(c), "secretId"); id != "" {
					f(id)
				}
			}

			walkSecretRefs(c, f)
		}
	case []any:
		for _, c := range t {
			walkSecretRefs(c, f)
		}
	}
}

func (b *InMemoryBackend) validateProviderUpdate(r *Resource, patch map[string]any) error {
	if v, ok := patch["credentialProviderVendor"].(string); ok && r.Kind == kindOAuthProvider &&
		v != strOf(r.Doc, "credentialProviderVendor") {
		return invalid("credentialProviderVendor cannot be changed from %q", strOf(r.Doc, "credentialProviderVendor"))
	}

	return b.checkSecretRefs(patch)
}

// managedSecret stores value as a Secrets Manager secret when a store is wired and returns its ARN; without a
// store the ARN is synthesized and the value is dropped.
func (b *InMemoryBackend) managedSecret(sc scope, kind, name, member string, value any) string {
	st, _ := b.refs.(SecretStore)
	text, _ := value.(string)

	if st == nil || text == "" {
		return b.managedSecretARN(sc, kind, name, member)
	}

	payload, _ := json.Marshal(map[string]string{member: text})
	secretName := fmt.Sprintf("%s%s/%s/%s", managedSecretName, kind, name, member)

	if secretARN, err := st.PutSecret(sc.account, sc.region, secretName, string(payload)); err == nil {
		return secretARN
	}

	return b.managedSecretARN(sc, kind, name, member)
}

// releaseSecrets deletes the managed secrets a removed credential provider owned.
func (b *InMemoryBackend) releaseSecrets(r *Resource) {
	st, _ := b.refs.(SecretStore)
	if st == nil {
		return
	}

	walkSecretARNs(r.Doc, func(id string) {
		if strings.Contains(id, ":secret:"+managedSecretName) {
			st.DeleteSecret(id)
		}
	})
}

func walkSecretARNs(v any, f func(id string)) {
	switch t := v.(type) {
	case map[string]any:
		if id, ok := t["secretArn"].(string); ok {
			f(id)
		}

		for _, c := range t {
			walkSecretARNs(c, f)
		}
	case []any:
		for _, c := range t {
			walkSecretARNs(c, f)
		}
	}
}

func (b *InMemoryBackend) managedSecretARN(sc scope, kind, name, suffix string) string {
	return arn.Build(
		"secretsmanager",
		sc.region,
		sc.account,
		fmt.Sprintf(
			"secret:"+managedSecretPrefix+"%s/%s/%s-%s",
			kind,
			name,
			suffix,
			awsid.NewN("", secretSuffixLen),
		),
	)
}

// secretMembers converts caller-supplied secret inputs into the output members clients read back.
func (b *InMemoryBackend) secretMembers(sc scope, kind, name string, cfg map[string]any) map[string]any {
	out := map[string]any{}

	for k, v := range cfg {
		switch {
		case strings.HasSuffix(k, "SecretConfig") || strings.HasSuffix(k, "KeyConfig"):
			base := strings.TrimSuffix(k, "Config")
			ref, _ := v.(map[string]any)
			out[base+"Arn"], out[base+"JsonKey"], out[base+"Source"] = secretRef(
				ref["secretId"],
			), ref["jsonKey"], "EXTERNAL"
		case strings.HasSuffix(k, "Secret") || strings.HasSuffix(k, "PrivateKey") || k == keyAPIKey:
			out[k+"Arn"] = secretRef(b.managedSecret(sc, kind, name, k, v))
			out[k+"JsonKey"] = k
			out[k+"Source"] = "MANAGED"
		case strings.HasSuffix(k, "Source"):
		default:
			out[k] = v
		}
	}

	return out
}

func decorateOAuth(b *InMemoryBackend, sc scope, r *Resource) {
	cfg := objOf(r.Doc, "oauth2ProviderConfigInput")

	var inner map[string]any

	for _, v := range cfg {
		inner, _ = v.(map[string]any)
	}

	conv := b.secretMembers(sc, "oauth2", r.ID, inner)

	r.Doc["clientSecretArn"], r.Doc["clientSecretJsonKey"], r.Doc["clientSecretSource"] =
		conv["clientSecretArn"], conv["clientSecretJsonKey"], conv["clientSecretSource"]

	out := map[string]any{}

	for typ := range cfg {
		kept := map[string]any{}

		for k, v := range conv {
			if !strings.HasPrefix(k, "clientSecret") {
				kept[k] = v
			}
		}

		out[typ] = kept
	}

	delete(r.Doc, "oauth2ProviderConfigInput")
	r.Doc["oauth2ProviderConfigOutput"] = out
	r.Doc["callbackUrl"] = fmt.Sprintf("https://bedrock-agentcore.%s.amazonaws.com/identities/oauth2/callback/%s",
		r.Region, awsid.NewN("", callbackIDLen))
}

func decorateAPIKey(b *InMemoryBackend, sc scope, r *Resource) {
	var conv map[string]any

	if cfg := objOf(r.Doc, "apiKeySecretConfig"); cfg != nil {
		conv = b.secretMembers(sc, "apikey", r.ID, map[string]any{"apiKeySecretConfig": cfg})
	} else {
		conv = b.secretMembers(sc, "apikey", r.ID, map[string]any{keyAPIKey: r.Doc[keyAPIKey]})
		conv = map[string]any{
			"apiKeySecretArn": conv["apiKeyArn"], "apiKeySecretJsonKey": keyAPIKey, "apiKeySecretSource": "MANAGED",
		}
	}

	delete(r.Doc, keyAPIKey)
	delete(r.Doc, "apiKeySecretConfig")
	delete(r.Doc, "apiKeySecretSource")

	maps.Copy(r.Doc, conv)
}

func decoratePaymentProvider(b *InMemoryBackend, sc scope, r *Resource) {
	in := objOf(r.Doc, "providerConfigurationInput")
	delete(r.Doc, "providerConfigurationInput")

	out := map[string]any{}

	for typ, v := range in {
		out[typ] = b.secretMembers(sc, "payment", r.ID, mapOf(v))
	}

	r.Doc["providerConfigurationOutput"] = out
}

func secretRef(arn any) map[string]any { return map[string]any{"secretArn": arn} }

func reDecorateProvider(b *InMemoryBackend, k *kindSpec, r *Resource) {
	sc := scope{account: r.Account, region: r.Region}

	switch k.Name {
	case kindOAuthProvider:
		if _, ok := r.Doc["oauth2ProviderConfigInput"]; ok {
			decorateOAuth(b, sc, r)
		}
	case kindAPIKeyProvider:
		if _, ok := r.Doc[keyAPIKey]; ok || r.Doc["apiKeySecretConfig"] != nil {
			decorateAPIKey(b, sc, r)
		}
	case kindPaymentProvider:
		if _, ok := r.Doc["providerConfigurationInput"]; ok {
			decoratePaymentProvider(b, sc, r)
		}
	}
}

func decorateWorkloadIdentity(r *Resource) {
	if _, ok := r.Doc["allowedResourceOauth2ReturnUrls"]; !ok {
		r.Doc["allowedResourceOauth2ReturnUrls"] = []any{}
	}
}
