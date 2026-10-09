package appconfig

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

var errExtensionFunction = errors.New("extension function error")

const (
	keyContentType                                 = "ContentType"
	keyContentVersion                              = "ContentVersion"
	actionPointPreCreateHostedConfigurationVersion = "PRE_CREATE_HOSTED_CONFIGURATION_VERSION"
	actionPointPreStartDeployment                  = "PRE_START_DEPLOYMENT"
	lambdaARNPrefix                                = "arn:aws:lambda:"
	invocationIDBytes                              = 4
)

// ExtensionLambdaInvoker runs a Lambda extension action synchronously; a function error is returned as an error.
type ExtensionLambdaInvoker interface {
	Invoke(ctx context.Context, functionARN string, payload []byte) ([]byte, error)
}

// SetExtensionLambdaInvoker overrides the Lambda invoker used for extension actions; by default the Lambda
// service reachable through the app context is used.
func (b *InMemoryBackend) SetExtensionLambdaInvoker(l ExtensionLambdaInvoker) {
	b.mu.Lock("SetExtensionLambdaInvoker")
	defer b.mu.Unlock()

	b.extLambda = l
}

type lambdaSibling interface {
	GetLambdaHandler() service.Registerable
}

type siblingLambdaInvoker struct {
	backend *lambdabackend.InMemoryBackend
}

func (i siblingLambdaInvoker) Invoke(ctx context.Context, functionARN string, payload []byte) ([]byte, error) {
	out, _, functionError, _, err := i.backend.InvokeFunctionWithQualifier(
		ctx, functionARN, "", "", "", lambdabackend.InvocationTypeRequestResponse, payload,
	)
	if err != nil {
		return nil, err
	}

	if functionError != "" {
		return out, fmt.Errorf("%w %s: %s", errExtensionFunction, functionError, out)
	}

	return out, nil
}

func (b *InMemoryBackend) extensionLambdaInvoker() ExtensionLambdaInvoker {
	b.mu.RLock("extensionLambdaInvoker")
	explicit, cfg := b.extLambda, b.appConfig
	b.mu.RUnlock()

	if explicit != nil {
		return explicit
	}

	sib, ok := cfg.(lambdaSibling)
	if !ok {
		return nil
	}

	h, ok := sib.GetLambdaHandler().(*lambdabackend.Handler)
	if !ok || h == nil {
		return nil
	}

	bk, ok := h.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	return siblingLambdaInvoker{backend: bk}
}

// preAction is one Lambda action of an extension associated with the resources a PRE_* action point acts on.
type preAction struct {
	params map[string]string
	uri    string
}

// preActionsLocked lists the Lambda actions registered for actionPoint by extensions associated with the
// application, environment or configuration profile, ordered by association ID. Must be called under lock.
func (b *InMemoryBackend) preActionsLocked(actionPoint, applicationID, environmentID, profileID string) []preAction {
	targets := map[string]bool{
		b.appconfigARN("application/" + applicationID):                                        true,
		b.appconfigARN("application/" + applicationID + "/configurationprofile/" + profileID): true,
	}
	if environmentID != "" {
		targets[b.appconfigARN("application/"+applicationID+"/environment/"+environmentID)] = true
	}

	assocs := make([]*ExtensionAssociation, 0)

	for _, a := range b.extensionAssociations.All() {
		if targets[a.ResourceArn] {
			assocs = append(assocs, a)
		}
	}

	sort.Slice(assocs, func(i, j int) bool { return assocs[i].ID < assocs[j].ID })

	var out []preAction

	for _, a := range assocs {
		ext, ok := b.extensions.Get(extensionVersionKey(extensionIDFromArn(a.ExtensionArn), a.ExtensionVersionNumber))
		if !ok {
			continue
		}

		for _, act := range ext.Actions[actionPoint] {
			if strings.HasPrefix(act.URI, lambdaARNPrefix) {
				out = append(out, preAction{uri: act.URI, params: maps.Clone(a.Parameters)})
			}
		}
	}

	return out
}

type actionResponse struct {
	Content *string `json:"Content"`
	Error   string  `json:"Error"`
	Message string  `json:"Message"`
}

// runPreActions invokes each action in order, handing content to the next action. A returned Content (base64)
// replaces the configuration content; an Error response or a function error rejects the request.
func (b *InMemoryBackend) runPreActions(
	actions []preAction, event map[string]any, content []byte, dynamic map[string]string,
) ([]byte, error) {
	if len(actions) == 0 {
		return content, nil
	}

	invoker := b.extensionLambdaInvoker()
	if invoker == nil {
		return content, nil
	}

	for _, act := range actions {
		params := maps.Clone(act.params)
		if params == nil {
			params = map[string]string{}
		}

		maps.Copy(params, dynamic)

		ev := maps.Clone(event)
		ev["InvocationId"] = newInvocationID()
		ev["Parameters"] = params
		ev["Content"] = base64.StdEncoding.EncodeToString(content)

		payload, err := json.Marshal(ev)
		if err != nil {
			return nil, fmt.Errorf("%w: encoding extension event: %w", ErrBadRequest, err)
		}

		raw, err := invoker.Invoke(context.Background(), act.uri, payload)
		if err != nil {
			return nil, fmt.Errorf("%w: extension action %s failed: %w", ErrBadRequest, act.uri, err)
		}

		content, err = applyActionResponse(act.uri, raw, content)
		if err != nil {
			return nil, err
		}
	}

	return content, nil
}

func applyActionResponse(uri string, raw, content []byte) ([]byte, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return content, nil
	}

	if !json.Valid(raw) {
		return content, nil
	}

	var resp actionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("%w: extension action %s returned an unreadable response: %w", ErrBadRequest, uri, err)
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("%w: extension action %s returned %s: %s", ErrBadRequest, uri, resp.Error, resp.Message)
	}

	if resp.Content == nil {
		return content, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(*resp.Content)
	if err != nil {
		return nil, fmt.Errorf("%w: extension action %s returned invalid base64 Content", ErrBadRequest, uri)
	}

	return decoded, nil
}

func newInvocationID() string {
	buf := make([]byte, invocationIDBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)[:7]
}

func (b *InMemoryBackend) resourceRef(id, name string) map[string]string {
	return map[string]string{"Id": id, "Name": name}
}

func (b *InMemoryBackend) applicationName(id string) string {
	if a, ok := b.applications.Get(id); ok {
		return a.Name
	}

	return ""
}

// preCreateHostedVersion runs PRE_CREATE_HOSTED_CONFIGURATION_VERSION actions and returns the content to store.
func (b *InMemoryBackend) preCreateHostedVersion(
	applicationID, profileID, contentType, description string, content []byte,
) ([]byte, error) {
	b.mu.RLock("preCreateHostedVersion")
	actions := b.preActionsLocked(actionPointPreCreateHostedConfigurationVersion, applicationID, "", profileID)

	if len(actions) == 0 || !b.applications.Has(applicationID) {
		b.mu.RUnlock()

		return content, nil
	}

	profile, profileFound := b.configProfiles.Get(profileID)
	if !profileFound || profile.ApplicationID != applicationID {
		b.mu.RUnlock()

		return content, nil
	}

	next := b.versionCounters[applicationID][profileID] + 1
	event := map[string]any{
		"Type":                 "PreCreateHostedConfigurationVersion",
		keyContentType:         contentType,
		keyContentVersion:      strconv.Itoa(int(next)),
		"Description":          description,
		"Application":          b.resourceRef(applicationID, b.applicationName(applicationID)),
		"ConfigurationProfile": b.resourceRef(profileID, profile.Name),
	}

	if prev, ok := b.hostedConfigVersions.Get(hcvKey(applicationID, profileID, next-1)); ok {
		event["PreviousContent"] = map[string]string{
			keyContentType:    prev.ContentType,
			keyContentVersion: strconv.Itoa(int(prev.VersionNumber)),
			"Content":         base64.StdEncoding.EncodeToString(prev.Content),
		}
	}

	b.mu.RUnlock()

	return b.runPreActions(actions, event, content, nil)
}

// preStartDeployment runs PRE_START_DEPLOYMENT actions and returns the (possibly transformed) content, or nil
// when no action changed it.
func (b *InMemoryBackend) preStartDeployment(
	applicationID, environmentID, profileID, strategyID, configVersion, description string,
	dynamic map[string]string,
) ([]byte, error) {
	b.mu.Lock("preStartDeployment")
	profile, _, _, validateErr := b.resolveStartDeploymentInputsLocked(
		applicationID, environmentID, profileID, strategyID, configVersion, nil,
	)
	if validateErr != nil {
		b.mu.Unlock()

		return nil, validateErr
	}

	actions := b.preActionsLocked(actionPointPreStartDeployment, applicationID, environmentID, profileID)
	if len(actions) == 0 {
		b.mu.Unlock()

		return nil, nil
	}

	var (
		content     []byte
		contentType string
	)

	if profile.LocationURI == contentTypeHostedLocation {
		if hcv, found := b.resolveHostedConfigVersion(applicationID, profileID, configVersion); found {
			content, contentType = hcv.Content, hcv.ContentType
		}
	}

	env, _ := b.environments.Get(environmentID)
	envName := ""

	if env != nil {
		envName = env.Name
	}

	event := map[string]any{
		"Type":                 "PreStartDeployment",
		keyContentType:         contentType,
		keyContentVersion:      configVersion,
		"Description":          description,
		"DeploymentNumber":     b.deploymentCounters[applicationID][environmentID] + 1,
		"Application":          b.resourceRef(applicationID, b.applicationName(applicationID)),
		"Environment":          b.resourceRef(environmentID, envName),
		"ConfigurationProfile": b.resourceRef(profileID, profile.Name),
	}
	b.mu.Unlock()

	out, err := b.runPreActions(actions, event, content, dynamic)
	if err != nil {
		return nil, err
	}

	if string(out) == string(content) {
		return nil, nil
	}

	return out, nil
}
