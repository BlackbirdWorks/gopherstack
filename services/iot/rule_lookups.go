package iot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// DynamoItemReader reads one item by key; the item uses the DynamoDB wire format and is nil when absent.
type DynamoItemReader interface {
	GetItem(ctx context.Context, region, table string, key map[string]any) (map[string]any, error)
}

// SecretValue is the current version of a Secrets Manager secret.
type SecretValue struct {
	ARN    string
	String string
	Binary []byte
}

// SecretReader reads the current version of a secret by name or ARN.
type SecretReader interface {
	GetSecretValue(ctx context.Context, region, secretID string) (SecretValue, error)
}

// ShadowReader returns a thing's shadow document; an empty shadowName is the classic shadow.
type ShadowReader interface {
	GetThingShadow(ctx context.Context, region, thingName, shadowName string) ([]byte, error)
}

// LambdaRequester invokes a function synchronously and returns its response payload.
type LambdaRequester interface {
	RequestLambda(ctx context.Context, region, functionARN string, payload []byte) ([]byte, error)
}

// RoleCredentialIssuer returns credentials for a role the IoT service principal assumes.
type RoleCredentialIssuer interface {
	IssueRoleCredentials(roleARN string) (aws.Credentials, error)
}

// roleCredentials signs as the role when IAM is enforced, and with the default test key otherwise.
func (h *ruleHook) roleCredentials(roleARN string) (aws.Credentials, error) {
	issuer := h.backend.actionTargets().Credentials
	if issuer == nil || h.backend.ruleAuthorizer() == nil {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	}

	creds, err := issuer.IssueRoleCredentials(roleARN)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("%w: %w", errRuleActionDenied, err)
	}

	return creds, nil
}

// backendFor returns the regional backend a rule's region names.
func (h *ruleHook) backendFor(region string) *InMemoryBackend {
	if region == "" || h.backend.region == region || h.others == nil {
		return h.backend
	}

	for _, ob := range h.others() {
		if ob.region == region {
			return ob
		}
	}

	return h.backend
}

func (m *ruleMessage) thingARN(thing string) string {
	return arn.Build("iot", m.region, m.account, "thing/"+thing)
}

func (h *ruleHook) readDynamoItem(
	m *ruleMessage, role, table string, key map[string]any,
) (map[string]any, error) {
	res := arn.Build("dynamodb", m.region, m.account, "table/"+table)
	if err := h.allow(role, res, "dynamodb:GetItem"); err != nil {
		return nil, err
	}

	r := h.backend.actionTargets().DynamoReader
	if r == nil {
		return nil, ErrActionTargetUnavailable
	}

	return r.GetItem(h.ctx, m.region, table, key)
}

// readSecret authorizes against the secret's own ARN once it is known; a denied read returns nothing.
func (h *ruleHook) readSecret(m *ruleMessage, role, id string) (SecretValue, error) {
	r := h.backend.actionTargets().Secrets
	if r == nil {
		return SecretValue{}, ErrActionTargetUnavailable
	}

	v, err := r.GetSecretValue(h.ctx, m.region, id)
	if err != nil {
		return SecretValue{}, err
	}

	if aerr := h.allow(role, v.ARN, "secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"); aerr != nil {
		return SecretValue{}, aerr
	}

	return v, nil
}

func (h *ruleHook) readShadow(m *ruleMessage, role, thing, shadow string) ([]byte, error) {
	if err := h.allow(role, m.thingARN(thing), "iot:GetThingShadow"); err != nil {
		return nil, err
	}

	r := h.backend.actionTargets().Shadows
	if r == nil {
		return nil, ErrActionTargetUnavailable
	}

	return r.GetThingShadow(h.ctx, m.region, thing, shadow)
}

func (h *ruleHook) callLambda(m *ruleMessage, functionARN string, payload []byte) ([]byte, error) {
	auth := h.backend.ruleAuthorizer()
	if err := roleauth.AuthorizeResource(auth, roleauth.PrincipalIoT, "lambda:InvokeFunction",
		functionARN, m.ruleARN); err != nil {
		return nil, fmt.Errorf("%w: %w", errRuleActionDenied, err)
	}

	r := h.backend.actionTargets().Lambda
	if r == nil {
		return nil, ErrActionTargetUnavailable
	}

	region, _ := ruleRegionAccount(functionARN)
	if region == "" {
		region = m.region
	}

	return r.RequestLambda(h.ctx, region, functionARN, payload)
}

// registryData returns the DescribeThing or ListThingGroupsForThing response as JSON.
func (h *ruleHook) registryData(m *ruleMessage, role, api, thing string) ([]byte, error) {
	perm := "iot:" + api
	if err := h.allow(role, m.thingARN(thing), perm); err != nil {
		return nil, err
	}

	bk := h.backendFor(m.region)

	switch api {
	case "DescribeThing":
		return describeThingJSON(bk, thing)
	case "ListThingGroupsForThing":
		return thingGroupsJSON(bk, thing)
	}

	return nil, fmt.Errorf("%w: unsupported registry API", errTemplate)
}

const (
	keyThingGroups = "thingGroups"
	groupCapHint   = 4
)

func describeThingJSON(bk *InMemoryBackend, thing string) ([]byte, error) {
	t, err := bk.DescribeThing(thing)
	if err != nil {
		return nil, err
	}

	return json.Marshal(thingDescription(t))
}

func thingGroupsJSON(bk *InMemoryBackend, thing string) ([]byte, error) {
	groups := make([]map[string]any, 0, groupCapHint)

	for _, name := range bk.ListThingGroupsForThing(thing) {
		entry := map[string]any{keyGroupName: name}
		if tg, err := bk.DescribeThingGroup(name); err == nil {
			entry[keyGroupArn] = tg.ThingGroupARN
		}

		groups = append(groups, entry)
	}

	return json.Marshal(map[string]any{keyThingGroups: groups})
}
