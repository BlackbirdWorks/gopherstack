package ssm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

const (
	secretsReferencePrefix = "/aws/reference/secretsmanager/"
	secretsArnParts        = 7

	secretNeedsDecryptionMsg = "WithDecryption flag must be True for retrieving a Secret Manager secret."
)

// ErrSecretNotFound is returned by a SecretResolver when no such secret (or
// version) exists; it surfaces as ParameterNotFound.
var ErrSecretNotFound = errors.New("secret not found")

// ErrSecretAccessDenied is returned by a SecretResolver when the caller may
// not read the secret; it surfaces as AccessDeniedException.
var ErrSecretAccessDenied = errors.New("AccessDeniedException")

var secretVersionIDPattern = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`,
)

// SecretRef identifies the secret version a parameter reference points at.
// VersionID and VersionStage are mutually exclusive; both empty means
// AWSCURRENT.
type SecretRef struct {
	ID           string
	VersionID    string
	VersionStage string
}

// SecretValue is the GetSecretValue result a SecretResolver returns.
type SecretValue struct {
	ARN           string
	Name          string
	VersionID     string
	SecretString  string
	SecretBinary  []byte
	VersionStages []string
	CreatedDate   float64
}

// SecretResolver reads a secret at call time. It is injected after startup
// (SetSecretResolver) and receives the request context, so an implementation
// can enforce secretsmanager:GetSecretValue for the caller.
type SecretResolver interface {
	ResolveSecret(ctx context.Context, ref SecretRef) (*SecretValue, error)
}

// SetSecretResolver wires the Secrets Manager reader used by
// /aws/reference/secretsmanager/* parameters. A nil resolver makes those
// references resolve to ParameterNotFound.
func (b *InMemoryBackend) SetSecretResolver(r SecretResolver) {
	b.mu.Lock("SetSecretResolver")
	defer b.mu.Unlock()
	b.secrets = r
}

func (b *InMemoryBackend) secretResolver() SecretResolver {
	b.mu.RLock("secretResolver")
	defer b.mu.RUnlock()

	return b.secrets
}

func isSecretReference(name string) bool {
	return strings.HasPrefix(name, secretsReferencePrefix)
}

// parseSecretReference splits "<id>[:version-id|:stage]" off a reference name;
// a full ARN's own colons are not mistaken for a selector.
func parseSecretReference(name string) SecretRef {
	id := strings.TrimPrefix(name, secretsReferencePrefix)

	var base, selector string

	if strings.HasPrefix(id, "arn:") {
		parts := strings.SplitN(id, ":", secretsArnParts+1)
		if len(parts) <= secretsArnParts {
			return SecretRef{ID: id}
		}

		base, selector = strings.Join(parts[:secretsArnParts], ":"), parts[secretsArnParts]
	} else {
		base, selector, _ = strings.Cut(id, ":")
	}

	switch {
	case selector == "":
		return SecretRef{ID: base}
	case secretVersionIDPattern.MatchString(selector):
		return SecretRef{ID: base, VersionID: selector}
	default:
		return SecretRef{ID: base, VersionStage: selector}
	}
}

type secretSourceResult struct {
	ARN           string   `json:"ARN"`
	Name          string   `json:"Name"`
	VersionID     string   `json:"VersionId"`
	SecretString  string   `json:"SecretString,omitempty"`
	SecretBinary  []byte   `json:"SecretBinary,omitempty"`
	VersionStages []string `json:"VersionStages,omitempty"`
	CreatedDate   float64  `json:"CreatedDate"`
}

// getSecretReference resolves a /aws/reference/secretsmanager/ name. Real AWS
// rejects the read unless WithDecryption is true.
func (b *InMemoryBackend) getSecretReference(
	ctx context.Context, name string, withDecryption bool,
) (ParameterOutput, error) {
	if !withDecryption {
		return ParameterOutput{}, fmt.Errorf("%w: %s", ErrValidationException, secretNeedsDecryptionMsg)
	}

	resolver := b.secretResolver()
	if resolver == nil {
		return ParameterOutput{}, ErrParameterNotFound
	}

	ref := parseSecretReference(name)

	secret, err := resolver.ResolveSecret(ctx, ref)
	if err != nil {
		if errors.Is(err, ErrSecretNotFound) {
			return ParameterOutput{}, ErrParameterNotFound
		}

		return ParameterOutput{}, err
	}

	value := secret.SecretString
	if value == "" && len(secret.SecretBinary) > 0 {
		value = string(secret.SecretBinary)
	}

	source, err := json.Marshal(secretSourceResult(*secret))
	if err != nil {
		return ParameterOutput{}, fmt.Errorf("marshal secret source result: %w", err)
	}

	return ParameterOutput{
		Name:             name,
		Type:             SecureStringType,
		Value:            value,
		ARN:              arn.Build("ssm", getRegion(ctx), awsmeta.Account(ctx), "parameter"+name),
		DataType:         publicParameterData,
		SourceResult:     string(source),
		LastModifiedDate: secret.CreatedDate,
		Version:          0,
	}, nil
}

type catalogResult struct {
	out ParameterOutput
	ok  bool
}

// getCatalogParameter serves names that are not stored parameters: the
// read-only /aws/service catalog and /aws/reference/secretsmanager references.
// handled is false for ordinary names.
func (b *InMemoryBackend) getCatalogParameter(
	ctx context.Context, name string, withDecryption bool,
) (ParameterOutput, bool, error) {
	if isSecretReference(name) {
		out, err := b.getSecretReference(ctx, name, withDecryption)

		return out, true, err
	}

	baseName, selector := splitParameterSelector(name)
	if !isPublicParameterName(baseName) {
		return ParameterOutput{}, false, nil
	}

	p, err := resolvePublicParameter(baseName, selector, getRegion(ctx))
	if err != nil {
		return ParameterOutput{}, true, err
	}

	return p.toParameterOutput(), true, nil
}
