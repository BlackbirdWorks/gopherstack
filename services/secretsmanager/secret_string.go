package secretsmanager

import (
	"context"
	"strings"
)

// SecretString returns the current string value of a secret by name or ARN, in the ARN's region.
func (b *InMemoryBackend) SecretString(ctx context.Context, secretID string) (string, error) {
	if parts := strings.SplitN(secretID, ":", arnRegionParts); len(parts) == arnRegionParts && parts[3] != "" {
		ctx = context.WithValue(ctx, regionContextKey{}, parts[3])
	}

	out, err := b.GetSecretValue(ctx, &GetSecretValueInput{SecretID: secretID})
	if err != nil {
		return "", err
	}

	return out.SecretString, nil
}

const arnRegionParts = 5
