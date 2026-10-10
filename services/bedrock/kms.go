package bedrock

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
)

// kmsKeyARN resolves a KmsKeyId-style member (key ID, alias or ARN) to the ARN the
// *KmsKeyArn output members report. An empty id stays empty.
func kmsKeyARN(region, accountID, id string) string {
	switch {
	case id == "", strings.HasPrefix(id, "arn:"):
		return id
	case strings.HasPrefix(id, "alias/"):
		return arn.Build("kms", region, accountID, id)
	default:
		return arn.Build("kms", region, accountID, "key/"+id)
	}
}

type kmsSibling interface {
	GetKMSHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

// kmsARN resolves a KmsKeyId-style member to the key's ARN. With KMS wired the key must exist
// (an alias resolves to its key's ARN); otherwise the ARN is derived from the identifier.
func (b *InMemoryBackend) kmsARN(id string) (string, error) {
	if id == "" {
		return "", nil
	}

	if s, ok := b.appConfig.(kmsSibling); ok {
		if h, hok := s.GetKMSHandler().(*kmsbackend.Handler); hok && h != nil && h.Backend != nil {
			out, err := h.Backend.DescribeKey(context.Background(), &kmsbackend.DescribeKeyInput{KeyID: id})
			if err != nil || out == nil || out.KeyMetadata.Arn == "" {
				return "", fmt.Errorf("%w: KMS key %q does not exist", ErrValidation, id)
			}

			return out.KeyMetadata.Arn, nil
		}
	}

	return kmsKeyARN(b.region, b.accountID, id), nil
}
