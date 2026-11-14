package gcpsecretmanager

import (
	"strings"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// validateTags checks the Resource Manager tag binding form: key "<parent>/<shortName>" or
// "tagKeys/<id>", non-empty value (docs.cloud.google.com/resource-manager/docs/tags/tags-creating-and-managing).
func validateTags(tags map[string]string) error {
	for k, v := range tags {
		if !strings.Contains(k, "/") || strings.HasPrefix(k, "/") || strings.HasSuffix(k, "/") {
			return errInvalid("Invalid tag key %q. Expected <parent>/<shortName> or tagKeys/<id>.", k)
		}

		if v == "" {
			return errInvalid("Tag %q must have a value.", k)
		}
	}

	return nil
}

func validateSecretType(t secretmanagerpb.Secret_SecretType) error {
	if _, ok := secretmanagerpb.Secret_SecretType_name[int32(t)]; !ok {
		return errInvalid("Invalid secret_type %d.", int32(t))
	}

	return nil
}
