package bedrock

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
