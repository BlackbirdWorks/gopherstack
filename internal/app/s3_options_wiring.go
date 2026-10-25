package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

type s3KMSKeys struct{ kms kmsbackend.StorageBackend }

func (k s3KMSKeys) KMSKeyExists(ctx context.Context, keyID string) bool {
	_, err := k.kms.DescribeKey(ctx, &kmsbackend.DescribeKeyInput{KeyID: keyID})

	return err == nil
}

// wireS3Options applies the S3 and API Gateway option toggles that need cross-service
// or CLI context: SSE-KMS key validation, S3-scoped signature verification, CORS overrides.
func wireS3Options(appCtx *service.AppContext, byName map[string]service.Registerable) {
	cli, _ := appCtx.Config.(*CLI)

	if s3h, ok := byName["S3"].(*s3backend.S3Handler); ok {
		if kmsH, isKMS := byName["KMS"].(*kmsbackend.Handler); isKMS {
			s3h.SetKMSKeyChecker(s3KMSKeys{kms: kmsH.Backend})
		}

		if cli != nil && cli.S3.ValidateSignatures {
			s3h.WithPresignValidation(cli.SigV4Secret)
		}
	}

	if apigw, ok := byName["APIGateway"].(*apigwbackend.Handler); ok && cli != nil {
		apigw.DisableCustomCORS = cli.CORS.DisableCustomAPIGateway
	}
}
