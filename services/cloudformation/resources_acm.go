package cloudformation

import (
	"context"
	"fmt"
)

// ---- ACM ----

func (rc *ResourceCreator) createACMCertificate(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ACM == nil {
		return logicalID + "-stub", nil
	}

	domainName := strProp(props, "DomainName", params, physicalIDs)
	if domainName == "" {
		domainName = logicalID + ".example.com"
	}

	validationMethod := strProp(props, "ValidationMethod", params, physicalIDs)

	var sans []string
	if list, ok := props["SubjectAlternativeNames"].([]any); ok {
		for _, v := range list {
			if s := resolve(v, params, physicalIDs); s != "" {
				sans = append(sans, s)
			}
		}
	}

	cert, err := rc.backends.ACM.Backend.RequestCertificate(
		ctx,
		domainName,
		"AMAZON_ISSUED",
		validationMethod,
		"",
		"",
		"",
		"",
		sans,
	)
	if err != nil {
		return "", fmt.Errorf("create ACM certificate for %s: %w", domainName, err)
	}

	return cert.ARN, nil
}

func (rc *ResourceCreator) deleteACMCertificate(ctx context.Context, arn string) error {
	if rc.backends.ACM == nil {
		return nil
	}

	return rc.backends.ACM.Backend.DeleteCertificate(ctx, arn)
}
