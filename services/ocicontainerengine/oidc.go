package ocicontainerengine

import (
	"encoding/base64"
	"net/url"
)

// checkOIDC applies the constraints the containerengine SDK documents on OpenIdConnectTokenAuthenticationConfig.
func checkOIDC(in *OIDCAuth) *APIError {
	if in == nil {
		return nil
	}

	if in.IsEnabled == nil {
		return errMissing("options.openIdConnectTokenAuthenticationConfig.isOpenIdConnectAuthEnabled is required")
	}

	if in.IssuerURL != nil {
		u, err := url.Parse(*in.IssuerURL)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return errParam("options.openIdConnectTokenAuthenticationConfig.issuerUrl must be an https:// URL")
		}
	}

	if aerr := checkBase64("caCertificate", in.CACertificate); aerr != nil {
		return aerr
	}

	return checkBase64("configurationFile", in.ConfigurationFile)
}

func checkBase64(name string, v *string) *APIError {
	if v == nil {
		return nil
	}

	if _, err := base64.StdEncoding.DecodeString(*v); err != nil {
		return errParam("options.openIdConnectTokenAuthenticationConfig." + name + " must be base64 encoded")
	}

	return nil
}
