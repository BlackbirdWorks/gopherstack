package azurekeyvault

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
)

// keyInfo describes a parsed private key.
func keyInfo(priv any) (string, string, int) {
	switch k := priv.(type) {
	case *ecdsa.PrivateKey:
		return ktyEC, curveName(k.Curve), 0
	case *rsa.PrivateKey:
		return ktyRSA, "", k.N.BitLen()
	}

	return ktyRSA, "", 0
}

func samePublicKey(a, b any) bool {
	ka, ok := a.(interface{ Equal(crypto.PublicKey) bool })
	if !ok {
		return false
	}

	return ka.Equal(b)
}

// derivePolicy builds the policy of an imported certificate from the
// certificate itself, honouring any caller-supplied policy fields.
func derivePolicy(in *CertPolicy, certDER, keyDER []byte, now int64) (*CertPolicy, error) {
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, errBadParameter("The certificate could not be parsed.")
	}

	priv, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, errBadParameter("The private key could not be parsed.")
	}

	kty, crv, size := keyInfo(priv)
	base := &CertPolicy{
		KeyProps:  &KeyProps{Kty: kty, Crv: crv, KeySize: size},
		X509Props: &X509Props{Subject: cert.Subject.String()},
		Issuer:    &IssuerRef{Name: "Unknown"},
	}

	if in != nil {
		if in.SecretProps != nil {
			base.SecretProps = in.SecretProps
		}

		base.LifetimeActions = in.LifetimeActions
	}

	out := base.normalized(now)
	out.Issuer = &IssuerRef{Name: "Unknown"}

	return out, nil
}
