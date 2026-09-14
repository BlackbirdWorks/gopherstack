package acmpca

import (
	"crypto/x509"
	"fmt"
	"strings"
)

// maxSubordinatePathLen is the highest PathLenN AWS Private CA defines a
// SubordinateCACertificate/BlankSubordinateCACertificate/BlankRootCACertificate
// template variant for (template-varieties.md lists PathLen0..PathLen3 only).
const maxSubordinatePathLen = 3

// templateProfile captures the fixed X.509 extension values a TemplateArn's
// family mandates, per the documented per-template extension tables --
// https://docs.aws.amazon.com/privateca/latest/userguide/template-definitions.html.
// keyUsageFixed/ekuFixed false means that extension is NOT fixed by this
// family: it is left to CSR/API passthrough (the Blank* families, and the
// EKU-less CA families).
type templateProfile struct {
	eku                      []x509.ExtKeyUsage
	keyUsage                 x509.KeyUsage
	pathLen                  int // -1 = no pathLenConstraint asserted
	isCA                     bool
	keyUsageFixed            bool
	ekuFixed                 bool
	ekuCritical              bool
	basicConstraintsCritical bool
	// noCRLDP is set for the RootCACertificate family: "No CRL information is
	// specified because a self-signed certificate cannot be revoked"
	// (template-definitions.md's RootCACertificate/V1 section).
	noCRLDP bool
}

// resolvedTemplate is a parsed, validated TemplateArn: which passthrough
// sources are honored (template-varieties.md) plus the family's fixed
// extension profile.
type resolvedTemplate struct {
	profile             templateProfile
	allowAPIPassthrough bool
	allowCSRPassthrough bool
}

const templateArnPrefix = "arn:aws:acm-pca:::template/"

// resolveTemplateArn parses and validates templateArn, defaulting an empty
// value to EndEntityCertificate/V1 (IssueCertificateInput.TemplateArn's
// documented default: "If this parameter is not provided, Amazon Web
// Services Private CA defaults to the EndEntityCertificate/V1 template").
// Anything that doesn't resolve to one of the documented template names
// returns ErrInvalidArgs (IssueCertificate's own deserializeOpError models
// InvalidArgsException -- see errors.go).
func resolveTemplateArn(templateArn string) (resolvedTemplate, error) {
	name := "EndEntityCertificate"

	if templateArn != "" {
		if !strings.HasPrefix(templateArn, templateArnPrefix) || !strings.HasSuffix(templateArn, "/V1") {
			return resolvedTemplate{}, fmt.Errorf("%w: malformed TemplateArn %q", ErrInvalidArgs, templateArn)
		}

		name = strings.TrimSuffix(strings.TrimPrefix(templateArn, templateArnPrefix), "/V1")
	}

	allowAPI, allowCSR := passthroughKind(&name)
	pathLen := extractPathLen(&name)

	profile, err := templateProfileFor(name, pathLen)
	if err != nil {
		return resolvedTemplate{}, err
	}

	return resolvedTemplate{profile: profile, allowAPIPassthrough: allowAPI, allowCSRPassthrough: allowCSR}, nil
}

// passthroughKind strips a trailing _APICSRPassthrough/_APIPassthrough/
// _CSRPassthrough suffix from *name (if any) and reports which sources it
// enables (template-varieties.md).
func passthroughKind(name *string) (bool, bool) {
	if trimmed, ok := strings.CutSuffix(*name, "_APICSRPassthrough"); ok {
		*name = trimmed

		return true, true
	}

	if trimmed, ok := strings.CutSuffix(*name, "_APIPassthrough"); ok {
		*name = trimmed

		return true, false
	}

	if trimmed, ok := strings.CutSuffix(*name, "_CSRPassthrough"); ok {
		*name = trimmed

		return false, true
	}

	return false, false
}

// extractPathLen strips a trailing _PathLenN suffix from *name (if any) and
// returns N, or -1 if none was present.
func extractPathLen(name *string) int {
	for n := 0; n <= maxSubordinatePathLen; n++ {
		suffix := fmt.Sprintf("_PathLen%d", n)
		if trimmed, ok := strings.CutSuffix(*name, suffix); ok {
			*name = trimmed

			return n
		}
	}

	return -1
}

func templateProfileFor(family string, pathLen int) (templateProfile, error) {
	switch family {
	case "EndEntityCertificate":
		return requireNoPathLen(pathLen, leafProfile(
			x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment,
			[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, false,
		))
	case "EndEntityClientAuthCertificate":
		return requireNoPathLen(pathLen, leafProfile(
			x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment,
			[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, false,
		))
	case "EndEntityServerAuthCertificate":
		return requireNoPathLen(pathLen, leafProfile(
			x509.KeyUsageDigitalSignature|x509.KeyUsageKeyEncipherment,
			[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, false,
		))
	case "CodeSigningCertificate":
		return requireNoPathLen(pathLen, leafProfile(
			x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning}, true,
		))
	case "OCSPSigningCertificate":
		return requireNoPathLen(pathLen, leafProfile(
			x509.KeyUsageDigitalSignature, []x509.ExtKeyUsage{x509.ExtKeyUsageOCSPSigning}, true,
		))
	case "RootCACertificate":
		return requireNoPathLen(pathLen, templateProfile{
			isCA: true, pathLen: -1, keyUsageFixed: true,
			keyUsage:                 x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
			ekuFixed:                 true,
			basicConstraintsCritical: true,
			noCRLDP:                  true,
		})
	case "SubordinateCACertificate":
		if pathLen < 0 {
			return templateProfile{}, fmt.Errorf(
				"%w: SubordinateCACertificate template requires a PathLenN suffix", ErrInvalidArgs,
			)
		}

		return templateProfile{
			isCA: true, pathLen: pathLen, keyUsageFixed: true,
			keyUsage:                 x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
			ekuFixed:                 true,
			basicConstraintsCritical: true,
		}, nil
	case "BlankEndEntityCertificate":
		return requireNoPathLen(pathLen, templateProfile{pathLen: -1, basicConstraintsCritical: false})
	case "BlankEndEntityCertificate_CriticalBasicConstraints":
		return requireNoPathLen(pathLen, templateProfile{pathLen: -1, basicConstraintsCritical: true})
	case "BlankSubordinateCACertificate":
		if pathLen < 0 {
			return templateProfile{}, fmt.Errorf(
				"%w: BlankSubordinateCACertificate template requires a PathLenN suffix", ErrInvalidArgs,
			)
		}

		return templateProfile{isCA: true, pathLen: pathLen, basicConstraintsCritical: true}, nil
	case "BlankRootCACertificate":
		return templateProfile{isCA: true, pathLen: pathLen, basicConstraintsCritical: true, noCRLDP: true}, nil
	default:
		return templateProfile{}, fmt.Errorf("%w: unrecognized TemplateArn %q", ErrInvalidArgs, family)
	}
}

func requireNoPathLen(pathLen int, p templateProfile) (templateProfile, error) {
	if pathLen >= 0 {
		return templateProfile{}, fmt.Errorf("%w: this template does not support a PathLenN suffix", ErrInvalidArgs)
	}

	return p, nil
}

func leafProfile(ku x509.KeyUsage, eku []x509.ExtKeyUsage, ekuCritical bool) templateProfile {
	return templateProfile{
		pathLen: -1, keyUsageFixed: true, keyUsage: ku,
		ekuFixed: true, eku: eku, ekuCritical: ekuCritical, basicConstraintsCritical: true,
	}
}

// applyTemplateFixedExtensions applies profile's fixed KeyUsage/
// ExtendedKeyUsage/BasicConstraints to tmpl, overriding whatever CSR- or
// ApiPassthrough-derived values were set earlier -- per the documented
// precedence rule ("the template definition has highest priority, followed
// by API passthrough values, followed by CSR passthrough extensions" --
// template-order-of-operations.md), this must run after CSR/API application.
func applyTemplateFixedExtensions(tmpl *x509.Certificate, profile templateProfile) error {
	if profile.keyUsageFixed {
		tmpl.KeyUsage = profile.keyUsage
	}

	if profile.ekuFixed {
		tmpl.ExtKeyUsage = profile.eku
		tmpl.UnknownExtKeyUsage = nil

		if profile.ekuCritical && len(profile.eku) > 0 {
			ext, err := criticalExtKeyUsageExtension(profile.eku)
			if err != nil {
				return err
			}

			tmpl.ExtKeyUsage = nil
			tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, ext)
		}
	}

	applyBasicConstraints(tmpl, profile)

	return nil
}

func applyBasicConstraints(tmpl *x509.Certificate, profile templateProfile) {
	tmpl.IsCA = profile.isCA
	tmpl.BasicConstraintsValid = true
	tmpl.MaxPathLen = 0
	tmpl.MaxPathLenZero = false

	if profile.pathLen >= 0 {
		tmpl.MaxPathLen = profile.pathLen
		tmpl.MaxPathLenZero = profile.pathLen == 0
	}

	if profile.basicConstraintsCritical {
		return
	}

	// crypto/x509 always marshals BasicConstraintsValid as Critical:true
	// (x509.go's marshalBasicConstraints); hand-build a non-critical copy
	// instead for the one family that documents it as non-critical.
	ext, err := basicConstraintsExtension(profile.isCA, profile.pathLen, false)
	if err != nil {
		// Both inputs are already-validated bools/ints; asn1.Marshal cannot
		// fail on them. Fall back to the (critical) default rather than drop
		// the extension entirely.
		return
	}

	tmpl.BasicConstraintsValid = false
	tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, ext)
}

// crlDistributionPoints returns the CRL Distribution Points URL(s) to embed
// in a certificate issued by ca, per its RevocationConfiguration.CrlConfiguration:
// the CustomCname when set (CrlConfiguration.CustomCname's doc comment: it is
// what gets "placed into the certificate CRL Distribution Points extension"),
// otherwise the S3 bucket; none at all when CRLs are disabled,
// CrlDistributionPointExtensionConfiguration.OmitExtension is set, or profile
// is a RootCACertificate/BlankRootCACertificate family (noCRLDP: "a
// self-signed certificate cannot be revoked" -- template-definitions.md).
func crlDistributionPoints(ca *CertificateAuthority, profile templateProfile) []string {
	if profile.noCRLDP {
		return nil
	}

	if ca.RevocationConfiguration == nil || ca.RevocationConfiguration.CrlConfiguration == nil {
		return nil
	}

	crl := ca.RevocationConfiguration.CrlConfiguration
	if !crl.Enabled || crl.OmitExtension {
		return nil
	}

	host := crl.CustomCname
	if host == "" {
		host = crl.S3BucketName + ".s3.amazonaws.com"
	}

	return []string{"http://" + host + "/crl"}
}
