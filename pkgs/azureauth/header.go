package azureauth

import "strings"

// Scheme identifies which Azure Storage Authorization scheme a header uses.
type Scheme int

const (
	// SchemeSharedKey is the standard "SharedKey <account>:<signature>" scheme.
	SchemeSharedKey Scheme = iota
	// SchemeSharedKeyLite is the legacy "SharedKeyLite <account>:<signature>"
	// scheme, kept for older clients and the Table service.
	SchemeSharedKeyLite
)

// String returns the wire name of the scheme ("SharedKey" or "SharedKeyLite").
func (s Scheme) String() string {
	if s == SchemeSharedKeyLite {
		return "SharedKeyLite"
	}

	return "SharedKey"
}

const (
	sharedKeyPrefix     = "SharedKey "
	sharedKeyLitePrefix = "SharedKeyLite "
)

// Authorization is the parsed form of an Azure Storage Authorization header:
// "SharedKey <account>:<signature>" or "SharedKeyLite <account>:<signature>".
type Authorization struct {
	// Account is the storage account name that signed the request.
	Account string
	// Signature is the base64-encoded HMAC-SHA256 signature the client sent.
	Signature string
	// Scheme is which of SharedKey / SharedKeyLite was used.
	Scheme Scheme
}

// ParseAuthorizationHeader parses the value of an Authorization header
// carrying a SharedKey or SharedKeyLite credential. The bool return is false
// when the header is empty, uses an unrecognised scheme, or is missing the
// account name or signature — it never inspects or validates the signature
// itself, so a structurally well-formed header with a wrong signature still
// parses successfully. Use [VerifySharedKey] to check the signature.
func ParseAuthorizationHeader(header string) (Authorization, bool) {
	if header == "" {
		return Authorization{}, false
	}

	scheme := SchemeSharedKey

	tail, ok := strings.CutPrefix(header, sharedKeyLitePrefix)
	if ok {
		scheme = SchemeSharedKeyLite
	} else {
		tail, ok = strings.CutPrefix(header, sharedKeyPrefix)
	}

	if !ok {
		return Authorization{}, false
	}

	account, signature, found := strings.Cut(tail, ":")
	if !found || account == "" || signature == "" {
		return Authorization{}, false
	}

	// Reject embedded whitespace: a well-formed credential never contains
	// spaces, so this catches trailing/leading junk appended to either field.
	if containsSpace(account) || containsSpace(signature) {
		return Authorization{}, false
	}

	return Authorization{Scheme: scheme, Account: account, Signature: signature}, true
}

// containsSpace reports whether s contains any ASCII whitespace.
func containsSpace(s string) bool {
	return strings.ContainsAny(s, " \t\r\n")
}
