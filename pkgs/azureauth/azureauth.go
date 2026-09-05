// Package azureauth implements Azure Storage's SharedKey and SharedKeyLite
// Authorization-header scheme: structural parsing of the header, the
// canonicalization + HMAC-SHA256 signing algorithm described in Microsoft's
// "Authorize with Shared Key" REST reference, and an explicit, opt-in
// verification call.
//
// It is intentionally self-contained (standard library only) so it can be
// shared by every Azure-flavoured service package (blob, queue, table,
// cosmosdb) without any of them depending on one another.
//
// Mirroring the rest of gopherstack's auth philosophy (see
// services/s3/sigv4.go), this package is permissive by default: parsing an
// Authorization header never fails because a signature happens to be wrong,
// and nothing in the parsing path performs cryptographic verification.
// Callers that want enforcement call [VerifySharedKey] explicitly.
package azureauth

// Azurite well-known development storage account. Real Azure SDKs configured
// with UseDevelopmentStorage=true, or an explicit connection string naming
// this account, sign requests with this fixed key — this is the same
// "fixed public dev secret" model Azurite itself uses, and lets gopherstack
// accept unmodified Azurite-targeting client configuration out of the box.
const (
	// DefaultAccountName is Azurite's fixed development storage account name.
	DefaultAccountName = "devstoreaccount1"

	// DefaultAccountKey is Azurite's fixed, publicly published development
	// storage account key (base64-encoded).
	DefaultAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
