package gcpidentitytoolkit

const (
	opAdminCreate         = "AdminCreate"
	opBatchGet            = "BatchGet"
	opBatchCreate         = "batchCreate"
	opBatchDelete         = "batchDelete"
	opQuery               = "query"
	opCreateSessionCookie = "CreateSessionCookie"
	opToken               = "Token"
	opPublicKeys          = "PublicKeys"
	opListOobCodes        = "ListOobCodes"
	opClearAccounts       = "ClearAccounts"
	opGetConfig           = "GetConfig"
	opUpdateConfig        = "UpdateConfig"
	opListTenants         = "ListTenants"
	opCreateTenant        = "CreateTenant"
	opGetTenant           = "GetTenant"
	opUpdateTenant        = "UpdateTenant"
	opDeleteTenant        = "DeleteTenant"
	opResetPassword       = "resetPassword"

	segTenants = "tenants"
	algRS256   = "RS256"
	keyKind    = "kind"

	algHMACSHA256 = "HMAC_SHA256"
	algHMACSHA512 = "HMAC_SHA512"
	algHMACSHA1   = "HMAC_SHA1"
	algHMACMD5    = "HMAC_MD5"
)
