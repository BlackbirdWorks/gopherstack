# IAM

In-memory IAM. Optional policy enforcement on every API call.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws iam create-role --role-name MyRole --assume-role-policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws iam create-policy --policy-name MyPolicy --policy-document \
  '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}'

aws iam attach-role-policy --role-name MyRole \
    --policy-arn arn:aws:iam::000000000000:policy/MyPolicy

aws iam create-user --user-name alice
aws iam create-access-key --user-name alice
```

## Supported operations

Most families have the usual `Create`, `Get`, `List`, `Update`, `Delete`, `Tag` and `Untag` operations.

| Area | Operations |
|------|------------|
| Users, groups, roles | `CreateUser`, `CreateGroup`, `CreateRole`, `AddUserToGroup`, `RemoveUserFromGroup`, `ListGroupsForUser`, `UpdateAssumeRolePolicy`, `UpdateRoleDescription`, `Put/DeleteRolePermissionsBoundary`, `Put/DeleteUserPermissionsBoundary`, and their Get/List/Update/Delete counterparts |
| Managed policies | `CreatePolicy`, `CreatePolicyVersion`, `SetDefaultPolicyVersion`, `ListPolicyVersions`, `ListEntitiesForPolicy`, `Attach/Detach{User,Group,Role}Policy`, `ListAttached*Policies` |
| Inline policies | `Put/Get/Delete/List{User,Group,Role}Policy` |
| Credentials | `CreateAccessKey`, `UpdateAccessKey`, `GetAccessKeyLastUsed`, `CreateLoginProfile`, `ChangePassword`, `Upload/Delete/List/UpdateSSHPublicKey`, `Upload/Delete/List/UpdateSigningCertificate`, `CreateServiceSpecificCredential`, `ResetServiceSpecificCredential` |
| MFA | `CreateVirtualMFADevice`, `EnableMFADevice`, `DeactivateMFADevice`, `ResyncMFADevice`, `ListMFADevices`, `ListVirtualMFADevices` |
| Instance profiles | `CreateInstanceProfile`, `AddRoleToInstanceProfile`, `RemoveRoleFromInstanceProfile`, `ListInstanceProfilesForRole`, `Get/Delete/List` |
| Identity providers | OpenID Connect and SAML providers: `Create/Get/Delete/List`, `AddClientIDToOpenIDConnectProvider`, `RemoveClientIDFromOpenIDConnectProvider`, `UpdateOpenIDConnectProviderThumbprint`, `UpdateSAMLProvider` |
| Certificates | `UploadServerCertificate`, `Get/Delete/List/UpdateServerCertificate` |
| Service-linked roles | `CreateServiceLinkedRole`, `DeleteServiceLinkedRole`, `GetServiceLinkedRoleDeletionStatus` |
| Account | `CreateAccountAlias`, `DeleteAccountAlias`, `ListAccountAliases`, `Get/Update/DeleteAccountPasswordPolicy`, `GetAccountSummary`, `GetAccountProperties`, `PutAccountProperties`, `GetAccountAuthorizationDetails`, `SetSecurityTokenServicePreferences` |
| Reports | `GenerateCredentialReport`, `GetCredentialReport`, `GenerateServiceLastAccessedDetails`, `GetServiceLastAccessedDetails`, `GetServiceLastAccessedDetailsWithEntities`, `ListPoliciesGrantingServiceAccess`, `GenerateOrganizationsAccessReport`, `GetOrganizationsAccessReport` |
| Simulation | `SimulateCustomPolicy`, `SimulatePrincipalPolicy`, `GetContextKeysForCustomPolicy`, `GetContextKeysForPrincipalPolicy` |
| Organizations integration | `Enable/DisableOrganizationsRootCredentialsManagement`, `Enable/DisableOrganizationsRootSessions`, `ListOrganizationsFeatures` |
| Outbound federation | `Enable/DisableOutboundWebIdentityFederation`, `GetOutboundWebIdentityFederationInfo` |
| Delegation requests | `Create/Get/List/Update/Accept/Reject/AssociateDelegationRequest`, `SendDelegationToken`, `AcquireRole` |
| Role templates | `GetRoleTemplateVersion` |

## Policy enforcement

Off by default: every call succeeds regardless of attached policies. Turn it on to evaluate each request against the caller's policies before dispatch.

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--enforce-iam` | `GOPHERSTACK_ENFORCE_IAM`, `ENFORCE_IAM` | `false` | Evaluate and deny |
| `--iam-enforce-scps` | `GOPHERSTACK_IAM_ENFORCE_SCPS` | `true` | With enforcement on, also evaluate Organizations SCPs for member-account principals |
| `--iam-audit` | `GOPHERSTACK_IAM_AUDIT`, `IAM_SOFT_MODE` | `false` | Evaluate and record each request in the IAM Policy Stream, never deny |

Evaluated: `Allow`/`Deny` (explicit deny wins), action and resource wildcards, `NotAction`/`NotResource`, condition operators (string, `Bool`, `Null`, IP address, ARN, numeric, `ForAllValues`/`ForAnyValue`, `IfExists`) and policy variables such as `${aws:username}`.

## Differences from AWS

> [!WARNING]
> Enforcement is single-account. The emulator's STS `GetSessionToken` mints root sessions only for the default (management) account, so a member-account root session cannot be created through the SDK.

- RCPs do not apply to KMS keys and Secrets Manager secrets named by ID or name. Those backends hold one account's resources, owned by the default account. RCPs apply when the request names a member-account ARN.
- Access advisor `Granularity=ACTION_LEVEL` is not supported: it needs real per-action usage tracking.
- Role templates have no create or list operation in the SDK. Property values are untyped.
- The `aws_iam_security_token_service_preferences` Terraform resource fails a Terraform Core state-consistency check with provider v5.100.0. The operation itself works.
