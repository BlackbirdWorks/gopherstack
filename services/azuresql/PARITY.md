---
service: azuresql
sdk_module: azure-sdk-for-go/sdk/resourcemanager/sql/armsql@v1.2.0   # go.mod dependency (test-only): internal/app/azuresql_sdk_test.go drives Microsoft.Sql through ARM
last_audit_commit: 4939f3c07
last_audit_date: 2026-10-22
overall: B
ops:
  Servers_CreateOrUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT Microsoft.Sql/servers/{name}: 201 on create and 200 on update, both with Azure-AsyncOperation and Location headers that resolve to a finished operation. administratorLogin and administratorLoginPassword (SQL Server complexity rules, reserved logins), version 12.0, minimalTlsVersion, publicNetworkAccess, restrictOutboundNetworkAccess, tags, identity and administrators (Entra) are stored; unmodelled properties round-trip. fullyQualifiedDomainName is <name>.database.windows.net. The server starts one SQL Server container (mcr.microsoft.com/mssql/server:2022-CU26-ubuntu-22.04) on a pooled loopback TDS port, reported as properties.gopherstackEndpoint, and the admin login is created with sysadmin rights."}
  Servers_Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH merges tags and properties; a new administratorLoginPassword resets the login password in the engine."}
  Servers_Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE answers 202 with an async operation; a missing server answers 204. Removes the container, its databases and its port."}
  Servers_Get_List: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET, list by resource group and list by subscription."}
  Databases_CreateOrUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT servers/{s}/databases/{d} creates the database in the engine (CREATE DATABASE with the identifier quoted by QUOTENAME in the server, optional COLLATE). sku, collation (immutable once set), maxSizeBytes, elasticPoolId and unmodelled properties are stored; databaseId, status Online and currentSku are reported. createMode other than Default is rejected."}
  Databases_Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH merges tags, sku and properties of an existing database."}
  Databases_Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "DROP DATABASE in the engine after kicking sessions out; a missing database answers 204."}
  Databases_ListByServer: {wire: ok, errors: ok, state: ok, persist: ok, note: "Lists the user databases; the system master database is not listed."}
  FirewallRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT/GET/DELETE and list of firewallRules with IPv4 range validation; AllowAllWindowsAzureIps must span 0.0.0.0. Rules are stored; connections to the TDS port are not filtered."}
  ElasticPools: {wire: ok, errors: ok, state: partial, persist: ok, note: "PUT/GET/DELETE and list of elasticPools with sku, maxSizeBytes, perDatabaseSettings and licenseType stored. A database may reference a pool of its server; a pool with databases cannot be deleted. Pool limits are not enforced by the engine."}
  Administrators: {wire: ok, errors: ok, state: partial, persist: ok, note: "PUT/GET/DELETE and list of administrators/ActiveDirectory (login, GUID sid, tenantId), also filled from properties.administrators on the server. Stored only: the engine does not authenticate Entra identities."}
  TransparentDataEncryption: {wire: ok, errors: ok, state: partial, persist: ok, note: "databases/{d}/transparentDataEncryption/current reports Enabled until a PUT stores Enabled or Disabled. The engine's own TDE is not changed."}
  SettingSingletons: {wire: ok, errors: ok, state: partial, persist: ok, note: "Singleton children Terraform reads and writes (server and database auditingSettings, extendedAuditingSettings, securityAlertPolicies, devOpsAuditingSettings, connectionPolicies, encryptionProtector, azureADOnlyAuthentications, sqlVulnerabilityAssessments, backup retention and geo backup policies, advancedThreatProtectionSettings, ledgerDigestUploads) report documented defaults and store what a PUT sets. None of them changes engine behaviour."}
  OperationResults: {wire: ok, errors: ok, state: ok, persist: n/a, note: "locations/{l}/{server,database,elasticPool}AzureAsyncOperation and *OperationResults resolve operations issued by this process; unknown IDs answer 404."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.Sql provider (rp_sql*.go): ARM keeps resource metadata and child resources; the SQL Server engines live in this service behind the azurearm.SQL seam (adapter in internal/app/azuresql_wiring.go). Applied, re-planned with no drift and destroyed by test/terraform/azure/sql_test.go (azurerm_mssql_server, _database, _elasticpool, _firewall_rule)."}
  engine: {status: ok, note: "One SQL Server container per logical server, started through pkgs/container with ports from the shared pool and container.CreateAndStartRetry. Databases and the admin login are created with sqlcmd run inside the container (/opt/mssql-tools18/bin/sqlcmd -C -x), logging in as sa with the container's own MSSQL_SA_PASSWORD, so no password is on the exec command line except the admin login's own, which is quoted in the engine with QUOTENAME. Without a container runtime (or --azure-sql-engine=none) servers are state-only: ARM state is real, the TDS port is absent."}
  sdk_compat: {status: ok, note: "armsql v1.2.0 servers, databases, firewall rules, Entra administrators, elastic pools and transparent data encryption; azurerm_mssql_server, azurerm_mssql_database, azurerm_mssql_elasticpool and azurerm_mssql_firewall_rule apply without drift."}
gaps:
  - "Azure SQL managed instances, SQL pools, replicas, failover groups, geo-replication, restore, copy and long-term retention data flow are not implemented; createMode other than Default is rejected."
  - "Entra authentication, firewall rules, minimalTlsVersion, publicNetworkAccess, elastic-pool limits and database sku/edition limits are stored but not enforced by the engine."
  - "Database sku and maxSizeBytes do not size the engine; every database is a plain SQL Server database."
  - "Server create waits for the container, so a first run that pulls the image can take minutes; ARM operations are reported complete as soon as the request returns."
  - "Engine data does not survive a restore: containers start empty and tracked databases are created again."
  - "Server-level administrator login is a sysadmin of its container, not Azure's dbmanager and loginmanager roles."
  - "The restorableDroppedDatabases collection is always empty."
deferred: []
items_still_open:
  - "Managed instances, failover groups, geo-replication, restore, copy and long-term retention data flow are not implemented."
  - "Entra authentication, firewall rules, TLS and public-network settings, and pool or sku limits are stored but not enforced."
  - "Engine data does not survive a snapshot restore."
  - "No dashboard page."
leaks: {status: clean, note: "Reset and Shutdown stop every container and release its port and the runtime client; the package runs under testleak."}
---

## Notes

The service has no HTTP surface of its own: ARM (services/azurearm) owns the Microsoft.Sql resource model and calls this backend through the azurearm.SQL seam. ids are the lower-cased ARM server IDs.

Passwords: ARM never stores the admin password; the backend keeps it to re-create the login after a restore, so snapshots carry it (as the GCP SQL and Redis emulators do).

Verified against: armsql v1.2.0 and azurerm 4.81.0 (mssql server, database, elastic pool, firewall rule).
