---
service: azurefunctions
sdk_module: azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6@v6.0.0   # go.mod dependency (test-only): drives the ARM plane in internal/app/azurefunctions_sdk_test.go
last_audit_commit: 959104c86
last_audit_date: 2026-10-25
overall: B
ops:
  InvokeHTTPFunction: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Any request on <app>.<domain> is reverse-proxied to the app's official Azure Functions v4 host container, which enforces authLevel itself (anonymous, function via ?code= or x-functions-key, admin via the master key). Verified against a real node:4-node20 host in docker_test.go (opt-in GOPHERSTACK_AZFN_DOCKER_TEST=1)."}
  ZipDeploy: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST or PUT /api/zipdeploy on <app>.scm.<domain> with the publishing credentials as Basic auth (or any Bearer token); validates the zip, stores it, records a deployment and discards the running host so the next request serves the new code. isAsync=true answers 202 with a Location."}
  OneDeploy: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /api/publish?type=zip behaves like zipdeploy; other types answer 400."}
  ListDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /api/deployments (newest first, capped at 50)."}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /api/deployments/{id|latest}."}
  DownloadSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /api/zip/site/wwwroot/ returns the deployed package."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.Web (internal/app adapter), verified with armappservice v6: serverfarms (PUT, GET, DELETE, lists; numberOfSites; delete refused while sites exist), sites (kind must contain functionapp; PUT, GET, PATCH, DELETE, lists; start, stop, restart; site name unique across groups), config/web, appsettings, connectionstrings, metadata, authsettings, authsettingsV2, logs, slotConfigNames, pushsettings (GET, PUT, PATCH, POST .../list), publishingcredentials/list, basicPublishingCredentialsPolicies ftp and scm, publishxml, functions (list and get from the running host, or from function.json files when no container runtime is available), function keys and secrets (listkeys, listsecrets, PUT and DELETE keys), host keys (listkeys, PUT and DELETE host/default/{functionKeys|systemKeys}/{name}), checknameavailability."}
  runtimes: {status: ok, note: "node 18/20/22/24, python 3.9-3.13, dotnet-isolated 8/9/10, dotnet (in-process, v4), java 11/17/21/25, powershell 7.2/7.4 and custom handlers (base:4) run in mcr.microsoft.com/azure-functions/* v4 images chosen from FUNCTIONS_WORKER_RUNTIME and linuxFxVersion; linuxFxVersion DOCKER|<image> runs the user's own image. App settings are the container environment. Only node was run end to end; the other image tags exist in the registry but were not started."}
  keys: {status: ok, note: "The emulator owns the master, host and function keys and writes them to the host's file secret store (AzureWebJobsSecretStorageType=files); the host watches the files, so key changes apply without a restart."}
  host_routing: {status: ok, note: "<app>.<domain>[:port] reaches the app, <app>.scm.<domain> its Kudu API; any other host whose first label names an app reaches it, else the only app when there is exactly one. The listener is separate from the AWS router, so no route collisions are possible. Kudu paths on a host that is not an app's own hostname go to Kudu."}
  persistence: {status: ok, note: "Apps, settings, keys, deployments and package bytes are in the snapshot; hosts restart lazily on the next request."}
gaps: []
deferred: []
items_still_open:
  - "Non-HTTP triggers (timer, queue, blob, Service Bus, Event Hubs) are run by the real host but are unverified here: containers cannot reach the emulator's Storage, Service Bus or Event Hubs ports (no host-gateway or extra-hosts support in pkgs/container) and a timer needs a reachable AzureWebJobsStorage."
  - "Remote build is not implemented: Python and Node dependencies must be packed into the zip (SCM_DO_BUILD_DURING_DEPLOYMENT is ignored)."
  - "WEBSITE_RUN_FROM_PACKAGE with a URL is ignored; the deployed zip is always used."
  - "The listener is plain HTTP; Kudu only supports zipdeploy, publish?type=zip, deployments and the site zip download (no vfs, command, logstream or webjobs APIs)."
  - "Windows function apps and Functions runtime versions other than ~4 run in the same Linux v4 images."
  - "Not implemented: DELETE sites/{name}/functions/{fn}, deployment slots, functions/admin/token, syncfunctiontriggers, Flex Consumption (functionAppConfig), app-level managed identity, VNet integration, custom domains and certificates, easy auth (authsettings are stored only)."
  - "Hosts are never reaped while idle; a stopped, redeployed or deleted app, or an emulator shutdown, removes them."
  - "No dashboard page."
leaks: {status: ok, note: "Shutdown closes the listener and removes every host container and scratch directory; verified with the leak check."}
---

Initial implementation (2026-10-25): data plane verified against a real Azure Functions v4 node host (opt-in docker test) and fake-runtime tests; ARM verified against armappservice v6.0.0.
