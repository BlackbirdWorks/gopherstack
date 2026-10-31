---
service: ociloadbalancer
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every control-plane operation in the service tests
last_audit_commit: aebf6f27d
last_audit_date: 2026-10-30
overall: B
ops:
  CreateLoadBalancer: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body (SDK CreateLoadBalancerResponse has none). shapeName in 10Mbps/10Mbps-Micro/100Mbps/400Mbps/8000Mbps/flexible; flexible needs shapeDetails 10 <= min <= max <= 8000 Mbps, fixed shapes reject shapeDetails; 1-2 subnetIds (1 when private), checked and used for the private address through OCI Core when wired; ipAddresses assigned; nested listeners, hostnames, backendSets, certificates, sslCipherSuites, pathRouteSets and ruleSets validated like the individual calls. reservedIps, ipMode other than IPV4 and ipv6SubnetCidr are rejected."}
  GetLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "Full model with all sub-resources and an etag header; listeners carry the gopherstack-only gopherstackAddress field while serving. DELETED load balancers stay readable."}
  ListLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok, note: "compartmentId required; displayName, lifecycleState and detail=simple; sortBy TIMECREATED/DISPLAYNAME, sortOrder, limit, opc-next-page."}
  UpdateLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, delete protection, request id settings, tags; If-Match gives 412; 409 IncorrectState unless ACTIVE; 409 Conflict while another work request on the load balancer is unfinished."}
  DeleteLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETING then DELETED; 409 while delete protection is on."}
  ChangeLoadBalancerCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Body compartmentId; work request."}
  UpdateLoadBalancerShape: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same shape validation as create."}
  UpdateNetworkSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces networkSecurityGroupIds; the ids are not checked."}
  ListShapes: {wire: ok, errors: ok, state: ok, persist: ok, note: "Static shape catalogue."}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "ROUND_ROBIN, LEAST_CONNECTIONS, IP_HASH."}
  ListProtocols: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP, HTTP2, TCP."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the transition time on the injectable clock, then SUCCEEDED; errorDetails always empty."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: ok, note: "Per load balancer, newest first, paged."}
  ListLoadBalancerHealths: {wire: ok, errors: ok, state: ok, persist: ok, note: "Per compartment summaries from live health state."}
  GetLoadBalancerHealth: {wire: ok, errors: ok, state: ok, persist: ok, note: "Aggregated from backend set health after running due checks."}
  CreateBackendSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "policy, healthChecker (HTTP/TCP, defaults port 0=backend port, returnCode 200, retries 3, timeout 3000, interval 10000, urlPath /), backends, backendMaxConnections, sslConfiguration (certificateName bundle), app-cookie and LB-cookie session persistence (mutually exclusive). Names are letters, digits, - and _."}
  GetBackendSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "With etag header."}
  ListBackendSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted by name."}
  UpdateBackendSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "Full replacement including backends."}
  DeleteBackendSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 Conflict while a listener, path route set or routing policy references it."}
  GetBackendSetHealth: {wire: ok, errors: ok, state: ok, persist: ok, note: "OK when all backends are OK, CRITICAL when none is healthy and one is critical, WARNING for a mix, UNKNOWN when nothing is checked yet or the set is empty."}
  GetHealthChecker: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns the stored health checker."}
  UpdateHealthChecker: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces the health checker; invalid regex 400."}
  CreateBackend: {wire: ok, errors: ok, state: ok, persist: ok, note: "Name is ip:port; weight>=1 default 1, backup, drain, offline, maxConnections; 409 on duplicate."}
  GetBackend: {wire: ok, errors: ok, state: ok, persist: ok, note: "By ip:port."}
  ListBackends: {wire: ok, errors: ok, state: ok, persist: ok, note: "In creation order."}
  UpdateBackend: {wire: ok, errors: ok, state: ok, persist: ok, note: "weight, backup, drain, offline, maxConnections."}
  DeleteBackend: {wire: ok, errors: ok, state: ok, persist: ok, note: "Removes the backend."}
  GetBackendHealth: {wire: ok, errors: ok, state: ok, persist: ok, note: "Runs due checks first; healthCheckResults has one entry from the first subnet and address of the load balancer; offline backends report UNKNOWN with result OFFLINE."}
  CreateListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP, HTTP2 (needs sslConfiguration) or TCP; defaultBackendSetName, hostnameNames, pathRouteSetName xor routingPolicyName, ruleSetNames, sslConfiguration (certificateName with a private key, protocols TLSv1.2/TLSv1.3, cipherSuiteName, verifyPeerCertificate for client certificates), connectionConfiguration.idleTimeout. Listeners share a port only with disjoint hostnames (409). backendTcpProxyProtocol* and certificateIds are rejected."}
  UpdateListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "Full replacement."}
  DeleteListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "Closes the data-plane socket when it was the last listener on the port."}
  ListListenerRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "Rules of the listener's rule sets with their set names."}
  CreateHostname: {wire: ok, errors: ok, state: ok, persist: ok, note: "Wildcard prefix *. and suffix .* allowed."}
  GetHostname: {wire: ok, errors: ok, state: ok, persist: ok, note: "By name."}
  ListHostnames: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted."}
  UpdateHostname: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces hostname."}
  DeleteHostname: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while a listener uses it."}
  CreatePathRouteSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "EXACT_MATCH, FORCE_LONGEST_PREFIX_MATCH, PREFIX_MATCH, SUFFIX_MATCH routes to existing backend sets."}
  GetPathRouteSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "By name."}
  ListPathRouteSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted."}
  UpdatePathRouteSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces routes."}
  DeletePathRouteSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while a listener uses it."}
  CreateRoutingPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "conditionLanguageVersion V1; condition subset: all()/any(), http.request.url.path, url.query[k], headers[k], cookies[k], method with eq ne sw ew co and (i '..') case-insensitive literals; one FORWARD_TO_BACKENDSET action per rule."}
  GetRoutingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Actions decode as ForwardToBackendSet."}
  ListRoutingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted."}
  UpdateRoutingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces rules."}
  DeleteRoutingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while a listener uses it."}
  CreateRuleSet: {wire: ok, errors: partial, state: ok, persist: ok, note: "ADD/EXTEND/REMOVE_HTTP_REQUEST_HEADER and RESPONSE variants, ALLOW (SOURCE_IP_ADDRESS), REDIRECT, CONTROL_ACCESS_USING_HTTP_METHODS and HTTP_HEADER (httpLargeHeaderSizeInKB). IP_BASED_MAX_CONNECTIONS, SOURCE_VCN_* conditions and areInvalidCharactersAllowed are rejected."}
  GetRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "Polymorphic items decode into the SDK rule types."}
  ListRuleSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sorted."}
  UpdateRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces items."}
  DeleteRuleSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while a listener uses it."}
  CreateSSLCipherSuite: {wire: ok, errors: partial, state: ok, persist: ok, note: "Custom suites of OpenSSL cipher names that Go's crypto/tls implements; the oci- prefix is reserved."}
  GetSSLCipherSuite: {wire: ok, errors: ok, state: ok, persist: ok, note: "By name."}
  ListSSLCipherSuites: {wire: ok, errors: ok, state: ok, persist: ok, note: "Custom suites only."}
  UpdateSSLCipherSuite: {wire: ok, errors: ok, state: ok, persist: ok, note: "Replaces ciphers."}
  DeleteSSLCipherSuite: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while a listener or backend set uses it."}
  CreateCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "PEM public certificate chain, optional CA bundle, private key checked as a matching pair with crypto/tls; encrypted keys are rejected. The key is held in memory for TLS termination only: never returned, never persisted."}
  ListCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "Names, public certificate and CA bundle."}
  DeleteCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "409 while referenced."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for keys registered via POST /_gopherstack/oci/keys or OCI Identity; --oci-loadbalancer-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "{code,message} with opc-request-id: NotAuthenticated, NotAuthorizedOrNotFound, InvalidParameter, MissingParameter, Conflict, IncorrectState, NoEtagMatch (OCI API error reference). Which of 409 Conflict and IncorrectState Oracle uses per case is from memory."}
  async_work_requests: {status: partial, note: "Every mutation answers 202 with opc-work-request-id and finishes after 2 s on the injectable clock; the change is visible immediately. A second mutation while one is unfinished is 409 Conflict (Oracle documents work-request based asynchronous LB operations; the exact conflict code is from memory)."}
  data_plane: {status: partial, note: "Each active load balancer port gets a loopback listener on an ephemeral host port (127.0.0.1:0), surfaced as gopherstackAddress on listeners in GetLoadBalancer and by GET /_gopherstack/loadbalancer/endpoints. HTTP is proxied with httputil.ReverseProxy (X-Forwarded-*, Host preserved, WebSocket upgrades), TCP by a byte pipe with idle deadlines (idleTimeout, default 60 s HTTP / 300 s TCP). 503 when no backend is usable, 502/504 on backend failure, 404 when no listener on the port matches the host."}
  balancing: {status: partial, note: "ROUND_ROBIN is smooth weighted round robin, LEAST_CONNECTIONS picks the lowest active/weight, IP_HASH hashes the client address over usable backends. Offline and unhealthy backends are skipped, drained ones only serve persistent sessions, backups serve only when no regular backend is usable, maxConnections limits are honoured. Exact Oracle algorithms are from memory."}
  health_checks: {status: partial, note: "HTTP (status, body regex) and TCP checks driven by intervalInMillis on the injectable clock, run before routing and by a 1 s ticker; a backend turns CRITICAL after retries+1 consecutive failures and OK again after retries+1 successes (the exact retries semantics are from memory). New backends are UNKNOWN and routable until checked. Health is runtime state and is not persisted."}
  routing: {status: partial, note: "Order: rule-set request rules, then routing policy (first matching rule) or path route set (EXACT, then longest FORCE_LONGEST_PREFIX, then first PREFIX/SUFFIX in list order; case-insensitive), else the default backend set. Hostname matching: exact, then wildcard, then listeners without hostnames. Precedence details are from memory."}
  rule_sets: {status: partial, note: "Header add appends, extend wraps an existing value, ALLOW rules deny with 403 when none matches the client address, REDIRECT matches any condition and supports {protocol} {host} {port} {path} {query} tokens (default 302), CONTROL_ACCESS answers statusCode (default 405). Semantics of multiple conditions and duplicate headers are from memory."}
  session_persistence: {status: partial, note: "LB cookie persistence sets X-Oracle-BMC-LBS-Route (or cookieName) with path/domain/maxAge/secure/httpOnly; application cookie persistence adds the route cookie when the backend sets cookieName. disableFallback answers 502 when the sticky backend is unusable."}
  tls: {status: partial, note: "Listeners terminate TLS (HTTP, HTTP2 with ALPN h2, TCP) with the uploaded certificate selected by SNI, protocols TLSv1.2/TLSv1.3 (default TLSv1.2 only), custom cipher suites and optional client certificate verification against the bundle CA. Built-in oci-* suite names are accepted as references but their cipher lists are not modelled."}
  backend_tls: {status: partial, note: "Backend sets with sslConfiguration use TLS to backends and for HTTPS health checks (isForcePlainText=false). The peer chain and IP SAN are always verified against the certificateName bundle (CA plus public certificate) or system roots; there is no InsecureSkipVerify, so verifyPeerCertificate=false does NOT disable verification (stricter than Oracle). verifyPeerCertificate=true requires a bundle."}
  ssrf_guard: {status: ok, note: "All backend dials, probes and TLS handshakes use pkgs/httpclient.LoopbackDialer, which checks the resolved peer address and refuses anything outside 127.0.0.0/8 and ::1 unless --oci-loadbalancer-allow-external-backends / OCI_LOADBALANCER_ALLOW_EXTERNAL_BACKENDS is set; proxy environment variables are ignored."}
  lifecycle: {status: ok, note: "CREATING, DELETING last 2 s; Sweep runs on every request and every second; listeners open when ACTIVE and close on DELETING."}
  persistence: {status: partial, note: "Load balancers (without certificate private keys) and work requests are in the snapshot; listeners reopen on restore. TLS listeners cannot terminate after a restore until the certificate is recreated."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-loadbalancer-port (default 8165); never on the shared AWS router; cli.go reserves the port."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards resource state, a second one the data-plane runtime state."}
items_still_open:
  - "Not routed: CancelWorkRequest and tag/lock APIs; the shape catalogue is static. reservedIps, ipv6SubnetCidr and ipMode other than IPV4 are rejected."
  - "Rejected with 400: IP_BASED_MAX_CONNECTIONS, SOURCE_VCN_ID and SOURCE_VCN_IP_ADDRESS conditions, areInvalidCharactersAllowed, backendTcpProxyProtocolVersion/Options, certificateIds and trustedCertificateAuthorityIds (OCI Certificates service), passphrase-protected private keys, TLS protocols below 1.2."
  - "verifyPeerCertificate=false on a backend set does not turn verification off (repo rule: no InsecureSkipVerify); backends with self-signed certificates need their certificate uploaded as the bundle."
  - "Unverified, from memory: flexible shape bounds (10-8000 Mbps), name character rules, retries/threshold semantics, health status aggregation, 409 code for in-flight work requests, listener port sharing by hostname, path route and rule precedence, ALLOW/REDIRECT multiple-condition semantics, header add semantics, HTTP 404 for non-matching virtual hosts, pathRouteSetName and routingPolicyName being mutually exclusive, the built-in cipher suite names, the routing-policy condition grammar subset."
  - "Addresses: public and private load balancers get an address from the emulator pool 198.18.0.0/15 (public) or the OCI Core subnet CIDR when wired; no real address is reachable, the data plane is on loopback ephemeral ports only. In containers those ports are not published; use the endpoints admin route from inside the container."
  - "Health state is not persisted; backend maxConnections and balancing cursors are per process. No PROXY protocol, no SNI-based backend selection, no OCSP or session-ticket handling, no per-listener access logging."
  - "opc-retry-token idempotency, IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified. Private keys of certificates are not persisted, so TLS listeners need the certificate recreated after a restore."
  - "ociapigateway still carries its own copy of the loopback dial guard instead of pkgs/httpclient.LoopbackDialer."
---
