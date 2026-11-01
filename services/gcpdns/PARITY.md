---
service: gcpdns
sdk_module: google.golang.org/api@v0.304.0
last_audit_commit: 423518f77
last_audit_date: 2026-10-25
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ManagedZones.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "Returns the ManagedZone (v1 does not return an operation). name must match [a-z]([-a-z0-9]*[a-z0-9])? up to 63 chars, dnsName a lower-cased FQDN with trailing dot, description is required. Public zones get a random int64 id, four nameServers (ns-cloud-{a-e}{1-4}.googledomains.com.) and visibility public; private zones keep privateVisibilityConfig networks/gkeClusters and have no nameServers. forwardingConfig and peeringConfig are validated (private only, mutually exclusive, targets need an address) and stored. Every zone starts with change 0 holding the default SOA (serial 1, 21600 3600 259200 300, hostmaster cloud-dns-hostmaster.google.com.) and NS records (TTL 21600; private zones use ns-gcp-private.googledomains.com.). 409 alreadyExists on a duplicate name. The wording of validation messages was chosen without a production reference."}
  ManagedZones.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "404 notFound \"The 'parameters.managedZone' resource named 'x' does not exist.\""}
  ManagedZones.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ordered by name; paged through pkgs/page (default 100); dnsName filter; an empty list omits managedZones."}
  ManagedZones.Delete: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; a zone holding anything besides its apex SOA and NS is 400 containerNotEmpty (message text unverified)."}
  ManagedZones.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "JSON merge into the stored zone; returns a done Operation (type update) with zoneContext old/new values, readable through managedZoneOperations. name, dnsName and visibility are immutable (400 invalid). Changing dnssecConfig.state from off to on/transfer generates the DNSKEYs, back to off drops them; nonExistence and defaultKeySpecs only change while off."}
  ManagedZones.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT: replaces the mutable fields (an omitted dnssecConfig turns DNSSEC off); same Operation result and immutability rules as Patch."}
  ManagedZoneOperations.Get: {wire: ok, errors: ok, state: ok, persist: ok, note: "Operations exist only for zone patch/update."}
  ManagedZoneOperations.List: {wire: ok, errors: ok, state: ok, persist: ok, note: "sortBy startTime (creation order) or id."}
  DnsKeys.Get: {wire: ok, errors: ok, state: ok, persist: ok, note: "Keys are really generated (RSA or ECDSA) when DNSSEC is switched on: one keySigning and one zoneSigning key by default (rsasha256 2048/1024); rsasha1, rsasha256, rsasha512, ecdsap256sha256 and ecdsap384sha384 are accepted. publicKey is the base64 DNSKEY key material, keyTag is computed from the DNSKEY record and KSKs carry DS digests (sha256 by default; digestType accepts sha1, sha256, sha384). Private keys are discarded."}
  DnsKeys.List: {wire: ok, errors: ok, state: ok, persist: ok, note: "Empty while DNSSEC is off; paged."}
  Changes.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "Atomic: every deletion must match an existing record set exactly (404 notFound when absent, 412 conditionNotMet when ttl or rrdata differ), additions may not exist (409), names must lie inside the zone, rrdata is parsed per type (A, AAAA, CNAME, MX, TXT/SPF quoted, NS, PTR, SRV, CAA, SOA and every other type the DNS library knows), CNAME must be alone at its name and not at the apex, apex SOA/NS can not be removed, a replaced SOA needs a higher serial, and the SOA serial is incremented automatically (the superseded and new SOA appear in the change). At most 1000 additions/deletions and 100000 bytes of rrdata (403 quotaExceeded). Returned pending, reads as done from the next Get/List; the zone content applies immediately."}
  Changes.Get: {wire: ok, errors: ok, state: ok, persist: ok, note: "Change 0 is the zone's initial change."}
  Changes.List: {wire: ok, errors: ok, state: ok, persist: ok, note: "sortBy changeSequence, sortOrder ascending or descending; paged."}
  ResourceRecordSets.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "Implemented as a one-addition change (so it appears in the change history and bumps the SOA serial); names are canonicalised to lower case with a trailing dot, types to upper case, ttl defaults to 300. routingPolicy (wrr, geo, primaryBackup) is validated and stored; rrdatas and routingPolicy are mutually exclusive."}
  ResourceRecordSets.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "name and type match case-insensitively."}
  ResourceRecordSets.List: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Canonical DNS order (reversed labels, then type); name and type filters (type without name is 400); paged. The filter parameter is rejected with 400."}
  ResourceRecordSets.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "Merges into the stored record set and applies it as a delete-and-add change with the usual validation."}
  ResourceRecordSets.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {} (ResourceRecordSetsDeleteResponse); a delete-only change."}
  Policies.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name pattern as zones, int64 id, networks (a network may be attached to one policy only), alternativeNameServerConfig targets, dns64Config, enableInboundForwarding and enableLogging are stored."}
  Policies.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  Policies.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged."}
  Policies.Delete: {wire: ok, errors: partial, state: ok, persist: ok, note: "400 containerNotEmpty while networks are attached (message text unverified)."}
  Policies.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {policy}; JSON merge."}
  Policies.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {policy}; replaces the policy."}
  ResponsePolicies.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "Stores description, labels, networks and gkeClusters. Response policies are stored but not applied by the data plane."}
  ResponsePolicies.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ResponsePolicies.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged."}
  ResponsePolicies.Delete: {wire: ok, errors: partial, state: ok, persist: ok, note: "400 containerNotEmpty while rules or networks exist."}
  ResponsePolicies.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {responsePolicy}."}
  ResponsePolicies.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {responsePolicy}."}
  ResponsePolicyRules.Create: {wire: ok, errors: partial, state: ok, persist: ok, note: "dnsName FQDN; exactly one of behavior bypassResponsePolicy or localData (record sets validated per type)."}
  ResponsePolicyRules.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ResponsePolicyRules.List: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ordered by ruleName; paged."}
  ResponsePolicyRules.Delete: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ResponsePolicyRules.Patch: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {responsePolicyRule}."}
  ResponsePolicyRules.Update: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns {responsePolicyRule}."}
  Projects.Get: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Quota block with the documented fixed limits (1000 additions/deletions per change, 100000 rrdata bytes per change, 8 name servers per delegation, 50 forwarding targets, 100 routing policy items, 10000 rules per response policy) and the default 10000 managed zones / record sets per zone; the other quota values are assumptions. The project number is derived from the project id, not a real number."}
families:
  data_plane: {status: partial, note: "Public zones are served over real DNS on UDP and TCP (--gcp-dns-dns-port, default 8141) through pkgs/dns with an interceptor: longest-suffix zone selection, AA answers for every record type (rrdata is parsed into wire records), CNAME chasing inside the zone, wildcards per RFC 4592 (only when the name does not exist), empty non-terminals, NXDOMAIN and NODATA with the SOA in authority (TTL capped at the SOA minimum), non-authoritative referrals with glue at NS delegations, additional A/AAAA for in-zone NS, MX and SRV targets, ANY, DNSKEY at the apex when DNSSEC is on, REFUSED for names outside any zone, AXFR/IXFR and non-IN classes, NOTIMP for other opcodes, EDNS0 echo and UDP truncation (512 bytes or the EDNS size). Changes are visible at once."}
  routing_policy_serving: {status: partial, note: "Record sets with a routingPolicy are answered with the union of all item rrdatas; weights, geo locations, health checks and primary/backup failover are not evaluated."}
  dnssec_signing: {status: gap, note: "DNSKEYs and DS digests are real, but responses are not signed: no RRSIG, NSEC or NSEC3 records are produced, the DO bit is echoed only."}
  private_and_forwarding_zones: {status: partial, note: "Private, forwarding and peering zones are stored with their configuration; their records are not served (the data plane answers public zones only) and forwarding targets are never queried."}
  persistence: {status: ok, note: "Zones with record sets, changes, operations and DNSKEYs, policies, response policies and rules are in backendSnapshot; DNSKEY private keys are not stored."}
  routing_isolation: {status: ok, note: "Own REST listener bound in StartWorker to --gcp-dns-port / GCP_DNS_PORT (default 8140) and a DNS server on --gcp-dns-dns-port / GCP_DNS_SERVER_PORT (default 8141, UDP and TCP); both fail fast if taken and are reserved in the shared PortAlloc pool; never registered on the shared AWS router. Only /dns/v1 is served."}
  auth: {status: partial, note: "No request is authenticated or authorised."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; requests go through telemetry. No dashboard page."}
items_still_open:
  - "managedZones.getIamPolicy, setIamPolicy and testIamPermissions are not routed (404); dns/v1beta2 and dns/v2 are not served."
  - "Responses are never DNSSEC-signed (no RRSIG/NSEC/NSEC3); DNSKEY private keys are discarded and DNSSEC state transfer is stored only."
  - "Response policies and rules, server policies, private zones, forwarding and peering are stored but have no effect on resolution; routing policies are answered as the union of their items."
  - "Error message texts (validation, containerNotEmpty, quota) and a few conditions (rrsets.* calls appearing in the change history, change completion timing, project number, non-fixed quota values) were chosen without a production reference."
  - "The rrsets list filter parameter, zone cloudLoggingConfig effects (no query logs), reverse-lookup and Service Directory zones are not implemented beyond storing their configuration."
leaks: {status: clean, note: "Background resources are the REST http.Server and the pkgs/dns UDP and TCP servers, both stopped and joined by Shutdown; tests run under testleak."}
---

# GCPDNS parity notes

Cloud DNS (`dns.googleapis.com/dns/v1`) is checked against `google.golang.org/api/dns/v1`: every request and response in the tests goes through that client, and the DNS data plane is checked with a real DNS client over UDP and TCP.
