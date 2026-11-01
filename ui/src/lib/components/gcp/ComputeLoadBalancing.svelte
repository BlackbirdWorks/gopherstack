<script lang="ts">
	import { untrack } from 'svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable from '#lib/components/gcp/GcpResourceTable.svelte';
	import { inRegionScope, listBackendServices, listForwardingRules, listTargetProxies, listUrlMaps, type BackendService, type ForwardingRule, type TargetProxy, type UrlMap } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; region: string; refresh: number };
	let { project, region, refresh }: Props = $props();

	const TABS = [
		{ id: 'backends', label: 'Backend services' },
		{ id: 'urlmaps', label: 'URL maps' },
		{ id: 'proxies', label: 'Target proxies' },
		{ id: 'rules', label: 'Forwarding rules' }
	];
	let tab = $state('backends');
	let loading = $state(true);
	let error = $state('');
	let backends = $state<BackendService[]>([]);
	let urlMaps = $state<UrlMap[]>([]);
	let proxies = $state<TargetProxy[]>([]);
	let rules = $state<ForwardingRule[]>([]);

	async function load() {
		loading = true;
		try {
			if (tab === 'backends') backends = await listBackendServices(project);
			else if (tab === 'urlmaps') urlMaps = await listUrlMaps(project);
			else if (tab === 'proxies') proxies = await listTargetProxies(project);
			else rules = await listForwardingRules(project);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void project;
		void refresh;
		untrack(() => void load());
	});

	function selectTab(id: string) {
		tab = id;
		void load();
	}

	const backendRows = $derived(
		backends.filter((b) => inRegionScope(b.scope, region)).map((b) => ({ id: `${b.scope}/${b.name}`, cells: [b.name, b.scope, b.scheme, b.protocol, b.backends.join(', '), b.healthChecks.join(', '), [b.securityPolicy, b.edgeSecurityPolicy && `${b.edgeSecurityPolicy} (edge)`].filter(Boolean).join(', ')], raw: b.raw }))
	);
	const urlMapRows = $derived(
		urlMaps.filter((u) => inRegionScope(u.scope, region)).map((u) => ({ id: `${u.scope}/${u.name}`, cells: [u.name, u.scope, u.defaultService, u.hostRules.join('; '), u.pathMatchers.join(', ')], raw: u.raw }))
	);
	const proxyRows = $derived(
		proxies.filter((p) => inRegionScope(p.scope, region)).map((p) => ({ id: `${p.kind}/${p.scope}/${p.name}`, cells: [p.name, p.scope, p.kind, p.urlMap, p.certificates.join(', ')], raw: p.raw }))
	);
	const ruleRows = $derived(
		rules.filter((r) => inRegionScope(r.scope, region)).map((r) => ({ id: `${r.scope}/${r.name}`, cells: [r.name, r.scope, r.ip, r.protocol, r.ports, r.target, r.scheme], raw: r.raw }))
	);
</script>

<div class="space-y-4">
	<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
	{#if tab === 'backends'}
		<GcpResourceTable testId="compute-backend" noun="backend services" columns={['Name', 'Scope', 'Scheme', 'Protocol', 'Backends', 'Health checks', 'Cloud Armor policy']} rows={backendRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'urlmaps'}
		<GcpResourceTable testId="compute-urlmap" noun="URL maps" columns={['Name', 'Scope', 'Default service', 'Host rules', 'Path matchers']} rows={urlMapRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'proxies'}
		<GcpResourceTable testId="compute-proxy" noun="target proxies" columns={['Name', 'Scope', 'Type', 'URL map', 'Certificates']} rows={proxyRows} {loading} {error} onRetry={() => load()} />
	{:else}
		<GcpResourceTable testId="compute-rule" noun="forwarding rules" columns={['Name', 'Scope', 'IP address', 'Protocol', 'Ports', 'Target', 'Scheme']} rows={ruleRows} {loading} {error} onRetry={() => load()} />
	{/if}
</div>
