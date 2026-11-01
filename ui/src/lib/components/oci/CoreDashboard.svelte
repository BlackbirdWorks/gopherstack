<script lang="ts">
	import { onMount } from 'svelte';
	import { Network, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError, str } from '#lib/gcprest.ts';
	import {
		allowedActions,
		instanceAction,
		instanceShape,
		listCompartments,
		listInstanceVnics,
		listNsgRules,
		listResources,
		readStoredCompartment,
		routeRules,
		securityListRules,
		shortId,
		writeStoredCompartment,
		type CoreKind,
		type CoreResource,
		type InstanceAction,
		type InstanceVnic,
		type RuleView
	} from '#lib/ocicore.ts';

	type Column = { label: string; get: (r: CoreResource) => string };
	type Section = { kind: CoreKind; title: string; noun: string; columns: Column[] };

	const joined = (v: unknown) => (Array.isArray(v) ? v.map(String).join(', ') : '');
	const field = (name: string): Column['get'] => (r) => str(r.raw[name]);
	const size = (r: CoreResource) => (r.raw.sizeInGBs === undefined ? '' : `${String(r.raw.sizeInGBs)} GB`);
	const count = (name: string): Column['get'] => (r) => String(Array.isArray(r.raw[name]) ? (r.raw[name] as unknown[]).length : 0);

	const SECTIONS: Record<string, Section[]> = {
		network: [
			{ kind: 'vcns', title: 'Virtual cloud networks', noun: 'VCNs', columns: [{ label: 'CIDR blocks', get: (r) => joined(r.raw.cidrBlocks) || str(r.raw.cidrBlock) }, { label: 'DNS label', get: field('dnsLabel') }] },
			{ kind: 'subnets', title: 'Subnets', noun: 'subnets', columns: [{ label: 'CIDR', get: field('cidrBlock') }, { label: 'VCN', get: (r) => shortId(str(r.raw.vcnId)) }, { label: 'Public IPs', get: (r) => (r.raw.prohibitPublicIpOnVnic === true ? 'prohibited' : 'allowed') }] },
			{ kind: 'routeTables', title: 'Route tables', noun: 'route tables', columns: [{ label: 'VCN', get: (r) => shortId(str(r.raw.vcnId)) }, { label: 'Rules', get: count('routeRules') }] },
			{ kind: 'securityLists', title: 'Security lists', noun: 'security lists', columns: [{ label: 'VCN', get: (r) => shortId(str(r.raw.vcnId)) }, { label: 'Ingress', get: count('ingressSecurityRules') }, { label: 'Egress', get: count('egressSecurityRules') }] },
			{ kind: 'networkSecurityGroups', title: 'Network security groups', noun: 'network security groups', columns: [{ label: 'VCN', get: (r) => shortId(str(r.raw.vcnId)) }] }
		],
		compute: [
			{ kind: 'instances', title: 'Instances', noun: 'instances', columns: [{ label: 'Shape', get: instanceShape }, { label: 'Availability domain', get: field('availabilityDomain') }, { label: 'Fault domain', get: field('faultDomain') }] }
		],
		storage: [
			{ kind: 'volumes', title: 'Block volumes', noun: 'block volumes', columns: [{ label: 'Size', get: size }, { label: 'VPUs/GB', get: (r) => String(r.raw.vpusPerGB ?? '') }, { label: 'Availability domain', get: field('availabilityDomain') }] },
			{ kind: 'bootVolumes', title: 'Boot volumes', noun: 'boot volumes', columns: [{ label: 'Size', get: size }, { label: 'Image', get: (r) => shortId(str(r.raw.imageId)) }, { label: 'Availability domain', get: field('availabilityDomain') }] },
			{ kind: 'volumeAttachments', title: 'Volume attachments', noun: 'volume attachments', columns: [{ label: 'Type', get: field('attachmentType') }, { label: 'Instance', get: (r) => shortId(str(r.raw.instanceId)) }, { label: 'Volume', get: (r) => shortId(str(r.raw.volumeId)) }, { label: 'Device', get: field('device') }] },
			{ kind: 'bootVolumeAttachments', title: 'Boot volume attachments', noun: 'boot volume attachments', columns: [{ label: 'Instance', get: (r) => shortId(str(r.raw.instanceId)) }, { label: 'Boot volume', get: (r) => shortId(str(r.raw.bootVolumeId)) }] },
			{ kind: 'volumeBackups', title: 'Volume backups', noun: 'volume backups', columns: [{ label: 'Type', get: field('type') }, { label: 'Source', get: field('sourceType') }, { label: 'Size', get: (r) => (r.raw.sizeInGBs === undefined ? '' : `${String(r.raw.sizeInGBs)} GB`) }, { label: 'Volume', get: (r) => shortId(str(r.raw.volumeId)) }] }
		]
	};

	const TABS = [
		{ id: 'network', label: 'Networking' },
		{ id: 'compute', label: 'Compute' },
		{ id: 'storage', label: 'Block storage' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredCompartment());
	let tab = $state('network');
	let data = $state<Partial<Record<CoreKind, CoreResource[]>>>({});
	let loading = $state(true);
	let error = $state('');
	let expanded = $state('');
	let vnics = $state<InstanceVnic[]>([]);
	let rules = $state<RuleView[]>([]);
	let detailError = $state('');
	let detailLoading = $state(false);

	async function loadTab() {
		if (!compartment) {
			data = {};
			return;
		}
		const sections = SECTIONS[tab] ?? [];
		try {
			const lists = await Promise.all(sections.map((s) => listResources(s.kind, compartment)));
			const next: Partial<Record<CoreKind, CoreResource[]>> = {};
			sections.forEach((s, i) => (next[s.kind] = lists[i] ?? []));
			data = next;
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
			error = '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadTab();
		loading = false;
	}

	function pickCompartment() {
		writeStoredCompartment(compartment);
		expanded = '';
		loading = true;
		void loadTab().then(() => (loading = false));
	}

	function selectTab(id: string) {
		tab = id;
		expanded = '';
		loading = true;
		void loadTab().then(() => (loading = false));
	}

	async function toggle(s: Section, r: CoreResource) {
		if (expanded === r.id) {
			expanded = '';
			return;
		}
		expanded = r.id;
		vnics = [];
		rules = [];
		detailError = '';
		detailLoading = true;
		try {
			if (s.kind === 'instances') vnics = await listInstanceVnics(compartment, r.id);
			else if (s.kind === 'networkSecurityGroups') rules = await listNsgRules(r.id);
			else if (s.kind === 'securityLists') rules = securityListRules(r.raw);
		} catch (e) {
			detailError = describeGcpError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function act(r: CoreResource, action: InstanceAction) {
		const run = () => instanceAction(r.id, action);
		const ok =
			action === 'STOP' || action === 'SOFTRESET'
				? await confirmedAttempt({ title: `${action === 'STOP' ? 'Stop' : 'Reset'} instance`, message: `${action === 'STOP' ? 'Stop' : 'Soft reset'} instance "${r.name}"?`, confirmLabel: action === 'STOP' ? 'Stop' : 'Reset' }, run, `${action} requested for ${r.name}`, `Failed to ${action.toLowerCase()} ${r.name}`)
				: await attempt(run, `${action} requested for ${r.name}`, `Failed to ${action.toLowerCase()} ${r.name}`);
		if (ok) {
			await loadTab();
			for (const ms of [1500, 4000]) setTimeout(() => void loadTab(), ms);
		}
	}

	const actionLabel: Record<InstanceAction, string> = { START: 'Start', STOP: 'Stop', SOFTRESET: 'Reset' };
	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'AVAILABLE' || s === 'RUNNING' || s === 'ATTACHED' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'TERMINATED' || s === 'FAULTY' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="ocicore-dashboard">
	<PageHeader icon={Network} title="Core Services" description="Oracle Cloud networking, compute instances and block storage" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="ocicore-error">
			<p class="font-medium">Could not load Core Services</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if compartments.length === 0 && !loading}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="ocicore-empty">No Core Services resources yet. Create a VCN with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="ocicore-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>

		<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="red" />
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="ocicore-tab-error">{error}</p>{/if}

		{#if loading}
			<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
		{:else}
			{#each SECTIONS[tab] ?? [] as s (s.kind)}
				{@const items = data[s.kind] ?? []}
				<section class="space-y-2" data-testid="ocicore-section-{s.kind}">
					<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">{s.title} <span class="font-normal text-gray-400">({items.length})</span></h2>
					{#if items.length === 0}
						<p class="py-3 text-sm text-gray-500 dark:text-gray-400" data-testid="ocicore-{s.kind}-empty">No {s.noun} in this compartment</p>
					{:else}
						<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
							<table class="w-full text-sm" data-testid="ocicore-{s.kind}">
								<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
									<tr><th class={cell}>Name</th><th class={cell}>State</th>{#each s.columns as c (c.label)}<th class={cell}>{c.label}</th>{/each}<th class="{cell} text-right">Actions</th></tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each items as r (r.id)}
										<tr data-testid="ocicore-{s.kind}-row">
											<td class="{cell} font-mono text-xs break-all" title={r.id}>{r.name}</td>
											<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.state)}" data-testid="ocicore-{s.kind}-state">{r.state || '—'}</span></td>
											{#each s.columns as c (c.label)}<td class="{cell} text-xs break-all">{c.get(r) || '—'}</td>{/each}
											<td class="{cell} text-right whitespace-nowrap space-x-1">
												{#if s.kind === 'instances'}
													{#each allowedActions(r.state) as a (a)}
														<button class={GCP_MINI} onclick={() => act(r, a)} data-testid="ocicore-instance-{a.toLowerCase()}">{actionLabel[a]}</button>
													{/each}
												{/if}
												<button class={GCP_MINI} onclick={() => toggle(s, r)} aria-expanded={expanded === r.id} data-testid="ocicore-{s.kind}-details">Details</button>
											</td>
										</tr>
										{#if expanded === r.id}
											<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="ocicore-detail">
												<td colspan={s.columns.length + 3} class="space-y-3 px-4 py-3 text-xs">
													{#if detailError}<p class="text-red-600" role="alert">{detailError}</p>{/if}
													{#if detailLoading}
														<RefreshCw class="h-4 w-4 animate-spin text-gray-400" />
													{:else if s.kind === 'instances'}
														<p class="font-medium">VNICs</p>
														{#if vnics.length === 0}
															<p class="text-gray-500" data-testid="ocicore-vnics-empty">No VNIC attachments</p>
														{:else}
															<table class="w-full" data-testid="ocicore-vnics">
																<thead class="text-left text-gray-500"><tr><th class="py-1 pr-4">VNIC</th><th class="py-1 pr-4">State</th><th class="py-1 pr-4">Private IP</th><th class="py-1 pr-4">Public IP</th><th class="py-1 pr-4">MAC</th><th class="py-1">Primary</th></tr></thead>
																<tbody>
																	{#each vnics as v (v.attachmentId)}
																		<tr data-testid="ocicore-vnic"><td class="py-1 pr-4 font-mono">{v.name || shortId(v.vnicId)}</td><td class="py-1 pr-4">{v.state}</td><td class="py-1 pr-4 font-mono" data-testid="ocicore-vnic-private">{v.privateIp || '—'}</td><td class="py-1 pr-4 font-mono">{v.publicIp || '—'}</td><td class="py-1 pr-4 font-mono">{v.mac || '—'}</td><td class="py-1">{v.isPrimary ? 'yes' : 'no'}</td></tr>
																	{/each}
																</tbody>
															</table>
														{/if}
													{:else if s.kind === 'routeTables'}
														<p class="font-medium">Route rules</p>
														<ul class="font-mono" data-testid="ocicore-route-rules">
															{#each routeRules(r.raw) as rr, i (i)}<li>{rr.destination} → {shortId(rr.target)}</li>{:else}<li class="text-gray-500">No rules</li>{/each}
														</ul>
													{:else if s.kind === 'securityLists' || s.kind === 'networkSecurityGroups'}
														<p class="font-medium">Security rules</p>
														{#if rules.length === 0}
															<p class="text-gray-500" data-testid="ocicore-rules-empty">No rules</p>
														{:else}
															<table class="w-full" data-testid="ocicore-rules">
																<thead class="text-left text-gray-500"><tr><th class="py-1 pr-4">Direction</th><th class="py-1 pr-4">Protocol</th><th class="py-1 pr-4">Source / destination</th><th class="py-1 pr-4">Ports</th><th class="py-1">Stateless</th></tr></thead>
																<tbody>
																	{#each rules as rule, i (i)}
																		<tr data-testid="ocicore-rule"><td class="py-1 pr-4">{rule.direction}</td><td class="py-1 pr-4">{rule.protocol}</td><td class="py-1 pr-4 font-mono">{rule.peer}</td><td class="py-1 pr-4">{rule.ports}</td><td class="py-1">{rule.stateless ? 'yes' : 'no'}</td></tr>
																	{/each}
																</tbody>
															</table>
														{/if}
													{/if}
													<pre class="max-h-72 overflow-auto whitespace-pre-wrap break-all rounded-md bg-white p-2 font-mono dark:bg-slate-900">{JSON.stringify(r.raw, null, 2)}</pre>
												</td>
											</tr>
										{/if}
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				</section>
			{/each}
		{/if}
	{/if}
</div>
