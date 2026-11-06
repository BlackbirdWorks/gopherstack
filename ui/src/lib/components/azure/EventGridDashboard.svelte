<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Zap, Send, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import RevealKeys from '#lib/components/azure/RevealKeys.svelte';
	import { attempt, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		deliverySummary,
		filterSummary,
		getOverview,
		publishTestEvent,
		revealKeys,
		validateTestEvent,
		type EgKind,
		type EgOverview,
		type EgSubscription,
		type PublishTarget
	} from '#lib/azureeventgrid.ts';

	type Row = { id: string; kind: EgKind; label: string; name: string; schema: string; detail: string; subs: EgSubscription[]; target?: PublishTarget; ns?: string; needsTopic?: boolean };

	let overview = $state<EgOverview>({ topics: [], domains: [], namespaces: [], scopes: [] });
	let selectedId = $state('');
	let loading = $state(true);
	let loadError = $state('');
	let publishing = $state(false);
	let fSubject = $state('/gopherstack/dashboard/test');
	let fType = $state('Gopherstack.Test');
	let fData = $state('{"message":"hello from the dashboard"}');
	let fDomainTopic = $state('');

	const rows = $derived.by<Row[]>(() => {
		const out: Row[] = [];
		for (const t of overview.topics) {
			out.push({ id: `t:${t.name}`, kind: 'topics', label: 'Custom topic', name: t.name, schema: t.inputSchema, detail: '', subs: t.subscriptions, target: { kind: 'topic', topic: t.name } });
		}
		for (const d of overview.domains) {
			out.push({ id: `d:${d.name}`, kind: 'domains', label: 'Domain', name: d.name, schema: d.inputSchema, detail: `${d.topics.length} domain topics, auto-create ${d.autoCreateTopics ? 'on' : 'off'}, auto-delete ${d.autoDeleteTopics ? 'on' : 'off'}`, subs: d.subscriptions, target: { kind: 'domain', domain: d.name }, needsTopic: true });
			for (const t of d.topics) {
				out.push({ id: `dt:${d.name}/${t.name}`, kind: 'domains', label: 'Domain topic', name: `${d.name}/${t.name}`, schema: d.inputSchema, detail: '', subs: t.subscriptions, target: { kind: 'domain', domain: d.name } });
			}
		}
		for (const n of overview.namespaces) {
			for (const t of n.topics) {
				out.push({ id: `nt:${n.name}/${t.name}`, kind: 'namespaces', label: 'Namespace topic', name: `${n.name}/${t.name}`, schema: 'CloudEventSchemaV1_0', detail: 'pull delivery', subs: t.subscriptions, target: { kind: 'namespace', namespace: n.name, topic: t.name }, ns: n.name });
			}
		}
		for (const s of overview.scopes) {
			out.push({ id: `s:${s.id}`, kind: 'topics', label: 'Resource scope', name: s.id, schema: 'EventGridSchema', detail: 'system events', subs: s.subscriptions });
		}
		return out;
	});

	const selected = $derived(rows.find((r) => r.id === selectedId));
	const keyOwner = $derived(selected && (selected.label === 'Custom topic' || selected.label === 'Domain') ? selected.name : selected?.ns);
	const keyKind = $derived<EgKind | ''>(selected?.label === 'Namespace topic' ? 'namespaces' : selected?.label === 'Custom topic' ? 'topics' : selected?.label === 'Domain' ? 'domains' : '');
	const needsDomainTopic = $derived(selected?.target?.kind === 'domain' && selected.label === 'Domain');
	const eventError = $derived(validateTestEvent({ subject: fSubject, eventType: fType, data: fData, domainTopic: fDomainTopic }, needsDomainTopic));

	async function load() {
		loading = true;
		loadError = '';
		try {
			overview = await getOverview();
			if (!rows.some((r) => r.id === selectedId)) selectedId = rows[0]?.id ?? '';
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Event Grid: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function publish(e: Event) {
		e.preventDefault();
		const target = selected?.target;
		if (eventError || !target) return;
		const sel = selected;
		let dt = fDomainTopic;
		if (sel?.label === 'Domain topic') dt = sel.name.split('/')[1] ?? '';
		publishing = true;
		let n = 0;
		const ok = await attempt(async () => {
			n = await publishTestEvent(target, { subject: fSubject, eventType: fType, data: fData, domainTopic: dt });
		}, '', 'Failed to publish');
		publishing = false;
		if (ok) toast.success(`Published ${n} event${n === 1 ? '' : 's'} to ${sel?.name}`);
	}

	async function fetchKeys() {
		const k = await revealKeys(keyKind as EgKind, keyOwner ?? '');
		return [{ label: 'Key 1', value: k.key1 }, { label: 'Key 2', value: k.key2 }];
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="eventgrid-dashboard">
	<PageHeader icon={Zap} title="Event Grid" description="Azure Event Grid: topics, domains, namespaces, event subscriptions and test events" color="purple" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="eventgrid-error">
			<p class="font-medium">Could not reach Azure Event Grid</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if rows.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="eventgrid-empty">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No topics, domains or namespaces. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="eventgrid-topics">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Resource</th><th class="px-4 py-2">Kind</th><th class="px-4 py-2">Input schema</th><th class="px-4 py-2">Subscriptions</th></tr></thead>
				<tbody>
					{#each rows as r (r.id)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {r.id === selectedId ? 'bg-blue-50 dark:bg-slate-800' : ''}" data-testid="eventgrid-row">
							<td class="px-4 py-2"><button class="break-all text-left font-medium text-blue-600 underline dark:text-blue-400" onclick={() => (selectedId = r.id)}>{r.name}</button></td>
							<td class="px-4 py-2">{r.label}</td>
							<td class="px-4 py-2">{r.schema}</td>
							<td class="px-4 py-2">{r.subs.length}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if selected}
			{#if selected.detail}<p class="text-sm text-gray-500 dark:text-gray-400">{selected.detail}</p>{/if}

			{#key selected.id}
				{#if keyKind && keyOwner}
					<section class="space-y-1" data-testid="eventgrid-keys">
						<h2 class="font-semibold">Access keys of {keyOwner}</h2>
						<RevealKeys {fetchKeys} testid="eventgrid-key" />
					</section>
				{/if}
			{/key}

			<section>
				<h2 class="mb-2 font-semibold">Event subscriptions of {selected.name}</h2>
				{#if selected.subs.length === 0}
					<p class="text-gray-500" data-testid="eventgrid-no-subs">No event subscriptions.</p>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full text-sm" data-testid="eventgrid-subs">
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Endpoint</th><th class="px-4 py-2">State</th><th class="px-4 py-2">Delivery schema</th><th class="px-4 py-2">Filters</th><th class="px-4 py-2">Retry and dead-letter</th></tr></thead>
							<tbody>
								{#each selected.subs as s (s.name)}
									<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2">{s.name}</td><td class="px-4 py-2">{s.endpointType}</td><td class="px-4 py-2">{s.state || '-'}</td><td class="px-4 py-2">{s.deliverySchema || '-'}</td><td class="px-4 py-2">{filterSummary(s)}</td><td class="px-4 py-2">{deliverySummary(s)}</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</section>

			{#if selected.target}
				<form class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={publish} data-testid="eventgrid-publish-form">
					<h2 class="font-semibold">Publish test event to {selected.name}</h2>
					<p class="text-xs text-gray-500">The event is built by the server in the resource's input schema ({selected.schema}).</p>
					<div class="grid gap-3 sm:grid-cols-2">
						<label class="text-sm">Event type<input class={GCP_INPUT} bind:value={fType} data-testid="eventgrid-type" /></label>
						<label class="text-sm">Subject<input class={GCP_INPUT} bind:value={fSubject} data-testid="eventgrid-subject" /></label>
					</div>
					{#if needsDomainTopic}
						<label class="block text-sm">Domain topic<input class={GCP_INPUT} bind:value={fDomainTopic} placeholder="orders" data-testid="eventgrid-domain-topic" /></label>
					{/if}
					<label class="block text-sm">Data (text or JSON)<textarea class="{GCP_INPUT} font-mono" rows="4" bind:value={fData} data-testid="eventgrid-json"></textarea></label>
					{#if eventError}<p class="text-sm text-red-600" data-testid="eventgrid-json-error">{eventError}</p>{/if}
					<button type="submit" class={GCP_PRIMARY} disabled={publishing || Boolean(eventError)} data-testid="eventgrid-publish"><Send class="mr-1 inline h-4 w-4" />Publish</button>
				</form>
			{/if}
		{/if}
	{/if}
</div>
