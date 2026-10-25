<script lang="ts">
	import { onMount } from 'svelte';
	import { Waypoints } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt } from '#lib/gcp-action.ts';
	import {
		deleteChannel,
		deleteTrigger,
		listChannels,
		listProviders,
		listTriggers,
		readStoredEventarcLocation,
		readStoredEventarcProject,
		writeStoredEventarcScope,
		type ChannelInfo,
		type ProviderInfo,
		type TriggerInfo
	} from '#lib/gcpeventarc.ts';

	const TABS = [
		{ id: 'triggers', label: 'Triggers' },
		{ id: 'channels', label: 'Channels' },
		{ id: 'providers', label: 'Providers' }
	];

	let project = $state(readStoredEventarcProject());
	let location = $state(readStoredEventarcLocation());
	let tab = $state('triggers');
	let triggers = $state<TriggerInfo[]>([]);
	let channels = $state<ChannelInfo[]>([]);
	let providers = $state<ProviderInfo[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		const scope = { project, location };
		try {
			[triggers, channels, providers] = await Promise.all([listTriggers(scope), listChannels(scope), listProviders(scope)]);
			triggers = triggers.toSorted((a, b) => a.id.localeCompare(b.id));
			channels = channels.toSorted((a, b) => a.id.localeCompare(b.id));
			providers = providers.toSorted((a, b) => a.id.localeCompare(b.id));
			error = '';
		} catch (e) {
			error = e instanceof Error ? e.message : String(e);
		} finally {
			loading = false;
		}
	}

	function applyScope(p: string, l: string) {
		project = p;
		location = l;
		writeStoredEventarcScope(p, l);
		loading = true;
		void load();
	}

	async function remove(kind: 'trigger' | 'channel', row: ResourceRow) {
		if (!(await confirmDestructive({ title: `Delete ${kind}`, message: `Delete ${kind} "${row.cells[0]}"?` }))) return;
		const fn = kind === 'trigger' ? deleteTrigger : deleteChannel;
		if (await attempt(() => fn(row.id), `Deleted ${kind} ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`)) await load();
	}

	const triggerRows = $derived(
		triggers.map((t) => ({
			id: t.name,
			cells: [t.id, t.eventFilters.map((f) => `${f.attribute}${f.operator === '=' ? '=' : ` ${f.operator} `}${f.value}`).join(', '), `${t.destinationKind}: ${t.destination}`, t.transportTopic],
			raw: t.raw
		}))
	);
	const channelRows = $derived(channels.map((c) => ({ id: c.name, cells: [c.id, c.state, c.provider, c.pubsubTopic], raw: c.raw })));
	const providerRows = $derived(providers.map((p) => ({ id: p.name, cells: [p.id, p.displayName, `${p.eventTypes.length} event types`], raw: p.raw })));

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="eventarc-dashboard">
	<PageHeader icon={Waypoints} title="Eventarc" description="Google Cloud Eventarc: triggers, channels and event providers" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="eventarc" {project} {location} hint="Resources are listed per project and location." onApply={applyScope} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'triggers'}
		<GcpResourceTable testId="eventarc-trigger" noun="triggers" columns={['Trigger', 'Event filters', 'Destination', 'Transport topic']} rows={triggerRows} {loading} {error} onRetry={load} onDelete={(r) => remove('trigger', r)}>
			{#snippet detail(row)}
				{@const t = triggers.find((x) => x.name === row.id)}
				{#if t}
					<p><span class="font-medium text-gray-500 dark:text-gray-400">Service account</span> {t.serviceAccount || '—'} · <span class="font-medium text-gray-500 dark:text-gray-400">Channel</span> {t.channel || '—'} · <span class="font-medium text-gray-500 dark:text-gray-400">Content type</span> {t.eventDataContentType || '—'}</p>
				{/if}
			{/snippet}
		</GcpResourceTable>
	{:else if tab === 'channels'}
		<GcpResourceTable testId="eventarc-channel" noun="channels" columns={['Channel', 'State', 'Provider', 'Pub/Sub topic']} rows={channelRows} {loading} {error} onRetry={load} onDelete={(r) => remove('channel', r)} />
	{:else}
		<GcpResourceTable testId="eventarc-provider" noun="providers" columns={['Provider', 'Display name', 'Event types']} rows={providerRows} {loading} {error} onRetry={load}>
			{#snippet detail(row)}
				{@const p = providers.find((x) => x.name === row.id)}
				{#if p}<p class="font-mono break-all">{p.eventTypes.join(', ') || 'No event types'}</p>{/if}
			{/snippet}
		</GcpResourceTable>
	{/if}
</div>
