<script lang="ts">
	// Alarms -- family W's alarm ops (PutAlarm, DeleteAlarm, GetAlarms,
	// TestAlarm). Contact methods (the sibling half of family W) get their
	// own tab, ContactMethodsPanel.svelte, since they're an independent
	// resource keyed by protocol rather than name.
	import {
		GetAlarmsCommand,
		PutAlarmCommand,
		DeleteAlarmCommand,
		TestAlarmCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Alarm,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let alarms = $state<Alarm[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchAlarms(reset: boolean): Promise<void> {
		const resp = await client().send(new GetAlarmsCommand({ pageToken: reset ? undefined : nextToken }));
		alarms = reset ? (resp.alarms ?? []) : [...alarms, ...(resp.alarms ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchAlarms(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchAlarms(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		alarms.filter((a) => (a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createResourceName = $state('');
	let createThreshold = $state(80);
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createResourceName = '';
		createThreshold = 80;
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new PutAlarmCommand({
					alarmName: createName,
					metricName: 'CPUUtilization',
					monitoredResourceName: createResourceName,
					comparisonOperator: 'GreaterThanThreshold',
					threshold: createThreshold,
					evaluationPeriods: 1
				})
			);
			toast.success('Alarm created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteAlarm(a: Alarm): Promise<void> {
		if (!a.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete alarm',
			message: `Delete alarm ${a.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAlarmCommand({ alarmName: a.name }));
			toast.success('Alarm deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function testAlarm(a: Alarm, state: 'OK' | 'ALARM' | 'INSUFFICIENT_DATA'): Promise<void> {
		if (!a.name) return;
		try {
			await client().send(new TestAlarmCommand({ alarmName: a.name, state }));
			toast.success(`Alarm test transitioned to ${state}`);
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Alarm | null>(null);

	function openDetail(a: Alarm): void {
		viewed = a;
		detailModal?.open();
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.name, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.name, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}

	function stateClass(state: string | undefined): string {
		if (state === 'OK') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'ALARM') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create alarm
	</button>
</div>

{#snippet monitorsCell(a: Alarm)}
	{a.monitoredResourceInfo?.name ?? '—'}
{/snippet}
{#snippet stateCell(a: Alarm)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(a.state)}">{a.state ?? '—'}</span>
{/snippet}
{#snippet createdCell(a: Alarm)}
	{formatDate(a.createdAt)}
{/snippet}
{#snippet rowActions(a: Alarm)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => testAlarm(a, 'ALARM')} class="text-red-600 hover:underline text-sm">Test ALARM</button>
		<button onclick={() => testAlarm(a, 'OK')} class="text-green-600 hover:underline text-sm">Test OK</button>
		<button onclick={() => openDetail(a)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteAlarm(a)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(a) => a.name ?? ''}
	columns={defineColumns<Alarm>([
		{ key: 'name', label: 'Name' },
		{ key: 'monitoredResourceInfo', label: 'Monitors', render: monitorsCell },
		{ key: 'state', label: 'State', render: stateCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No alarms found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create alarm">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-alarm-name">
				Name
				<input id="ls-alarm-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-alarm-resource">
				Monitored resource name
				<input id="ls-alarm-resource" bind:value={createResourceName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-alarm-threshold">
				CPU utilization threshold (%)
				<input id="ls-alarm-threshold" type="number" bind:value={createThreshold} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createResourceName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Alarm {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Monitors</dt><dd>{viewed.monitoredResourceInfo?.name ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Metric</dt><dd>{viewed.metricName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Comparison</dt><dd>{viewed.comparisonOperator ?? '—'} {viewed.threshold ?? ''}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={tagsToRecord(viewed.tags)} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
