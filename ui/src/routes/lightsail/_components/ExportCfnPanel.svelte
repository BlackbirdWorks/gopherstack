<script lang="ts">
	// Export & CloudFormation -- family K (4 ops). A real Lightsail-to-EC2/
	// CloudFormation migration path: ExportSnapshot converts an instance or
	// disk snapshot into an EC2-compatible export record; CreateCloudFormationStack
	// then launches EC2 instances from one or more exported snapshots via a
	// real CloudFormation stack (when this backend's CloudFormation hookup is
	// wired -- see services/lightsail/store.go's CloudFormationBackend).
	import {
		GetExportSnapshotRecordsCommand,
		ExportSnapshotCommand,
		GetCloudFormationStackRecordsCommand,
		CreateCloudFormationStackCommand,
		type ExportSnapshotRecord,
		type CloudFormationStackRecord,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let exportRecords = $state<ExportSnapshotRecord[]>([]);
	let stackRecords = $state<CloudFormationStackRecord[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			const [exp, stacks] = await Promise.all([
				client().send(new GetExportSnapshotRecordsCommand({})),
				client().send(new GetCloudFormationStackRecordsCommand({}))
			]);
			exportRecords = exp.exportSnapshotRecords ?? [];
			stackRecords = stacks.cloudFormationStackRecords ?? [];
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void refresh());

	const filteredExports = $derived(
		exportRecords.filter((r) => (r.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredStacks = $derived(
		stackRecords.filter((r) => (r.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Export snapshot -------------------------

	let exportModal = $state<Modal | null>(null);
	let exportSourceName = $state('');
	let exportBusy = $state(false);
	let exportError = $state<string | null>(null);

	function openExport(): void {
		exportSourceName = '';
		exportError = null;
		exportModal?.open();
	}

	async function submitExport(): Promise<void> {
		exportBusy = true;
		exportError = null;
		try {
			await client().send(new ExportSnapshotCommand({ sourceSnapshotName: exportSourceName }));
			toast.success('Snapshot export started');
			exportModal?.close();
			await refresh();
		} catch (e) {
			exportError = describeError(e);
		} finally {
			exportBusy = false;
		}
	}

	// ------------------------------ CloudFormation stack --------------------

	let stackModal = $state<Modal | null>(null);
	let stackSourceName = $state('');
	let stackInstanceType = $state('t2.micro');
	let stackAvailabilityZone = $state('');
	let stackBusy = $state(false);
	let stackError = $state<string | null>(null);

	function openStack(): void {
		stackSourceName = '';
		stackInstanceType = 't2.micro';
		stackAvailabilityZone = '';
		stackError = null;
		stackModal?.open();
	}

	async function submitStack(): Promise<void> {
		stackBusy = true;
		stackError = null;
		try {
			await client().send(
				new CreateCloudFormationStackCommand({
					instances: [
						{
							sourceName: stackSourceName,
							instanceType: stackInstanceType,
							portInfoSource: 'CLOSED',
							availabilityZone: stackAvailabilityZone
						}
					]
				})
			);
			toast.success('CloudFormation stack creation started');
			stackModal?.close();
			await refresh();
		} catch (e) {
			stackError = describeError(e);
		} finally {
			stackBusy = false;
		}
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<p class="text-xs text-slate-500 dark:text-slate-400">
	ExportSnapshot/CreateCloudFormationStack model the real Lightsail-to-EC2 migration path. Both of
	these record kinds -- ExportSnapshotRecord and CloudFormationStackRecord -- are among the four
	Lightsail resource kinds with no Tags field on the wire, so no tag UI is offered here.
</p>

<div class="flex justify-end gap-2">
	<button onclick={openExport} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600">
		Export snapshot
	</button>
	<button onclick={openStack} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create CloudFormation stack
	</button>
</div>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Export snapshot records</p>
	{#snippet exportCreatedCell(r: ExportSnapshotRecord)}
		{formatDate(r.createdAt)}
	{/snippet}
	<DataTable
		rows={filteredExports}
		rowKey={(r) => r.name ?? ''}
		columns={defineColumns<ExportSnapshotRecord>([
			{ key: 'name', label: 'Name' },
			{ key: 'state', label: 'State' },
			{ key: 'createdAt', label: 'Created', render: exportCreatedCell }
		])}
		{loading}
		emptyMessage="No export snapshot records found"
	/>
</div>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">CloudFormation stack records</p>
	{#snippet stackCreatedCell(r: CloudFormationStackRecord)}
		{formatDate(r.createdAt)}
	{/snippet}
	<DataTable
		rows={filteredStacks}
		rowKey={(r) => r.name ?? ''}
		columns={defineColumns<CloudFormationStackRecord>([
			{ key: 'name', label: 'Name' },
			{ key: 'state', label: 'State' },
			{ key: 'createdAt', label: 'Created', render: stackCreatedCell }
		])}
		{loading}
		emptyMessage="No CloudFormation stack records found"
	/>
</div>

<Modal bind:this={exportModal} title="Export snapshot">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-export-source">
				Source snapshot name (instance or disk snapshot)
				<input id="ls-export-source" bind:value={exportSourceName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if exportError}<p class="text-sm text-red-600 dark:text-red-400">{exportError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => exportModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitExport} disabled={exportBusy || !exportSourceName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Export</button>
	{/snippet}
</Modal>

<Modal bind:this={stackModal} title="Create CloudFormation stack">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-stack-source">
				Source (exported snapshot name)
				<input id="ls-stack-source" bind:value={stackSourceName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-stack-type">
				EC2 instance type
				<input id="ls-stack-type" bind:value={stackInstanceType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-stack-az">
				Availability zone
				<input id="ls-stack-az" bind:value={stackAvailabilityZone} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if stackError}<p class="text-sm text-red-600 dark:text-red-400">{stackError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => stackModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStack} disabled={stackBusy || !stackSourceName || !stackAvailabilityZone} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>
