<script lang="ts">
	// Jobs (services/mgn/PARITY.md family B, 3 ops): DescribeJobs,
	// DescribeJobLogItems, DeleteJob. Jobs are produced as a side effect of
	// StartTest/StartCutover/TerminateTargetInstances on the Source Servers
	// tab -- there is no direct "create job" action here.
	import {
		DescribeJobsCommand,
		DescribeJobLogItemsCommand,
		DeleteJobCommand,
		type Job,
		type JobLog,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let jobs = $state<Job[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchJobs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeJobsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		jobs = reset ? (resp.items ?? []) : [...jobs, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchJobs(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchJobs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		jobs.filter((j) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(j.jobID ?? '').toLowerCase().includes(q) ||
				(j.status ?? '').toLowerCase().includes(q) ||
				(j.type ?? '').toLowerCase().includes(q)
			);
		})
	);

	async function deleteJob(j: Job): Promise<void> {
		if (!j.jobID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete job',
			message: `Delete job ${j.jobID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteJobCommand({ jobID: j.jobID }));
			toast.success('Job deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Job | null>(null);
	let logItems = $state<JobLog[]>([]);
	let logNextToken = $state<string | undefined>();
	let logLoading = $state(false);
	let logError = $state<string | null>(null);

	async function openDetail(j: Job): Promise<void> {
		viewed = j;
		logItems = [];
		logNextToken = undefined;
		logError = null;
		detailModal?.open();
		await loadLogs(true);
	}

	async function loadLogs(reset: boolean): Promise<void> {
		if (!viewed?.jobID) return;
		logLoading = true;
		try {
			const resp = await client().send(
				new DescribeJobLogItemsCommand({
					jobID: viewed.jobID,
					maxResults: 50,
					nextToken: reset ? undefined : logNextToken
				})
			);
			logItems = reset ? (resp.items ?? []) : [...logItems, ...(resp.items ?? [])];
			logNextToken = resp.nextToken;
		} catch (e) {
			logError = describeError(e);
		} finally {
			logLoading = false;
		}
	}

	const columns = defineColumns<Job>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'type', label: 'Type' },
		{ key: 'status', label: 'Status' },
		{ key: 'initiatedBy', label: 'Initiated By' },
		{
			key: 'creationDateTime',
			label: 'Created'
		}
	]);
	const logColumns = defineColumns<JobLog>([
		{ key: 'logDateTime', label: 'Time' },
		{ key: 'event', label: 'Event' }
	]);
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(j: Job)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(j)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteJob(j)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(j) => j.jobID ?? ''}
	columns={[...columns, { key: 'actions', label: '', render: rowActions }]}
	{loading}
	emptyMessage="No jobs found -- jobs are created by Start Test / Start Cutover / Terminate Target Instances on the Source Servers tab"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={detailModal} title="Job {viewed?.jobID ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-3 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Type</dt><dd>{viewed.type ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Status</dt><dd>{viewed.status ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.creationDateTime)}</dd></div>
					<div><dt class="text-slate-500">Ended</dt><dd>{formatDate(viewed.endDateTime)}</dd></div>
				</dl>
				<div>
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Participating servers</p>
					<ul class="text-sm space-y-1">
						{#each viewed.participatingServers ?? [] as p (p.sourceServerID)}
							<li>{p.sourceServerID}: {p.launchStatus ?? '—'}</li>
						{:else}
							<li class="text-slate-500">None</li>
						{/each}
					</ul>
				</div>
				<div>
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
						Job log
						<span class="ml-1 text-xs font-normal text-amber-600 dark:text-amber-400"
							>(deterministic simulated progression, not a real launch engine)</span
						>
					</p>
					{#if logError}<p class="text-sm text-red-600 dark:text-red-400">{logError}</p>{/if}
					<DataTable rows={logItems} rowKey={(l) => `${l.logDateTime}-${l.event}`} columns={logColumns} loading={logLoading} emptyMessage="No log entries" />
					<LoadMore hasMore={!!logNextToken} loading={logLoading} onLoadMore={() => loadLogs(false)} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
