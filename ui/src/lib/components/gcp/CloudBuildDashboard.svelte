<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { Hammer } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import CloudBuildDetail from '#lib/components/gcp/CloudBuildDetail.svelte';
	import CloudBuildTriggers from '#lib/components/gcp/CloudBuildTriggers.svelte';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		ACTIVE_BUILD_STATUSES,
		approveBuild,
		cancelBuild,
		deleteTrigger,
		describeGcpError,
		durationMs,
		formatDuration,
		lastRun,
		listBuilds,
		listTriggers,
		listWorkerPools,
		readStoredCloudBuildLocation,
		readStoredCloudBuildProject,
		retryBuild,
		runTriggerSource,
		writeStoredCloudBuildScope,
		type BuildInfo,
		type PoolInfo,
		type RunSource,
		type TriggerInfo
	} from '#lib/gcpcloudbuild.ts';

	const TABS = [
		{ id: 'builds', label: 'Builds' },
		{ id: 'triggers', label: 'Triggers' },
		{ id: 'pools', label: 'Worker pools' }
	];
	const POLL_MS = 3000;

	let project = $state(readStoredCloudBuildProject());
	let location = $state(readStoredCloudBuildLocation());
	let tab = $state('builds');
	let builds = $state<BuildInfo[]>([]);
	let triggers = $state<TriggerInfo[]>([]);
	let pools = $state<PoolInfo[]>([]);
	let timer: ReturnType<typeof setInterval> | undefined;
	let loading = $state(true);
	let error = $state('');

	const scope = $derived({ project, location });

	async function load() {
		try {
			[builds, triggers, pools] = await Promise.all([listBuilds(scope), listTriggers(scope), listWorkerPools(scope)]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function applyScope(p: string, l: string) {
		project = p;
		location = l;
		writeStoredCloudBuildScope(p, l);
		loading = true;
		void load();
	}

	async function cancel(b: BuildInfo) {
		const ok = await confirmedAttempt({ title: 'Cancel build', message: `Cancel build "${b.id}"?`, confirmLabel: 'Cancel build' }, () => cancelBuild(scope, b.id), `Cancelled build ${b.id}`, `Failed to cancel ${b.id}`);
		if (ok) await load();
	}

	async function decide(b: BuildInfo, approved: boolean) {
		const verb = approved ? 'Approve' : 'Reject';
		const ok = await confirmedAttempt({ title: `${verb} build`, message: `${verb} build "${b.id}"?`, confirmLabel: verb, dangerous: !approved }, () => approveBuild(scope, b.id, approved, ''), `${verb}d build ${b.id}`, `Failed to ${verb.toLowerCase()} ${b.id}`);
		if (ok) await load();
	}

	async function retry(b: BuildInfo) {
		if (await attempt(() => retryBuild(scope, b.id), `Retried build ${b.id}`, `Failed to retry ${b.id}`)) await load();
	}

	async function run(t: TriggerInfo, src: RunSource): Promise<void> {
		if (await attempt(() => runTriggerSource(scope, t.id, src), `Started a build from ${t.displayName || t.id}`, `Failed to run ${t.displayName || t.id}`)) {
			tab = 'builds';
			await load();
		}
	}

	async function removeTrigger(row: ResourceRow) {
		const ok = await confirmedAttempt({ title: 'Delete trigger', message: `Delete trigger "${row.cells[0]}"?` }, () => deleteTrigger(scope, row.id), `Deleted trigger ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`);
		if (ok) await load();
	}

	const buildById = (id: string) => builds.find((b) => b.id === id);

	const triggerName = (id: string) => (id ? triggers.find((t) => t.id === id)?.displayName || id : '');
	const buildRows = $derived(
		builds.map((b) => ({
			id: b.id,
			cells: [b.id, b.approval === 'PENDING' ? 'PENDING (awaiting approval)' : b.status, triggerName(b.triggerId), b.source, formatDuration(durationMs(b.startTime, b.finishTime)), b.resultImages.length > 0 ? b.resultImages.map((i) => i.name).join(', ') : b.images.join(', '), b.createTime],
			raw: b.raw
		}))
	);
	const triggerRows = $derived(
		triggers.map((t) => {
			const last = lastRun(t.id, builds);
			return { id: t.id, cells: [t.displayName || t.id, t.event, t.filter, t.disabled ? 'disabled' : 'enabled', last ? `${last.status} (${last.id.slice(0, 8)})` : 'never', t.description], raw: t.raw };
		})
	);
	const poolRows = $derived(pools.map((p) => ({ id: p.name, cells: [p.id, p.state, p.machineType, p.diskSizeGb, p.createTime], raw: p.raw })));
	const anyActive = $derived(builds.some((b) => ACTIVE_BUILD_STATUSES.has(b.status)));

	$effect(() => {
		clearInterval(timer);
		if (anyActive) timer = setInterval(() => void load(), POLL_MS);
		return () => clearInterval(timer);
	});

	onMount(load);
	onDestroy(() => clearInterval(timer));
</script>

<div class="space-y-6 pb-20" data-testid="cloudbuild-dashboard">
	<PageHeader icon={Hammer} title="Cloud Build" description="Google Cloud Build: builds, steps, logs and triggers" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="cloudbuild" {project} {location} hint="Resources are listed per project and location." onApply={applyScope} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'builds'}
		<GcpResourceTable testId="cloudbuild-build" noun="builds" columns={['Build', 'Status', 'Trigger', 'Source', 'Duration', 'Images', 'Created']} rows={buildRows} {loading} {error} onRetry={load}>
			{#snippet actions(row)}
				{@const b = buildById(row.id)}
				{#if b}
					{#if ACTIVE_BUILD_STATUSES.has(b.status)}
						<button class={GCP_MINI} onclick={() => cancel(b)} data-testid="cloudbuild-cancel">Cancel</button>
					{/if}
					{#if b.approval === 'PENDING'}
						<button class={GCP_MINI} onclick={() => decide(b, true)} data-testid="cloudbuild-approve">Approve</button>
						<button class="{GCP_MINI} text-red-600" onclick={() => decide(b, false)} data-testid="cloudbuild-reject">Reject</button>
					{/if}
					<button class={GCP_MINI} onclick={() => retry(b)} data-testid="cloudbuild-retry">Retry</button>
				{/if}
			{/snippet}
			{#snippet detail(row)}
				{@const b = buildById(row.id)}
				{#if b}<CloudBuildDetail build={b} {scope} />{/if}
			{/snippet}
		</GcpResourceTable>
	{:else if tab === 'triggers'}
		<CloudBuildTriggers {triggers} rows={triggerRows} {loading} {error} onRetry={load} onRun={run} onDelete={removeTrigger} />
	{:else}
		<GcpResourceTable testId="cloudbuild-pool" noun="worker pools" columns={['Pool', 'State', 'Machine type', 'Disk (GB)', 'Created']} rows={poolRows} {loading} {error} onRetry={load} />
	{/if}
</div>
