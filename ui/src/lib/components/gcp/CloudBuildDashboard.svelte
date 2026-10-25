<script lang="ts">
	import { onMount } from 'svelte';
	import { Hammer } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import CloudBuildDetail from '#lib/components/gcp/CloudBuildDetail.svelte';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		ACTIVE_BUILD_STATUSES,
		cancelBuild,
		deleteTrigger,
		listBuilds,
		listTriggers,
		readStoredCloudBuildLocation,
		readStoredCloudBuildProject,
		retryBuild,
		runTrigger,
		writeStoredCloudBuildScope,
		type BuildInfo,
		type TriggerInfo
	} from '#lib/gcpcloudbuild.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'builds', label: 'Builds' },
		{ id: 'triggers', label: 'Triggers' }
	];

	let project = $state(readStoredCloudBuildProject());
	let location = $state(readStoredCloudBuildLocation());
	let tab = $state('builds');
	let builds = $state<BuildInfo[]>([]);
	let triggers = $state<TriggerInfo[]>([]);
	let loading = $state(true);
	let error = $state('');

	const scope = $derived({ project, location });

	async function load() {
		try {
			[builds, triggers] = await Promise.all([listBuilds(scope), listTriggers(scope)]);
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
		if (await attempt(() => cancelBuild(scope, b.id), `Cancelled build ${b.id}`, `Failed to cancel ${b.id}`)) await load();
	}

	async function retry(b: BuildInfo) {
		if (await attempt(() => retryBuild(scope, b.id), `Retried build ${b.id}`, `Failed to retry ${b.id}`)) await load();
	}

	async function run(t: TriggerInfo) {
		if (await attempt(() => runTrigger(scope, t.id, ''), `Started a build from ${t.displayName || t.id}`, `Failed to run ${t.displayName || t.id}`)) {
			tab = 'builds';
			await load();
		}
	}

	async function removeTrigger(row: ResourceRow) {
		const ok = await confirmedAttempt({ title: 'Delete trigger', message: `Delete trigger "${row.cells[0]}"?` }, () => deleteTrigger(scope, row.id), `Deleted trigger ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`);
		if (ok) await load();
	}

	const buildById = (id: string) => builds.find((b) => b.id === id);
	const triggerById = (id: string) => triggers.find((t) => t.id === id);

	const buildRows = $derived(
		builds.map((b) => ({
			id: b.id,
			cells: [b.id, b.status, `${b.steps.length}`, b.triggerId, b.createTime],
			raw: b.raw
		}))
	);
	const triggerRows = $derived(triggers.map((t) => ({ id: t.id, cells: [t.displayName || t.id, t.event, t.disabled ? 'disabled' : 'enabled', t.description], raw: t.raw })));

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="cloudbuild-dashboard">
	<PageHeader icon={Hammer} title="Cloud Build" description="Google Cloud Build: builds, steps, logs and triggers" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="cloudbuild" {project} {location} hint="Resources are listed per project and location." onApply={applyScope} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'builds'}
		<GcpResourceTable testId="cloudbuild-build" noun="builds" columns={['Build', 'Status', 'Steps', 'Trigger', 'Created']} rows={buildRows} {loading} {error} onRetry={load}>
			{#snippet actions(row)}
				{@const b = buildById(row.id)}
				{#if b}
					{#if ACTIVE_BUILD_STATUSES.has(b.status)}
						<button class={GCP_MINI} onclick={() => cancel(b)} data-testid="cloudbuild-cancel">Cancel</button>
					{/if}
					<button class={GCP_MINI} onclick={() => retry(b)} data-testid="cloudbuild-retry">Retry</button>
				{/if}
			{/snippet}
			{#snippet detail(row)}
				{@const b = buildById(row.id)}
				{#if b}<CloudBuildDetail build={b} {scope} />{/if}
			{/snippet}
		</GcpResourceTable>
	{:else}
		<GcpResourceTable testId="cloudbuild-trigger" noun="triggers" columns={['Trigger', 'Event', 'State', 'Description']} rows={triggerRows} {loading} {error} onRetry={load} onDelete={removeTrigger}>
			{#snippet actions(row)}
				{@const t = triggerById(row.id)}
				{#if t}<button class={GCP_MINI} onclick={() => run(t)} disabled={t.disabled} data-testid="cloudbuild-run">Run</button>{/if}
			{/snippet}
		</GcpResourceTable>
	{/if}
</div>
