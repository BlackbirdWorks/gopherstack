<script lang="ts">
	import { onMount, untrack } from 'svelte';
	import { Plus } from 'lucide-svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import ComputeInstanceDetail from '#lib/components/gcp/ComputeInstanceDetail.svelte';
	import ComputeInstanceForm from '#lib/components/gcp/ComputeInstanceForm.svelte';
	import { confirmedAttempt, attempt, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { deleteInstance, inZoneScope, instanceAction, instanceTransitioning, labelText, listInstances, POLL_MS, shouldPoll, type Instance, type InstanceAction } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; zone: string; region: string; zones: string[]; refresh: number };
	let { project, zone, region, zones, refresh }: Props = $props();

	let instances = $state<Instance[]>([]);
	let loading = $state(true);
	let error = $state('');
	let creating = $state(false);
	let rounds = 0;

	const unsettled = $derived(instances.some((i) => instanceTransitioning(i)));
	const byId = $derived(new Map(instances.map((i) => [`${i.zone}/${i.name}`, i])));
	const rows = $derived(
		instances
			.filter((i) => inZoneScope(i.zone, zone, region))
			.map((i) => ({
				id: `${i.zone}/${i.name}`,
				cells: [i.name, i.zone, i.machineType, i.status, i.internalIps.join(', '), i.externalIps.join(', '), i.created],
				raw: i.raw
			}))
	);

	async function load(quiet = false) {
		if (!quiet) loading = true;
		try {
			instances = await listInstances(project);
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
		untrack(() => {
			rounds = 0;
			void load();
		});
	});

	onMount(() => {
		const timer = setInterval(() => {
			if (!shouldPoll(unsettled, rounds)) return;
			rounds++;
			void load(true);
		}, POLL_MS);
		return () => clearInterval(timer);
	});

	async function run(action: InstanceAction, i: Instance) {
		rounds = 0;
		const label = `${action[0].toUpperCase()}${action.slice(1)}`;
		if (await attempt(() => instanceAction(project, i.zone, i.name, action), `${label} requested for ${i.name}`, `Failed to ${action} ${i.name}`)) await load(true);
	}

	async function remove(i: Instance) {
		rounds = 0;
		const ok = await confirmedAttempt({ title: 'Delete instance', message: `Delete instance "${i.name}"? Its auto-delete disks are removed too.` }, () => deleteInstance(project, i.zone, i.name), `Deleting ${i.name}`, `Failed to delete ${i.name}`);
		if (ok) await load(true);
	}

	function created() {
		creating = false;
		rounds = 0;
		void load(true);
	}
</script>

<div class="space-y-4">
	<div class="flex justify-end">
		<button class="{GCP_PRIMARY} flex items-center gap-1 !py-1" onclick={() => (creating = !creating)} data-testid="compute-instance-create-toggle"><Plus class="h-4 w-4" /> Create instance</button>
	</div>
	{#if creating}
		<ComputeInstanceForm {project} {zone} {zones} onCreated={created} onClose={() => (creating = false)} />
	{/if}
	<GcpResourceTable testId="compute-instance" noun="instances" columns={['Name', 'Zone', 'Machine type', 'Status', 'Internal IP', 'External IP', 'Created']} {rows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const i = byId.get(r.id); if (i) void remove(i); }}>
		{#snippet actions(row: ResourceRow)}
			{@const i = byId.get(row.id)}
			{#if i}
				<button class={GCP_MINI} disabled={i.status !== 'TERMINATED'} onclick={() => run('start', i)} data-testid="compute-instance-start">Start</button>
				<button class={GCP_MINI} disabled={i.status !== 'RUNNING'} onclick={() => run('stop', i)} data-testid="compute-instance-stop">Stop</button>
				<button class={GCP_MINI} disabled={i.status !== 'RUNNING'} onclick={() => run('suspend', i)} data-testid="compute-instance-suspend">Suspend</button>
				<button class={GCP_MINI} disabled={i.status !== 'SUSPENDED'} onclick={() => run('resume', i)} data-testid="compute-instance-resume">Resume</button>
				<button class={GCP_MINI} disabled={i.status !== 'RUNNING'} onclick={() => run('reset', i)} data-testid="compute-instance-reset">Reset</button>
			{/if}
		{/snippet}
		{#snippet detail(row: ResourceRow)}
			{@const i = byId.get(row.id)}
			{#if i}<ComputeInstanceDetail {project} instance={i} labels={labelText(i.labels)} />{/if}
		{/snippet}
	</GcpResourceTable>
</div>
