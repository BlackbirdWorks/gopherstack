<script lang="ts">
	import { onMount, untrack } from 'svelte';
	import GcpResourceTable from '#lib/components/gcp/GcpResourceTable.svelte';
	import { listOperations, operationInScope, operationRunning, POLL_MS, shouldPoll, type ComputeOperation } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; zone: string; region: string; refresh: number };
	let { project, zone, region, refresh }: Props = $props();

	const RECENT = 100;
	let operations = $state<ComputeOperation[]>([]);
	let loading = $state(true);
	let error = $state('');
	let rounds = 0;

	const unsettled = $derived(operations.some((o) => operationRunning(o)));
	const rows = $derived(
		operations
			.filter((o) => operationInScope(o.scope, zone, region))
			.slice(0, RECENT)
			.map((o) => ({ id: o.name, cells: [o.type, o.target, o.scope, o.progress === 100 ? o.status : `${o.status} ${o.progress}%`, o.started, o.ended, o.error], raw: o.raw }))
	);

	async function load(quiet = false) {
		if (!quiet) loading = true;
		try {
			operations = await listOperations(project);
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
</script>

<GcpResourceTable testId="compute-operation" noun="operations" columns={['Operation', 'Target', 'Scope', 'Status', 'Started', 'Finished', 'Error']} {rows} {loading} {error} onRetry={() => load()} />
