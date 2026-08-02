<script lang="ts">
	// Network Migration -- Mapper segments & constructs (services/mgn/PARITY.md
	// family M's other half, 4 ops): ListNetworkMigrationMapperSegments,
	// GetNetworkMigrationMapperSegmentConstruct,
	// ListNetworkMigrationMapperSegmentConstructs,
	// UpdateNetworkMigrationMapperSegment.
	//
	// This family is DELIBERATELY EMPTY in this backend, and documented as
	// such (PARITY.md's Implementation Summary: "the OTHER option the task
	// weighed -- leave the families genuinely empty and record it as a gap
	// -- rather than a third synthetic seeding seam"). List* always return an
	// empty Items array (after validating the definition/execution scope
	// exists); Get/Update always 404. There is no real network-analysis
	// engine to produce a segment from. This panel calls the real ops (so
	// they are genuinely wired, not stubbed out of the UI) but shows an
	// honest empty/expected-404 state rather than fabricating segment data.
	import {
		ListNetworkMigrationMapperSegmentsCommand,
		ListNetworkMigrationMapperSegmentConstructsCommand,
		GetNetworkMigrationMapperSegmentConstructCommand,
		UpdateNetworkMigrationMapperSegmentCommand,
		type NetworkMigrationMapperSegment,
		type NetworkMigrationMapperSegmentConstruct,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import { describeError } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client }: Props = $props();

	// This panel is on-demand only (scoped by definition+execution IDs the
	// user supplies) -- there is no account-wide list to eagerly load on
	// mount, so it exposes no `refresh()` for +page.svelte's Refresh button
	// to call (that button is a no-op on this tab; every list here is
	// user-triggered via its own button).

	let definitionID = $state('');
	let executionID = $state('');
	let segmentID = $state('');
	let constructID = $state('');

	let segments = $state<NetworkMigrationMapperSegment[]>([]);
	let segmentsError = $state<string | null>(null);
	let segmentsLoading = $state(false);
	let segmentsLoaded = $state(false);

	let constructs = $state<NetworkMigrationMapperSegmentConstruct[]>([]);
	let constructsError = $state<string | null>(null);
	let constructsLoading = $state(false);

	let constructDetail = $state<NetworkMigrationMapperSegmentConstruct | null>(null);
	let constructDetailError = $state<string | null>(null);

	let updateScopeTagsText = $state('');
	let updateError = $state<string | null>(null);

	async function listSegments(): Promise<void> {
		if (!definitionID.trim() || !executionID.trim()) {
			segmentsError = 'Definition ID and Execution ID are required.';
			return;
		}
		segmentsLoading = true;
		segmentsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationMapperSegmentsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			segments = resp.items ?? [];
			segmentsLoaded = true;
		} catch (e) {
			segmentsError = describeError(e);
		} finally {
			segmentsLoading = false;
		}
	}

	async function listConstructs(): Promise<void> {
		if (!definitionID.trim() || !executionID.trim() || !segmentID.trim()) {
			constructsError = 'Definition ID, Execution ID and Segment ID are required.';
			return;
		}
		constructsLoading = true;
		constructsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationMapperSegmentConstructsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					segmentID: segmentID.trim()
				})
			);
			constructs = resp.items ?? [];
		} catch (e) {
			constructsError = describeError(e);
		} finally {
			constructsLoading = false;
		}
	}

	async function getConstruct(): Promise<void> {
		if (!definitionID.trim() || !executionID.trim() || !segmentID.trim() || !constructID.trim()) {
			constructDetailError = 'Definition ID, Execution ID, Segment ID and Construct ID are required.';
			return;
		}
		constructDetail = null;
		constructDetailError = null;
		try {
			const resp = await client().send(
				new GetNetworkMigrationMapperSegmentConstructCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					segmentID: segmentID.trim(),
					constructID: constructID.trim()
				})
			);
			constructDetail = resp.construct ?? null;
		} catch (e) {
			constructDetailError = describeError(e);
		}
	}

	async function submitUpdateSegment(): Promise<void> {
		if (!definitionID.trim() || !executionID.trim() || !segmentID.trim()) {
			updateError = 'Definition ID, Execution ID and Segment ID are required.';
			return;
		}
		updateError = null;
		try {
			const scopeTags: Record<string, string> = {};
			for (const pair of updateScopeTagsText.split(',')) {
				const [k, v] = pair.split('=');
				if (k?.trim()) scopeTags[k.trim()] = (v ?? '').trim();
			}
			await client().send(
				new UpdateNetworkMigrationMapperSegmentCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					segmentID: segmentID.trim(),
					scopeTags
				})
			);
			toast.success('Segment updated');
		} catch (e) {
			updateError = describeError(e);
			toast.error(updateError);
		}
	}

	const segmentColumns = defineColumns<NetworkMigrationMapperSegment>([
		{ key: 'segmentID', label: 'Segment ID' },
		{ key: 'segmentType', label: 'Type' },
		{ key: 'name', label: 'Name' }
	]);
	const constructColumns = defineColumns<NetworkMigrationMapperSegmentConstruct>([
		{ key: 'constructID', label: 'Construct ID' },
		{ key: 'constructType', label: 'Type' },
		{ key: 'name', label: 'Name' }
	]);
</script>

<div class="rounded-lg border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/30 px-4 py-3 text-sm text-amber-800 dark:text-amber-300">
	This family is deliberately empty in this emulator: there is no real
	network-analysis engine to produce mapper segments from, and PARITY.md
	documents this as a genuine, honest gap rather than fabricating segment
	data. The calls below are real and wired -- List operations return an
	empty list (after validating the definition/execution scope), and
	Get/Update return 404 (no segment has ever been created).
</div>

<div class="grid grid-cols-2 gap-3">
	<label class="flex flex-col gap-1 text-sm">Network migration definition ID
		<input bind:value={definitionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<label class="flex flex-col gap-1 text-sm">Network migration execution ID
		<input bind:value={executionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
</div>

<div class="space-y-2">
	<button onclick={listSegments} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700">List mapper segments</button>
	{#if segmentsError}<p class="text-sm text-red-600 dark:text-red-400">{segmentsError}</p>{/if}
	{#if segmentsLoaded}
		<DataTable rows={segments} rowKey={(s) => s.segmentID ?? ''} columns={segmentColumns} loading={segmentsLoading} emptyMessage="No mapper segments -- expected, see the note above" />
	{/if}
</div>

<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
	<label class="flex flex-col gap-1 text-sm w-64">Segment ID
		<input bind:value={segmentID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<button onclick={listConstructs} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700">List segment constructs</button>
	{#if constructsError}<p class="text-sm text-red-600 dark:text-red-400">{constructsError}</p>{/if}
	<DataTable rows={constructs} rowKey={(c) => c.constructID ?? ''} columns={constructColumns} loading={constructsLoading} emptyMessage="No constructs -- expected, see the note above" />
</div>

<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
	<label class="flex flex-col gap-1 text-sm w-64">Construct ID
		<input bind:value={constructID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<button onclick={getConstruct} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700">Get construct</button>
	{#if constructDetailError}<p class="text-sm text-red-600 dark:text-red-400">{constructDetailError}</p>{/if}
	{#if constructDetail}<p class="text-sm text-slate-600 dark:text-slate-300">{JSON.stringify(constructDetail)}</p>{/if}
</div>

<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
	<label class="flex flex-col gap-1 text-sm">Update segment scope tags (key=value, comma separated)
		<input bind:value={updateScopeTagsText} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<button onclick={submitUpdateSegment} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700">Update segment</button>
	{#if updateError}<p class="text-sm text-red-600 dark:text-red-400">{updateError}</p>{/if}
</div>
