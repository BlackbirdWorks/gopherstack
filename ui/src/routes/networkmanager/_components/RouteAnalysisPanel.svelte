<script lang="ts">
	// Route Analysis (services/networkmanager/PARITY.md family S, 2 ops).
	// This emulator is deliberately honest here: StartRouteAnalysis runs a
	// real RUNNING -> COMPLETED state machine, then resolves to
	// NOT_CONNECTED with a real reason code (NO_DESTINATION_ARN_PROVIDED or
	// TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND), because no cross-service walk
	// into EC2's transit-gateway route tables was wired in this pass
	// (tracked as bd issue gopherstack-700y). This UI must not imply a real
	// path computation happened -- it surfaces the reason code and this note
	// instead of drawing a plausible-looking path diagram.
	//
	// There is no List op for analyses (only Start/Get) -- this panel keeps
	// a session-local history of analyses started from this page so the
	// user can revisit a result without re-running it.
	import {
		StartRouteAnalysisCommand,
		GetRouteAnalysisCommand,
		type RouteAnalysis,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let globalNetworkId = $state('');

	type EndpointKind = 'ip' | 'tgw-attachment';
	let sourceKind = $state<EndpointKind>('ip');
	let sourceValue = $state('');
	let destKind = $state<EndpointKind>('ip');
	let destValue = $state('');
	let includeReturnPath = $state(false);
	let useMiddleboxes = $state(false);
	let starting = $state(false);
	let startError = $state<string | null>(null);

	let history = $state<RouteAnalysis[]>([]);
	let selected = $state<RouteAnalysis | null>(null);

	function endpointSpec(kind: EndpointKind, value: string): { IpAddress?: string; TransitGatewayAttachmentArn?: string } | undefined {
		if (!value.trim()) return undefined;
		return kind === 'ip' ? { IpAddress: value.trim() } : { TransitGatewayAttachmentArn: value.trim() };
	}

	function sleep(ms: number): Promise<void> {
		return new Promise((resolve) => {
			setTimeout(resolve, ms);
		});
	}

	async function pollUntilDone(routeAnalysisId: string): Promise<RouteAnalysis | undefined> {
		for (let attempt = 0; attempt < 20; attempt += 1) {
			const resp = await client().send(
				new GetRouteAnalysisCommand({ GlobalNetworkId: globalNetworkId, RouteAnalysisId: routeAnalysisId })
			);
			const analysis = resp.RouteAnalysis;
			if (analysis?.Status !== 'RUNNING') {
				return analysis;
			}
			await sleep(500);
		}
		return undefined;
	}

	async function startAnalysis(): Promise<void> {
		if (!globalNetworkId) {
			startError = 'Select a global network first.';
			return;
		}
		const source = endpointSpec(sourceKind, sourceValue);
		const destination = endpointSpec(destKind, destValue);
		if (!source || !destination) {
			startError = 'Source and destination are both required.';
			return;
		}
		starting = true;
		startError = null;
		try {
			const resp = await client().send(
				new StartRouteAnalysisCommand({
					GlobalNetworkId: globalNetworkId,
					Source: source,
					Destination: destination,
					IncludeReturnPath: includeReturnPath,
					UseMiddleboxes: useMiddleboxes
				})
			);
			let analysis = resp.RouteAnalysis;
			if (analysis?.RouteAnalysisId) {
				const done = await pollUntilDone(analysis.RouteAnalysisId);
				if (done) analysis = done;
			}
			if (analysis) {
				history = [analysis, ...history];
				selected = analysis;
				toast.success(`Route analysis ${analysis.Status ?? 'submitted'}`);
			}
		} catch (e) {
			startError = describeError(e);
			toast.error(startError);
		} finally {
			starting = false;
		}
	}

	const filteredHistory = $derived(
		history.filter((h) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (h.RouteAnalysisId ?? '').toLowerCase().includes(q);
		})
	);

	function resultBadgeClass(resultCode: string | undefined): string {
		if (resultCode === 'CONNECTED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (resultCode === 'NOT_CONNECTED') return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}
</script>

<div class="space-y-4">
	<div class="rounded-lg border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/30 px-4 py-3 text-sm text-amber-800 dark:text-amber-300">
		Path computation is not implemented in this emulator: no cross-service walk into EC2's
		transit-gateway route tables was wired (tracked as bd issue <strong>gopherstack-700y</strong>).
		Every analysis genuinely runs RUNNING → COMPLETED and then honestly resolves to
		<strong>NOT_CONNECTED</strong> with a real reason code -- it never fabricates a connected path.
	</div>

	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-ra-gn" />

	<div class="grid grid-cols-2 gap-4 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
		<div class="space-y-2">
			<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Source</p>
			<select bind:value={sourceKind} class="w-full px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
				<option value="ip">IP address</option>
				<option value="tgw-attachment">Transit gateway attachment ARN</option>
			</select>
			<input bind:value={sourceValue} placeholder={sourceKind === 'ip' ? '10.0.0.1' : 'arn:aws:ec2:...:transit-gateway-attachment/...'} class="w-full px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
		</div>
		<div class="space-y-2">
			<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Destination</p>
			<select bind:value={destKind} class="w-full px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
				<option value="ip">IP address</option>
				<option value="tgw-attachment">Transit gateway attachment ARN</option>
			</select>
			<input bind:value={destValue} placeholder={destKind === 'ip' ? '10.0.1.1' : 'arn:aws:ec2:...:transit-gateway-attachment/...'} class="w-full px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
		</div>
		<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={includeReturnPath} /> Include return path</label>
		<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={useMiddleboxes} /> Use middleboxes</label>
		{#if startError}<p class="col-span-2 text-sm text-red-600 dark:text-red-400">{startError}</p>{/if}
		<button onclick={startAnalysis} disabled={starting || !globalNetworkId} class="col-span-2 px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">
			{starting ? 'Running…' : 'Start route analysis'}
		</button>
	</div>

	{#if selected}
		<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-2">
			<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Result: {selected.RouteAnalysisId}</p>
			<p class="text-sm">Status: {selected.Status ?? '—'}</p>
			<p class="text-sm">
				Forward path:
				<span class="text-xs px-2 py-0.5 rounded-full {resultBadgeClass(selected.ForwardPath?.CompletionStatus?.ResultCode)}">
					{selected.ForwardPath?.CompletionStatus?.ResultCode ?? '—'}
				</span>
				{#if selected.ForwardPath?.CompletionStatus?.ReasonCode}
					<span class="text-xs text-slate-500 dark:text-slate-400">({selected.ForwardPath.CompletionStatus.ReasonCode})</span>
				{/if}
			</p>
			{#if selected.IncludeReturnPath}
				<p class="text-sm">
					Return path:
					<span class="text-xs px-2 py-0.5 rounded-full {resultBadgeClass(selected.ReturnPath?.CompletionStatus?.ResultCode)}">
						{selected.ReturnPath?.CompletionStatus?.ResultCode ?? '—'}
					</span>
					{#if selected.ReturnPath?.CompletionStatus?.ReasonCode}
						<span class="text-xs text-slate-500 dark:text-slate-400">({selected.ReturnPath.CompletionStatus.ReasonCode})</span>
					{/if}
				</p>
			{/if}
		</div>
	{/if}

	<div class="space-y-2">
		<p class="text-sm font-medium text-slate-700 dark:text-slate-300">This session's analyses</p>
		{#if filteredHistory.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No analyses started yet.</p>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-2 py-1 font-medium">Analysis ID</th>
						<th class="px-2 py-1 font-medium">Status</th>
						<th class="px-2 py-1 font-medium">Result</th>
						<th class="px-2 py-1 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each filteredHistory as h (h.RouteAnalysisId)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-2 py-1">{h.RouteAnalysisId}</td>
							<td class="px-2 py-1">{h.Status ?? '—'}</td>
							<td class="px-2 py-1">{h.ForwardPath?.CompletionStatus?.ResultCode ?? '—'}</td>
							<td class="px-2 py-1 text-right">
								<button onclick={() => (selected = h)} class="text-blue-600 hover:underline text-xs">View</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</div>
</div>
