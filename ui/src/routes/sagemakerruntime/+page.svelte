<script lang="ts">
	import { regionalClient } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import { getSageMakerRuntimeClient } from '$lib/aws-client';
	import {
		InvokeEndpointCommand,
		InvokeEndpointAsyncCommand,
		InvokeEndpointWithResponseStreamCommand
	} from '@aws-sdk/client-sagemaker-runtime';
	import { toast } from 'svelte-sonner';
	import { Zap, RefreshCw, Send, Activity, Radio } from 'lucide-svelte';

	interface AsyncInvocation {
		inferenceId: string;
		outputLocation: string;
		endpointName: string;
		startedAt: string;
		region: string;
	}

	const smr = regionalClient(getSageMakerRuntimeClient);

	let activeTab = $state<'invoke' | 'streaming' | 'async'>('invoke');

	// Sync invocation state
	let invokeEndpoint = $state('');
	let invokeBody = $state('{"inputs": "Hello, world!"}');
	let invokeResponse = $state<string | null>(null);
	let invoking = $state(false);

	// Streaming invocation state
	let streamEndpoint = $state('');
	let streamBody = $state('{"inputs": "Tell me about AWS SageMaker."}');
	let streamChunks = $state<string[]>([]);
	let streaming = $state(false);

	// Async invocation state
	let asyncEndpoint = $state('');
	let asyncBody = $state('{"inputs": "Async inference request"}');
	let asyncInputLocation = $state('s3://my-bucket/input/request.json');
	let asyncInvocations = $state<AsyncInvocation[]>([]);
	let asyncInvoking = $state(false);

	async function invokeEndpointSync() {
		if (!invokeEndpoint.trim()) {
			toast.error('Please enter an endpoint name');
			return;
		}
		invoking = true;
		invokeResponse = null;
		try {
			const resp = await smr().send(
				new InvokeEndpointCommand({
					EndpointName: invokeEndpoint.trim(),
					Body: new TextEncoder().encode(invokeBody),
					ContentType: 'application/json'
				})
			);
			const decoded = new TextDecoder().decode(resp.Body as Uint8Array);
			invokeResponse = decoded;
			toast.success('Endpoint invoked successfully');
		} catch (e) {
			toast.error('Invocation failed: ' + String(e));
		} finally {
			invoking = false;
		}
	}

	async function invokeEndpointStream() {
		if (!streamEndpoint.trim()) {
			toast.error('Please enter an endpoint name');
			return;
		}
		streaming = true;
		streamChunks = [];
		try {
			const resp = await smr().send(
				new InvokeEndpointWithResponseStreamCommand({
					EndpointName: streamEndpoint.trim(),
					Body: new TextEncoder().encode(streamBody),
					ContentType: 'application/json'
				})
			);
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const stream = (resp as any).Body;
			if (stream && typeof stream[Symbol.asyncIterator] === 'function') {
				for await (const event of stream) {
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					const part = (event as any).PayloadPart;
					if (part?.Bytes) {
						const chunk = new TextDecoder().decode(part.Bytes as Uint8Array);
						streamChunks = [...streamChunks, chunk];
					}
				}
			} else if (resp.Body) {
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				const decoded = new TextDecoder().decode(resp.Body as unknown as Uint8Array);
				streamChunks = [decoded];
			}
			toast.success('Streaming complete');
		} catch (e) {
			toast.error('Streaming failed: ' + String(e));
		} finally {
			streaming = false;
		}
	}

	async function invokeEndpointAsync() {
		if (!asyncEndpoint.trim()) {
			toast.error('Please enter an endpoint name');
			return;
		}
		asyncInvoking = true;
		try {
			const resp = await smr().send(
				new InvokeEndpointAsyncCommand({
					EndpointName: asyncEndpoint.trim(),
					InputLocation: asyncInputLocation.trim() || 's3://mock-bucket/input',
					ContentType: 'application/json'
				})
			);
			const inv: AsyncInvocation = {
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				inferenceId: (resp as any).InferenceId ?? 'mock-inference-id',
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				outputLocation: (resp as any).OutputLocation ?? 's3://mock-bucket/output',
				endpointName: asyncEndpoint.trim(),
				startedAt: new Date().toISOString(),
				region: currentRegion()
			};
			asyncInvocations = [inv, ...asyncInvocations];
			toast.success('Async invocation submitted: ' + inv.inferenceId);
		} catch (e) {
			toast.error('Async invocation failed: ' + String(e));
		} finally {
			asyncInvoking = false;
		}
	}

</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">SageMaker Runtime</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Invoke SageMaker endpoints for real-time and async inference</p>
			</div>
		</div>
		<button onclick={() => { invokeResponse = null; streamChunks = []; asyncInvocations = []; }} title="Clear results" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Clear
		</button>
	</div>

	<div class="grid grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg"><Send class="w-5 h-5 text-teal-600 dark:text-teal-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{invokeResponse ? 1 : 0}</p><p class="text-sm text-gray-500 dark:text-gray-400">Sync Responses</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Radio class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{streamChunks.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Stream Chunks</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{asyncInvocations.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Async Jobs</p></div>
		</div>
	</div>

	<div class="flex gap-2">
		{#each [['invoke', 'Invoke Endpoint'], ['streaming', 'Streaming Invocation'], ['async', 'Async Tracking']] as [tab, label]}
			<button onclick={() => { activeTab = tab as typeof activeTab; }}
				class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-teal-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
				{label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'invoke'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Invoke Endpoint (Synchronous)</h2>
			<div>
				<label for="invoke-endpoint" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Endpoint Name</label>
				<input id="invoke-endpoint" bind:value={invokeEndpoint} placeholder="my-sagemaker-endpoint" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm" />
			</div>
			<div>
				<label for="invoke-body" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Request Body (JSON)</label>
				<textarea id="invoke-body" bind:value={invokeBody} rows={4} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none font-mono"></textarea>
			</div>
			<button onclick={invokeEndpointSync} disabled={invoking} class="flex items-center gap-2 px-4 py-2 bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
				<Send class="w-4 h-4" /> {invoking ? 'Invoking...' : 'Invoke'}
			</button>
			{#if invokeResponse}
				<div class="p-4 bg-gray-50 dark:bg-slate-700/50 rounded-lg">
					<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-2">Response:</p>
					<pre class="text-sm text-gray-900 dark:text-white whitespace-pre-wrap font-mono">{invokeResponse}</pre>
				</div>
			{/if}
		</div>
	{:else if activeTab === 'streaming'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Streaming Invocation</h2>
			<p class="text-sm text-gray-500 dark:text-gray-400">Invoke an endpoint and receive the response as a stream of chunks.</p>
			<div>
				<label for="stream-endpoint" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Endpoint Name</label>
				<input id="stream-endpoint" bind:value={streamEndpoint} placeholder="my-streaming-endpoint" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm" />
			</div>
			<div>
				<label for="stream-body" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Request Body (JSON)</label>
				<textarea id="stream-body" bind:value={streamBody} rows={3} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none font-mono"></textarea>
			</div>
			<div class="flex gap-3">
				<button onclick={invokeEndpointStream} disabled={streaming} class="flex items-center gap-2 px-4 py-2 bg-purple-600 hover:bg-purple-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
					<Radio class="w-4 h-4" /> {streaming ? 'Streaming...' : 'Start Stream'}
				</button>
				{#if streamChunks.length > 0}
					<button onclick={() => { streamChunks = []; }} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
						Clear
					</button>
				{/if}
			</div>
			{#if streaming || streamChunks.length > 0}
				<div class="space-y-1">
					<p class="text-xs font-semibold text-gray-500 dark:text-gray-400">Stream Output ({streamChunks.length} chunk{streamChunks.length !== 1 ? 's' : ''}):</p>
					<div class="min-h-24 max-h-64 overflow-y-auto p-3 bg-gray-50 dark:bg-slate-900/50 rounded-lg font-mono text-sm text-gray-900 dark:text-white">
						{#if streamChunks.length === 0 && streaming}
							<span class="text-gray-400 dark:text-gray-500 animate-pulse">Waiting for chunks...</span>
						{:else}
							{#each streamChunks as chunk, i}
								<div class="flex gap-2">
									<span class="text-gray-400 dark:text-gray-500 select-none">[{i + 1}]</span>
									<span class="whitespace-pre-wrap">{chunk}</span>
								</div>
							{/each}
							{#if streaming}
								<span class="text-gray-400 dark:text-gray-500 animate-pulse">Receiving...</span>
							{/if}
						{/if}
					</div>
				</div>
			{/if}
		</div>
	{:else if activeTab === 'async'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Async Invocation Tracking</h2>
			<p class="text-sm text-gray-500 dark:text-gray-400">Submit async inference requests and track inference IDs and output locations.</p>
			<div>
				<label for="async-endpoint" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Endpoint Name</label>
				<input id="async-endpoint" bind:value={asyncEndpoint} placeholder="my-async-endpoint" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm" />
			</div>
			<div>
				<label for="async-input" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Input Location (S3 URI)</label>
				<input id="async-input" bind:value={asyncInputLocation} placeholder="s3://my-bucket/input/request.json" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm font-mono" />
			</div>
			<div>
				<label for="async-body" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Request Body (JSON)</label>
				<textarea id="async-body" bind:value={asyncBody} rows={3} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none font-mono"></textarea>
			</div>
			<button onclick={invokeEndpointAsync} disabled={asyncInvoking} class="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
				<Activity class="w-4 h-4" /> {asyncInvoking ? 'Submitting...' : 'Submit Async Request'}
			</button>

			{#if asyncInvocations.length > 0}
				<div>
					<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-2">Submitted Invocations:</p>
					<div class="space-y-2">
						{#each asyncInvocations as inv}
							<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 space-y-1">
								<div class="flex items-center justify-between">
									<span class="flex items-center gap-2 text-sm font-medium text-gray-900 dark:text-white">
										{inv.endpointName}
										<RegionChip region={inv.region} />
									</span>
									<span class="text-xs text-gray-400 dark:text-gray-500">{inv.startedAt}</span>
								</div>
								<p class="text-xs text-gray-500 dark:text-gray-400">Inference ID: <span class="font-mono text-gray-700 dark:text-gray-300">{inv.inferenceId}</span></p>
								<p class="text-xs text-gray-500 dark:text-gray-400">Output: <span class="font-mono text-gray-700 dark:text-gray-300">{inv.outputLocation}</span></p>
							</div>
						{/each}
					</div>
				</div>
			{:else}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">No async invocations submitted yet</div>
			{/if}
		</div>
	{/if}
</div>
