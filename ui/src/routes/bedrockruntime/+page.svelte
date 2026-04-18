<script lang="ts">
	import { onMount } from 'svelte';
	import { getBedrockRuntimeClient } from '$lib/aws-client';
	import {
		InvokeModelCommand,
		ListAsyncInvokesCommand
	} from '@aws-sdk/client-bedrock-runtime';
	import { toast } from 'svelte-sonner';
	import { Zap, RefreshCw, Search, Send, Activity, MessageSquare } from 'lucide-svelte';

	interface AsyncInvokeSummary {
		invocationArn?: string;
		[key: string]: unknown;
	}

	const br = getBedrockRuntimeClient();

	let loading = $state(false);
	let activeTab = $state<'invoke' | 'asyncInvocations'>('invoke');
	let searchQuery = $state('');
	let modelId = $state('amazon.titan-text-express-v1');
	let prompt = $state('What is Amazon Web Services?');
	let modelResponse = $state<string | null>(null);
	let invoking = $state(false);
	let asyncInvocations = $state<AsyncInvokeSummary[]>([]);

	const supportedModels = [
		{ id: 'amazon.titan-text-express-v1', label: 'Titan Text Express' },
		{ id: 'amazon.titan-text-lite-v1', label: 'Titan Text Lite' },
		{ id: 'anthropic.claude-instant-v1', label: 'Claude Instant' },
		{ id: 'anthropic.claude-v2', label: 'Claude v2' },
		{ id: 'ai21.j2-ultra-v1', label: 'Jurassic-2 Ultra' },
		{ id: 'cohere.command-text-v14', label: 'Command' }
	];

	async function loadAsyncInvocations() {
		loading = true;
		try {
			const resp = await br.send(new ListAsyncInvokesCommand({}));
			asyncInvocations = (resp as { asyncInvokeSummaries?: AsyncInvokeSummary[] }).asyncInvokeSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load async invocations: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function invokeModel() {
		if (!prompt.trim()) {
			toast.error('Please enter a prompt');
			return;
		}
		invoking = true;
		modelResponse = null;
		try {
			const body = JSON.stringify({ inputText: prompt, textGenerationConfig: { maxTokenCount: 512, temperature: 0.7 } });
			const resp = await br.send(new InvokeModelCommand({ modelId, body: new TextEncoder().encode(body) }));
			const decoded = new TextDecoder().decode(resp.body);
			const parsed = JSON.parse(decoded);
			modelResponse = parsed.results?.[0]?.outputText ?? parsed.completion ?? decoded;
			toast.success('Model invoked successfully');
		} catch (e) {
			toast.error('Failed to invoke model: ' + String(e));
		} finally {
			invoking = false;
		}
	}

	onMount(loadAsyncInvocations);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Bedrock Runtime</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Run inference using foundation models</p>
			</div>
		</div>
		<button onclick={loadAsyncInvocations} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg"><Zap class="w-5 h-5 text-yellow-600 dark:text-yellow-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{supportedModels.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Supported Models</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{asyncInvocations.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Async Invocations</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><MessageSquare class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{modelResponse ? 1 : 0}</p><p class="text-sm text-gray-500 dark:text-gray-400">Responses</p></div>
		</div>
	</div>

	<div class="flex gap-2">
		{#each [['invoke', 'Invoke Model'], ['asyncInvocations', 'Async Invocations']] as [tab, label]}
			<button onclick={() => { activeTab = tab as typeof activeTab; }}
				class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-yellow-500 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
				{label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'invoke'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Invoke Foundation Model</h2>
			<div>
				<label for="br-model-id" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Model</label>
				<select id="br-model-id" bind:value={modelId} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm">
					{#each supportedModels as model}
						<option value={model.id}>{model.label} ({model.id})</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="br-prompt" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Prompt</label>
				<textarea id="br-prompt" bind:value={prompt} rows={4} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none"></textarea>
			</div>
			<button onclick={invokeModel} disabled={invoking} class="flex items-center gap-2 px-4 py-2 bg-yellow-500 hover:bg-yellow-600 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
				<Send class="w-4 h-4" /> {invoking ? 'Invoking...' : 'Invoke Model'}
			</button>
			{#if modelResponse}
				<div class="p-4 bg-gray-50 dark:bg-slate-700/50 rounded-lg">
					<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-2">Model Response:</p>
					<p class="text-sm text-gray-900 dark:text-white whitespace-pre-wrap">{modelResponse}</p>
				</div>
			{/if}
		</div>
	{:else if activeTab === 'asyncInvocations'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if asyncInvocations.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No async invocations found</div>
			{:else}
				<div class="space-y-2">
					{#each asyncInvocations as inv}
						<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
							<Activity class="w-5 h-5 text-blue-500" />
							<p class="text-sm text-gray-900 dark:text-white">{inv.invocationArn}</p>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>
