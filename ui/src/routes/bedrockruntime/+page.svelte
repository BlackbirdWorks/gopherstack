<script lang="ts">
	// Bedrock Runtime is a *data plane*: it invokes models and streams
	// responses. It has no resources to create, update or delete at all (its
	// 11 real operations are InvokeModel/Converse/CountTokens/ApplyGuardrail/
	// InvokeGuardrailChecks and the three async-invoke/three streaming
	// variants) -- so this page is NOT shaped as CRUD tables. Instead each tab
	// is one of the service's real actions:
	//   - Invoke Model: InvokeModel + CountTokens (estimate tokens for the
	//     same body before sending it)
	//   - Converse Playground: Converse (chat loop)
	//   - Guardrails: ApplyGuardrail and InvokeGuardrailChecks, the service's
	//     only two "evaluate this content" actions
	//   - Async Invocations: the one family that DOES have a create-like
	//     action (StartAsyncInvoke) plus a real list/detail (ListAsyncInvokes/
	//     GetAsyncInvoke) -- no update or delete exists for an async
	//     invocation once started.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getBedrockRuntimeClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		InvokeModelCommand,
		CountTokensCommand,
		ConverseCommand,
		ApplyGuardrailCommand,
		InvokeGuardrailChecksCommand,
		ListAsyncInvokesCommand,
		StartAsyncInvokeCommand,
		GetAsyncInvokeCommand,
		type AsyncInvokeSummary,
		type GetAsyncInvokeResponse,
		type GuardrailAssessment
	} from '@aws-sdk/client-bedrock-runtime';
	import { toast } from 'svelte-sonner';
	import { Zap, Send, Activity, MessageSquare, ShieldCheck, Plus, Eye, Hash } from 'lucide-svelte';

	interface ConverseTurn {
		role: 'user' | 'assistant';
		text: string;
	}

	const br = regionalClient(getBedrockRuntimeClient);

	type TabId = 'invoke' | 'converse' | 'guardrails' | 'asyncInvocations';

	const tabs: TabDef[] = [
		{ id: 'invoke', label: 'Invoke Model' },
		{ id: 'converse', label: 'Converse Playground' },
		{ id: 'guardrails', label: 'Guardrails' },
		{ id: 'asyncInvocations', label: 'Async Invocations' }
	];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}
	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	let activeTab = $state<TabId>('invoke');

	const supportedModels = [
		{ id: 'amazon.titan-text-express-v1', label: 'Titan Text Express' },
		{ id: 'amazon.titan-text-lite-v1', label: 'Titan Text Lite' },
		{ id: 'anthropic.claude-instant-v1', label: 'Claude Instant' },
		{ id: 'anthropic.claude-v2', label: 'Claude v2' },
		{ id: 'ai21.j2-ultra-v1', label: 'Jurassic-2 Ultra' },
		{ id: 'cohere.command-text-v14', label: 'Command' }
	];

	// --- Invoke Model + Count Tokens ---
	let modelId = $state('amazon.titan-text-express-v1');
	let prompt = $state('What is Amazon Web Services?');
	let modelResponse = $state<string | null>(null);
	let invoking = $state(false);
	let invokeError = $state<string | null>(null);
	let countingTokens = $state(false);
	let tokenCount = $state<number | null>(null);

	function invokeBody(): string {
		return JSON.stringify({ inputText: prompt, textGenerationConfig: { maxTokenCount: 512, temperature: 0.7 } });
	}

	async function invokeModel() {
		if (!prompt.trim()) {
			toast.error('Please enter a prompt');
			return;
		}
		invoking = true;
		invokeError = null;
		modelResponse = null;
		try {
			const resp = await br().send(new InvokeModelCommand({ modelId, body: new TextEncoder().encode(invokeBody()) }));
			const decoded = new TextDecoder().decode(resp.body);
			const parsed = JSON.parse(decoded);
			modelResponse = parsed.results?.[0]?.outputText ?? parsed.completion ?? decoded;
			toast.success('Model invoked successfully');
		} catch (e) {
			invokeError = describeError(e);
			toast.error('Failed to invoke model: ' + invokeError);
		} finally {
			invoking = false;
		}
	}

	async function countTokens() {
		if (!prompt.trim()) {
			toast.error('Please enter a prompt');
			return;
		}
		countingTokens = true;
		tokenCount = null;
		try {
			const resp = await br().send(
				new CountTokensCommand({
					modelId,
					input: { invokeModel: { body: new TextEncoder().encode(invokeBody()) } }
				})
			);
			tokenCount = resp.inputTokens ?? null;
		} catch (e) {
			toast.error('Failed to count tokens: ' + describeError(e));
		} finally {
			countingTokens = false;
		}
	}

	// --- Converse playground ---
	let converseModelId = $state('anthropic.claude-instant-v1');
	let converseInput = $state('');
	let converseTurns = $state<ConverseTurn[]>([]);
	let conversing = $state(false);

	const converseModels = [
		{ id: 'anthropic.claude-instant-v1', label: 'Claude Instant' },
		{ id: 'anthropic.claude-v2', label: 'Claude v2' },
		{ id: 'amazon.titan-text-express-v1', label: 'Titan Text Express' },
		{ id: 'amazon.titan-text-lite-v1', label: 'Titan Text Lite' }
	];

	async function sendConverseMessage() {
		const text = converseInput.trim();
		if (!text) {
			toast.error('Please enter a message');
			return;
		}
		converseTurns = [...converseTurns, { role: 'user', text }];
		converseInput = '';
		conversing = true;
		try {
			const messages = converseTurns.map((t) => ({ role: t.role, content: [{ text: t.text }] }));
			const resp = await br().send(new ConverseCommand({ modelId: converseModelId, messages }));
			const output = resp.output?.message?.content?.[0]?.text ?? '(no response)';
			converseTurns = [...converseTurns, { role: 'assistant', text: output }];
		} catch (e) {
			toast.error('Converse failed: ' + describeError(e));
			converseTurns = converseTurns.slice(0, -1);
			converseInput = text;
		} finally {
			conversing = false;
		}
	}

	function clearConversation() {
		converseTurns = [];
		converseInput = '';
	}

	// --- Guardrails: ApplyGuardrail + InvokeGuardrailChecks ---
	let guardrailIdentifier = $state('');
	let guardrailVersion = $state('DRAFT');
	let guardrailText = $state('');
	let applyingGuardrail = $state(false);
	let guardrailApplyError = $state<string | null>(null);
	let guardrailAction = $state<string | null>(null);
	let guardrailAssessments = $state<GuardrailAssessment[]>([]);

	async function submitApplyGuardrail() {
		if (!guardrailIdentifier || !guardrailVersion || !guardrailText.trim()) {
			guardrailApplyError = 'Guardrail identifier, version and text are all required.';
			return;
		}
		applyingGuardrail = true;
		guardrailApplyError = null;
		guardrailAction = null;
		guardrailAssessments = [];
		try {
			const resp = await br().send(
				new ApplyGuardrailCommand({
					guardrailIdentifier,
					guardrailVersion,
					source: 'INPUT',
					content: [{ text: { text: guardrailText } }]
				})
			);
			guardrailAction = resp.action ?? null;
			guardrailAssessments = resp.assessments ?? [];
			toast.success(`Guardrail action: ${resp.action}`);
		} catch (e) {
			guardrailApplyError = describeError(e);
			toast.error(guardrailApplyError);
		} finally {
			applyingGuardrail = false;
		}
	}

	// The only sensitive-information entity types this backend genuinely
	// detects (see services/bedrockruntime/PARITY.md) -- every other real
	// GuardrailChecksSensitiveInformationEntityType value is honestly never
	// matched, so offering it here would promise detection that never fires.
	const checkableEntityTypes = [
		'EMAIL',
		'PHONE',
		'IP_ADDRESS',
		'URL',
		'AWS_ACCESS_KEY',
		'MAC_ADDRESS',
		'US_SOCIAL_SECURITY_NUMBER',
		'CREDIT_DEBIT_CARD_NUMBER'
	] as const;

	let checksText = $state('');
	let checkingSensitiveInfo = $state(true);
	let runningChecks = $state(false);
	let checksError = $state<string | null>(null);
	let checksResults = $state<{ type?: string; confidenceScore?: number }[] | null>(null);

	async function submitInvokeGuardrailChecks() {
		if (!checksText.trim()) {
			checksError = 'Text to evaluate is required.';
			return;
		}
		runningChecks = true;
		checksError = null;
		checksResults = null;
		try {
			const resp = await br().send(
				new InvokeGuardrailChecksCommand({
					messages: [{ role: 'user', content: [{ text: checksText }] }],
					checks: checkingSensitiveInfo
						? { sensitiveInformation: { entities: checkableEntityTypes.map((type) => ({ type })) } }
						: {}
				})
			);
			checksResults = resp.results?.sensitiveInformation?.results ?? [];
		} catch (e) {
			checksError = describeError(e);
			toast.error(checksError);
		} finally {
			runningChecks = false;
		}
	}

	// --- Async Invocations: Start / List / Get detail ---
	let asyncInvocations = $state<AsyncInvokeSummary[]>([]);
	async function fetchAsyncInvocations(): Promise<void> {
		const resp = await br().send(new ListAsyncInvokesCommand({}));
		asyncInvocations = resp.asyncInvokeSummaries ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		invoke: () => Promise.resolve(),
		converse: () => Promise.resolve(),
		guardrails: () => Promise.resolve(),
		asyncInvocations: () => fetchAsyncInvocations().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		viewedInvocation = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	let createInvokeModal = $state<Modal | null>(null);
	let startingInvoke = $state(false);
	let startInvokeError = $state<string | null>(null);
	let newInvokeModelId = $state('amazon.titan-text-express-v1');
	let newInvokeS3Uri = $state('');
	let newInvokeModelInputJson = $state('{}');

	function openCreateInvokeModal(): void {
		startInvokeError = null;
		newInvokeModelId = 'amazon.titan-text-express-v1';
		newInvokeS3Uri = '';
		newInvokeModelInputJson = '{}';
		createInvokeModal?.open();
	}

	async function submitStartAsyncInvoke(): Promise<void> {
		if (!newInvokeModelId || !newInvokeS3Uri) {
			startInvokeError = 'Model ID and S3 output URI are required.';
			return;
		}
		let modelInput: Record<string, unknown>;
		try {
			modelInput = JSON.parse(newInvokeModelInputJson || '{}');
		} catch {
			startInvokeError = 'Model input must be valid JSON.';
			return;
		}
		startingInvoke = true;
		startInvokeError = null;
		try {
			await br().send(
				new StartAsyncInvokeCommand({
					modelId: newInvokeModelId,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					modelInput: modelInput as any,
					outputDataConfig: { s3OutputDataConfig: { s3Uri: newInvokeS3Uri } }
				})
			);
			toast.success('Async invocation started');
			createInvokeModal?.close();
			await tabLoader.refresh('asyncInvocations');
		} catch (e) {
			const msg = describeError(e);
			startInvokeError = msg;
			toast.error(msg);
		} finally {
			startingInvoke = false;
		}
	}

	let invocationDetailModal = $state<Modal | null>(null);
	let viewedInvocation = $state<GetAsyncInvokeResponse | null>(null);
	let invocationDetailLoading = $state(false);
	let invocationDetailError = $state<string | null>(null);

	async function openInvocationDetail(inv: AsyncInvokeSummary): Promise<void> {
		viewedInvocation = null;
		invocationDetailError = null;
		invocationDetailModal?.open();
		if (!inv.invocationArn) return;
		invocationDetailLoading = true;
		try {
			const resp = await br().send(new GetAsyncInvokeCommand({ invocationArn: inv.invocationArn }));
			viewedInvocation = resp;
		} catch (e) {
			invocationDetailError = describeError(e);
		} finally {
			invocationDetailLoading = false;
		}
	}

	function statusClass(s: unknown): string {
		if (s === 'Failed') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (s === 'Completed') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Zap}
		title="Amazon Bedrock Runtime"
		description="Run inference using foundation models"
		onRefresh={handleRefresh}
		color="amber"
	>
		{#snippet actions()}
			{#if activeTab === 'asyncInvocations'}
				<button onclick={openCreateInvokeModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Start async invocation
				</button>
			{/if}
		{/snippet}
	</PageHeader>

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
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{converseTurns.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Converse Turns</p></div>
		</div>
	</div>

	<Tabs {tabs} active={activeTab} onSelect={switchTab} color="amber" />

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
			<div class="flex items-center gap-2">
				<button onclick={invokeModel} disabled={invoking} class="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
					<Send class="w-4 h-4" /> {invoking ? 'Invoking...' : 'Invoke Model'}
				</button>
				<button onclick={countTokens} disabled={countingTokens} class="flex items-center gap-2 px-4 py-2 border border-gray-200 dark:border-gray-600 disabled:opacity-50 text-gray-700 dark:text-gray-200 rounded-lg text-sm font-medium hover:bg-gray-50 dark:hover:bg-slate-700">
					<Hash class="w-4 h-4" /> {countingTokens ? 'Counting...' : 'Count Tokens'}
				</button>
				{#if tokenCount !== null}
					<span class="text-sm text-gray-500 dark:text-gray-400">{tokenCount} input tokens</span>
				{/if}
			</div>
			{#if invokeError}
				<p class="text-sm text-red-600 dark:text-red-400">{invokeError}</p>
			{/if}
			{#if modelResponse}
				<div class="p-4 bg-gray-50 dark:bg-slate-700/50 rounded-lg">
					<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 mb-2">Model Response:</p>
					<p class="text-sm text-gray-900 dark:text-white whitespace-pre-wrap">{modelResponse}</p>
				</div>
			{/if}
		</div>
	{:else if activeTab === 'converse'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Converse Playground</h2>
				<button onclick={clearConversation} class="px-3 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
					Clear
				</button>
			</div>
			<div>
				<label for="converse-model-id" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Model</label>
				<select id="converse-model-id" bind:value={converseModelId} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm">
					{#each converseModels as model}
						<option value={model.id}>{model.label} ({model.id})</option>
					{/each}
				</select>
			</div>
			<div class="min-h-48 max-h-96 overflow-y-auto space-y-3 p-3 bg-gray-50 dark:bg-slate-900/50 rounded-lg">
				{#if converseTurns.length === 0}
					<p class="text-center text-sm text-gray-400 dark:text-gray-500 py-8">Start a conversation below</p>
				{:else}
					{#each converseTurns as turn}
						<div class="flex {turn.role === 'user' ? 'justify-end' : 'justify-start'}">
							<div class="max-w-[80%] px-4 py-2 rounded-2xl text-sm whitespace-pre-wrap
								{turn.role === 'user'
									? 'bg-amber-600 text-white rounded-br-sm'
									: 'bg-white dark:bg-slate-700 text-gray-900 dark:text-white border border-gray-200 dark:border-slate-600 rounded-bl-sm'}">
								{turn.text}
							</div>
						</div>
					{/each}
					{#if conversing}
						<div class="flex justify-start">
							<div class="px-4 py-2 rounded-2xl rounded-bl-sm bg-white dark:bg-slate-700 border border-gray-200 dark:border-slate-600 text-sm text-gray-400 dark:text-gray-500">
								Thinking...
							</div>
						</div>
					{/if}
				{/if}
			</div>
			<div class="flex gap-2">
				<textarea
					bind:value={converseInput}
					onkeydown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendConverseMessage(); } }}
					rows={2}
					placeholder="Type a message… (Enter to send, Shift+Enter for newline)"
					class="flex-1 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none"
				></textarea>
				<button onclick={sendConverseMessage} disabled={conversing || !converseInput.trim()} class="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium self-end">
					<Send class="w-4 h-4" /> Send
				</button>
			</div>
		</div>
	{:else if activeTab === 'guardrails'}
		<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2"><ShieldCheck class="w-5 h-5 text-amber-500" /> Apply Guardrail</h2>
				<div>
					<label for="gr-id" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Guardrail identifier</label>
					<input id="gr-id" bind:value={guardrailIdentifier} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm" />
				</div>
				<div>
					<label for="gr-version" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Guardrail version</label>
					<input id="gr-version" bind:value={guardrailVersion} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm" />
				</div>
				<div>
					<label for="gr-text" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Content</label>
					<textarea id="gr-text" bind:value={guardrailText} rows={3} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"></textarea>
				</div>
				<button onclick={submitApplyGuardrail} disabled={applyingGuardrail} class="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
					<ShieldCheck class="w-4 h-4" /> {applyingGuardrail ? 'Applying...' : 'Apply Guardrail'}
				</button>
				{#if guardrailApplyError}
					<p class="text-sm text-red-600 dark:text-red-400">{guardrailApplyError}</p>
				{/if}
				{#if guardrailAction}
					<div class="p-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg text-sm">
						<p class="font-medium text-gray-900 dark:text-white">Action: {guardrailAction}</p>
						{#if guardrailAssessments.length > 0}
							<ul class="mt-2 space-y-1 text-xs text-gray-600 dark:text-gray-300">
								{#each guardrailAssessments as a, i (i)}
									<li>{JSON.stringify(a.wordPolicy ?? a)}</li>
								{/each}
							</ul>
						{/if}
					</div>
				{/if}
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2"><ShieldCheck class="w-5 h-5 text-amber-500" /> Invoke Guardrail Checks</h2>
				<div>
					<label for="checks-text" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Message text</label>
					<textarea id="checks-text" bind:value={checksText} rows={3} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"></textarea>
				</div>
				<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
					<input type="checkbox" bind:checked={checkingSensitiveInfo} /> Check for sensitive information (email, phone, IP, URL, AWS key, MAC address, SSN, card number)
				</label>
				<button onclick={submitInvokeGuardrailChecks} disabled={runningChecks} class="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
					<ShieldCheck class="w-4 h-4" /> {runningChecks ? 'Checking...' : 'Run Checks'}
				</button>
				{#if checksError}
					<p class="text-sm text-red-600 dark:text-red-400">{checksError}</p>
				{/if}
				{#if checksResults}
					{#if checksResults.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No sensitive information detected</p>
					{:else}
						<ul class="space-y-1">
							{#each checksResults as r, i (i)}
								<li class="text-sm px-2 py-1 rounded bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300">{r.type} (confidence {r.confidenceScore})</li>
							{/each}
						</ul>
					{/if}
				{/if}
			</div>
		</div>
	{:else if activeTab === 'asyncInvocations'}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			{#if activeTabError}
				<div role="alert" class="mb-4 rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}
			{#snippet invocationStatusCell(inv: AsyncInvokeSummary)}
				<span class="text-xs px-2 py-1 rounded-full {statusClass(inv.status)}">{inv.status ?? '—'}</span>
			{/snippet}
			{#snippet invocationActionsCell(inv: AsyncInvokeSummary)}
				<div class="flex items-center gap-2 justify-end">
					<button onclick={() => openInvocationDetail(inv)} title="View" aria-label="View invocation {inv.invocationArn}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
				</div>
			{/snippet}
			<DataTable
				rows={asyncInvocations}
				rowKey={(inv) => inv.invocationArn ?? ''}
				columns={defineColumns<AsyncInvokeSummary>([
					{ key: 'invocationArn', label: 'Invocation ARN' },
					{ key: 'modelArn', label: 'Model' },
					{ key: 'status', label: 'Status', render: invocationStatusCell },
					{ key: 'actions', label: '', render: invocationActionsCell }
				])}
				loading={tabLoader.isLoading('asyncInvocations')}
				emptyMessage="No async invocations found"
			/>
		</div>
	{/if}
</div>

<!-- Start Async Invocation -->
<Modal bind:this={createInvokeModal} title="Start Async Invocation">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ai-model-id" class="text-sm text-slate-600 dark:text-slate-300">Model ID</label>
				<input id="ai-model-id" bind:value={newInvokeModelId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ai-s3-uri" class="text-sm text-slate-600 dark:text-slate-300">S3 output URI</label>
				<input id="ai-s3-uri" bind:value={newInvokeS3Uri} placeholder="s3://my-bucket/output/" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ai-model-input" class="text-sm text-slate-600 dark:text-slate-300">Model input (JSON)</label>
				<textarea id="ai-model-input" bind:value={newInvokeModelInputJson} rows={4} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if startInvokeError}
				<p class="text-sm text-red-600 dark:text-red-400">{startInvokeError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createInvokeModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStartAsyncInvoke} disabled={startingInvoke} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{startingInvoke ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<!-- Async Invocation detail -->
<Modal bind:this={invocationDetailModal} title="Async Invocation">
	{#snippet children()}
		{#if invocationDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if invocationDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{invocationDetailError}</p>
		{:else if viewedInvocation}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Invocation ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedInvocation.invocationArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Model ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedInvocation.modelArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedInvocation.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Submitted</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedInvocation.submitTime)}</dd></div>
				{#if viewedInvocation.failureMessage}
					<div><dt class="text-slate-500 dark:text-slate-400">Failure message</dt><dd class="text-red-600 dark:text-red-400">{viewedInvocation.failureMessage}</dd></div>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => invocationDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
