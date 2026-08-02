<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getBedrockClient, getBedrockRuntimeClient } from '$lib/aws-client';
	import {
		ListFoundationModelsCommand,
		ListCustomModelsCommand,
		GetFoundationModelCommand,
		GetCustomModelCommand,
		CreateCustomModelCommand,
		ListGuardrailsCommand,
		CreateGuardrailCommand,
		DeleteGuardrailCommand,
		type FoundationModelSummary,
		type CustomModelSummary,
		type GuardrailSummary,
		type GetCustomModelResponse
	} from '@aws-sdk/client-bedrock';
	import { InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime';
	import { toast } from 'svelte-sonner';
	import {
		Brain,
		Search,
		RefreshCw,
		Eye,
		Sparkles,
		Cpu,
		CheckCircle,
		XCircle,
		Shield,
		Plus,
		Trash2,
		MessageSquare,
		Play
	} from 'lucide-svelte';

	const bedrock = regionalClient(getBedrockClient);
	const runtime = regionalClient(getBedrockRuntimeClient);

	let loading = $state(false);
	let activeTab = $state<'foundation' | 'custom' | 'guardrails' | 'playground'>('foundation');

	// ── Model invoke / test playground ──────────────────────────────────────
	const SAMPLE_PROMPTS = [
		'Explain quantum entanglement in one sentence.',
		'Write a haiku about cloud computing.',
		'List three benefits of infrastructure-as-code.'
	];
	let playgroundModelId = $state('');
	let playgroundPrompt = $state(SAMPLE_PROMPTS[0]);
	let playgroundMaxTokens = $state(512);
	let playgroundTemperature = $state(0.7);
	let invoking = $state(false);
	let playgroundOutput = $state('');
	let playgroundRaw = $state('');

	// Build a provider-appropriate request body for InvokeModel.
	function buildBody(modelId: string): string {
		const id = modelId.toLowerCase();
		if (id.includes('anthropic') || id.includes('claude')) {
			return JSON.stringify({
				anthropic_version: 'bedrock-2023-05-31',
				max_tokens: playgroundMaxTokens,
				temperature: playgroundTemperature,
				messages: [{ role: 'user', content: playgroundPrompt }]
			});
		}
		if (id.includes('titan') || id.includes('nova') || id.includes('amazon')) {
			return JSON.stringify({
				inputText: playgroundPrompt,
				textGenerationConfig: { maxTokenCount: playgroundMaxTokens, temperature: playgroundTemperature }
			});
		}
		if (id.includes('llama') || id.includes('meta')) {
			return JSON.stringify({
				prompt: playgroundPrompt,
				max_gen_len: playgroundMaxTokens,
				temperature: playgroundTemperature
			});
		}
		// Cohere / Mistral / generic fallback.
		return JSON.stringify({
			prompt: playgroundPrompt,
			max_tokens: playgroundMaxTokens,
			temperature: playgroundTemperature
		});
	}

	// Extract text from any of the common Bedrock response shapes.
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function extractText(parsed: any): string {
		if (!parsed) return '';
		if (Array.isArray(parsed.content)) return parsed.content.map((c: { text?: string }) => c.text ?? '').join('');
		if (parsed.completion) return parsed.completion;
		if (Array.isArray(parsed.results)) return parsed.results.map((r: { outputText?: string }) => r.outputText ?? '').join('');
		if (parsed.generation) return parsed.generation;
		if (Array.isArray(parsed.generations)) return parsed.generations.map((g: { text?: string }) => g.text ?? '').join('');
		if (Array.isArray(parsed.outputs)) return parsed.outputs.map((o: { text?: string }) => o.text ?? '').join('');
		if (parsed.outputText) return parsed.outputText;
		return '';
	}

	async function invokeModel() {
		if (!playgroundModelId.trim()) {
			toast.error('Select or enter a model ID');
			return;
		}
		if (!playgroundPrompt.trim()) {
			toast.error('Enter a prompt');
			return;
		}
		invoking = true;
		playgroundOutput = '';
		playgroundRaw = '';
		try {
			const resp = await runtime().send(
				new InvokeModelCommand({
					modelId: playgroundModelId.trim(),
					contentType: 'application/json',
					accept: 'application/json',
					body: new TextEncoder().encode(buildBody(playgroundModelId.trim()))
				})
			);
			const text = new TextDecoder().decode(resp.body);
			playgroundRaw = text;
			try {
				playgroundOutput = extractText(JSON.parse(text)) || text;
			} catch {
				playgroundOutput = text;
			}
			toast.success('Model invoked');
		} catch (e) {
			toast.error(`Failed to invoke model: ${e}`);
		} finally {
			invoking = false;
		}
	}
	let searchQuery = $state('');
	let modalityFilter = $state('all');
	let providerFilter = $state('all');

	// Foundation Models
	let foundationModels = $state<FoundationModelSummary[]>([]);
	let selectedModel = $state<FoundationModelSummary | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let modelDetail = $state<any | null>(null);
	let loadingDetail = $state(false);

	// Custom Models
	let customModels = $state<CustomModelSummary[]>([]);
	let selectedCustomModel = $state<CustomModelSummary | null>(null);
	let customModelDetail = $state<GetCustomModelResponse | null>(null);
	let loadingCustomDetail = $state(false);
	let showCreateCustomModel = $state(false);
	let newCustomModelName = $state('');
	let newCustomModelBase = $state('');
	let creatingCustomModel = $state(false);

	// Guardrails
	let guardrails = $state<GuardrailSummary[]>([]);
	let showCreateGuardrail = $state(false);
	let newGuardrailName = $state('');
	let newGuardrailDesc = $state('');
	let newGuardrailBlockedInput = $state('Sorry, I cannot assist with that request.');
	let newGuardrailBlockedOutput = $state('The model response was blocked.');
	let creatingGuardrail = $state(false);

	const providers = $derived([...new Set(foundationModels.map((m) => m.providerName ?? 'Unknown'))].toSorted());
	const modalities = $derived([...new Set(foundationModels.flatMap((m) => m.inputModalities ?? []))].toSorted());

	const filteredFoundation = $derived(
		foundationModels.filter((m) => {
			const nameMatch =
				(m.modelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.providerName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.modelId ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const providerMatch = providerFilter === 'all' || m.providerName === providerFilter;
			const modalityMatch =
				modalityFilter === 'all' ||
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				((m.inputModalities as any[]) ?? []).includes(modalityFilter);
			return nameMatch && providerMatch && modalityMatch;
		})
	);

	const filteredCustom = $derived(
		customModels.filter(
			(m) =>
				(m.modelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.baseModelArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredGuardrails = $derived(
		guardrails.filter((g) =>
			(g.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function statusBadge(status?: string) {
		if (status === 'ACTIVE' || status === 'READY') {
			return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		}
		if (status === 'LEGACY') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadFoundationModels() {
		loading = true;
		try {
			const res = await bedrock().send(new ListFoundationModelsCommand({}));
			foundationModels = res.modelSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load foundation models: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadCustomModels() {
		loading = true;
		try {
			const res = await bedrock().send(new ListCustomModelsCommand({ maxResults: 100 }));
			customModels = res.modelSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load custom models: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadGuardrails() {
		loading = true;
		try {
			const res = await bedrock().send(new ListGuardrailsCommand({}));
			guardrails = (res.guardrails ?? []) as GuardrailSummary[];
		} catch (e) {
			toast.error(`Failed to load guardrails: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewModelDetail(model: FoundationModelSummary) {
		selectedModel = model;
		if (!model.modelArn) return;
		loadingDetail = true;
		modelDetail = null;
		try {
			const res = await bedrock().send(
				new GetFoundationModelCommand({ modelIdentifier: model.modelId ?? '' })
			);
			modelDetail = res.modelDetails ?? null;
		} catch (e) {
			toast.error(`Failed to load model details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function selectCustomModel(model: CustomModelSummary) {
		selectedCustomModel = model;
		customModelDetail = null;
		if (!model.modelArn) return;
		loadingCustomDetail = true;
		try {
			const res = await bedrock().send(new GetCustomModelCommand({ modelIdentifier: model.modelArn }));
			customModelDetail = res as GetCustomModelResponse;
		} catch (e) {
			toast.error(`Failed to load custom model details: ${e}`);
		} finally {
			loadingCustomDetail = false;
		}
	}

	async function createCustomModel() {
		if (!newCustomModelName.trim()) {
			toast.error('Model name is required');
			return;
		}
		creatingCustomModel = true;
		try {
			await bedrock().send(
				new CreateCustomModelCommand({
					modelName: newCustomModelName.trim(),
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					modelSourceConfig: { s3DataSource: { s3Uri: newCustomModelBase.trim() } } as any
				})
			);
			toast.success(`Custom model "${newCustomModelName}" created`);
			newCustomModelName = '';
			newCustomModelBase = '';
			showCreateCustomModel = false;
			await loadCustomModels();
		} catch (e) {
			toast.error(`Failed to create custom model: ${e}`);
		} finally {
			creatingCustomModel = false;
		}
	}

	async function createGuardrail() {
		if (!newGuardrailName.trim()) {
			toast.error('Guardrail name is required');
			return;
		}
		creatingGuardrail = true;
		try {
			await bedrock().send(
				new CreateGuardrailCommand({
					name: newGuardrailName.trim(),
					description: newGuardrailDesc.trim() || undefined,
					blockedInputMessaging: newGuardrailBlockedInput,
					blockedOutputsMessaging: newGuardrailBlockedOutput
				})
			);
			toast.success(`Guardrail "${newGuardrailName}" created`);
			newGuardrailName = '';
			newGuardrailDesc = '';
			showCreateGuardrail = false;
			await loadGuardrails();
		} catch (e) {
			toast.error(`Failed to create guardrail: ${e}`);
		} finally {
			creatingGuardrail = false;
		}
	}

	async function deleteGuardrail(id: string, name: string) {
		try {
			await bedrock().send(new DeleteGuardrailCommand({ guardrailIdentifier: id }));
			toast.success(`Guardrail "${name}" deleted`);
			await loadGuardrails();
		} catch (e) {
			toast.error(`Failed to delete guardrail: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedModel = null;
		selectedCustomModel = null;
		customModelDetail = null;
		if (tab === 'foundation') await loadFoundationModels();
		else if (tab === 'custom') await loadCustomModels();
		else if (tab === 'guardrails') await loadGuardrails();
		else if (tab === 'playground' && foundationModels.length === 0) await loadFoundationModels();
	}

	// Custom models and guardrails are only unique within a region, so a
	// selection (and its detail state) from the old region must not survive
	// a region switch. Reuse onTabChange to clear that selection and reload
	// whichever tab is active; `activeTab` is read with `untrack` since a tab
	// switch already reloads directly, so letting the effect also depend on
	// it would double-fetch on every subsequent region change.
	onRegionChange(() => {
		untrack(() => onTabChange(activeTab));
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Brain class="h-8 w-8 text-violet-600" />
			<div>
				<h1 class="text-2xl font-bold">Amazon Bedrock</h1>
				<p class="text-sm text-muted-foreground">
					Foundation models and generative AI capabilities
				</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Summary Cards -->
	{#if foundationModels.length > 0 || customModels.length > 0}
		<div class="grid gap-4 sm:grid-cols-4">
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{foundationModels.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Foundation Models</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{providers.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Providers</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{customModels.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Custom Models</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{guardrails.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Guardrails</div>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [
			{ id: 'foundation', label: 'Foundation Models', icon: Sparkles },
			{ id: 'custom', label: 'Custom Models', icon: Cpu },
			{ id: 'guardrails', label: 'Guardrails', icon: Shield },
			{ id: 'playground', label: 'Playground', icon: MessageSquare }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Foundation Models Tab -->
	{#if activeTab === 'foundation'}
		<div class="flex flex-wrap gap-3">
			<div class="relative flex-1 min-w-[200px]">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search models..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={providerFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Providers</option>
				{#each providers as provider}
					<option value={provider}>{provider}</option>
				{/each}
			</select>
			<select
				bind:value={modalityFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Modalities</option>
				{#each modalities as modality}
					<option value={modality}>{modality}</option>
				{/each}
			</select>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredFoundation.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Brain class="h-12 w-12 mb-3 opacity-30" />
				<p>No foundation models found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Model Name</th>
							<th class="px-4 py-3 text-left font-medium">Provider</th>
							<th class="px-4 py-3 text-left font-medium">Input Modalities</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-right font-medium">Details</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredFoundation as model}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewModelDetail(model)}
							>
								<td class="px-4 py-3">
									<div class="font-medium">{model.modelName}</div>
									<div class="text-xs text-muted-foreground font-mono">{model.modelId}</div>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{model.providerName}</td>
								<td class="px-4 py-3">
									<div class="flex flex-wrap gap-1">
										{#each model.inputModalities ?? [] as modality}
											<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{modality}</span>
										{/each}
									</div>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(model.modelLifecycle?.status)}">
										{model.modelLifecycle?.status ?? 'ACTIVE'}
									</span>
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); viewModelDetail(model); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
									>
										<Eye class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Model Details -->
			{#if selectedModel}
				<div class="rounded-lg border p-5 space-y-4">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-2">
							<Sparkles class="h-5 w-5 text-violet-500" />
							<h3 class="font-semibold">{selectedModel.modelName}</h3>
						</div>
						<button onclick={() => (selectedModel = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					<div class="grid grid-cols-2 gap-3 text-sm">
						<div>
							<p class="text-muted-foreground">Model ID</p>
							<p class="font-mono text-xs">{selectedModel.modelId}</p>
						</div>
						<div>
							<p class="text-muted-foreground">Provider</p>
							<p>{selectedModel.providerName}</p>
						</div>
						<div>
							<p class="text-muted-foreground">Customizations</p>
							<div class="flex flex-wrap gap-1 mt-1">
								{#each selectedModel.customizationsSupported ?? [] as c}
									<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{c}</span>
								{:else}
									<span class="text-muted-foreground text-xs">None</span>
								{/each}
							</div>
						</div>
						<div>
							<p class="text-muted-foreground">Inference Types</p>
							<div class="flex flex-wrap gap-1 mt-1">
								{#each selectedModel.inferenceTypesSupported ?? [] as t}
									<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{t}</span>
								{:else}
									<span class="text-muted-foreground text-xs">None</span>
								{/each}
							</div>
						</div>
					</div>
					<div class="flex items-center gap-4 text-sm">
						<div class="flex items-center gap-1">
							{#if selectedModel.responseStreamingSupported}
								<CheckCircle class="h-4 w-4 text-green-500" />
								<span>Streaming supported</span>
							{:else}
								<XCircle class="h-4 w-4 text-muted-foreground" />
								<span class="text-muted-foreground">No streaming</span>
							{/if}
						</div>
					</div>
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Custom Models Tab -->
	{#if activeTab === 'custom'}
		<div class="flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search custom models..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateCustomModel = !showCreateCustomModel)}
				class="flex items-center gap-2 rounded-md bg-primary text-primary-foreground px-3 py-2 text-sm hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Model
			</button>
		</div>

		{#if showCreateCustomModel}
			<div class="rounded-lg border p-5 space-y-4 bg-muted/20">
				<h3 class="font-semibold flex items-center gap-2">
					<Cpu class="h-4 w-4" />
					Create Custom Model
				</h3>
				<div class="grid gap-3">
					<div>
						<label class="text-sm font-medium" for="custom-model-name">Model Name *</label>
						<input
							id="custom-model-name"
							type="text"
							placeholder="my-fine-tuned-model"
							bind:value={newCustomModelName}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
					<div>
						<label class="text-sm font-medium" for="custom-model-base">Base Model ID (optional)</label>
						<input
							id="custom-model-base"
							type="text"
							placeholder="anthropic.claude-v2"
							bind:value={newCustomModelBase}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				</div>
				<div class="flex gap-2">
					<button
						onclick={createCustomModel}
						disabled={creatingCustomModel}
						class="flex items-center gap-2 rounded-md bg-primary text-primary-foreground px-4 py-2 text-sm hover:bg-primary/90 disabled:opacity-50"
					>
						{#if creatingCustomModel}
							<RefreshCw class="h-4 w-4 animate-spin" />
						{:else}
							<Plus class="h-4 w-4" />
						{/if}
						Create
					</button>
					<button
						onclick={() => (showCreateCustomModel = false)}
						class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
					>
						Cancel
					</button>
				</div>
			</div>
		{/if}

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredCustom.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Cpu class="h-12 w-12 mb-3 opacity-30" />
				<p>No custom models found</p>
				<p class="text-sm">Fine-tune a foundation model to create a custom model</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Model Name</th>
							<th class="px-4 py-3 text-left font-medium">Base Model</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredCustom as model}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => selectCustomModel(model)}
							>
								<td class="px-4 py-3 font-medium">{model.modelName}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs font-mono">{model.baseModelName ?? model.baseModelArn}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{model.customizationType ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{model.creationTime ? new Date(model.creationTime).toLocaleDateString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Custom Model Detail -->
			{#if selectedCustomModel}
				<div class="rounded-lg border p-5 space-y-4">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-2">
							<Cpu class="h-5 w-5 text-violet-500" />
							<h3 class="font-semibold">{selectedCustomModel.modelName}</h3>
						</div>
						<button onclick={() => { selectedCustomModel = null; customModelDetail = null; }} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingCustomDetail}
						<div class="flex justify-center py-6">
							<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
						</div>
					{:else if customModelDetail}
						<div class="grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
							<div>
								<p class="text-muted-foreground">Status</p>
								<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(customModelDetail.modelStatus)}">
									{customModelDetail.modelStatus ?? '—'}
								</span>
							</div>
							<div>
								<p class="text-muted-foreground">Customization Type</p>
								<p>{customModelDetail.customizationType ?? '—'}</p>
							</div>
							<div>
								<p class="text-muted-foreground">Created</p>
								<p>{customModelDetail.creationTime ? new Date(customModelDetail.creationTime).toLocaleDateString() : '—'}</p>
							</div>
							<div class="col-span-2 sm:col-span-3">
								<p class="text-muted-foreground">Base Model ARN</p>
								<p class="font-mono text-xs break-all">{customModelDetail.baseModelArn ?? '—'}</p>
							</div>
							{#if customModelDetail.jobArn}
								<div class="col-span-2 sm:col-span-3">
									<p class="text-muted-foreground">Job ARN</p>
									<p class="font-mono text-xs break-all">{customModelDetail.jobArn}</p>
								</div>
							{/if}
							{#if customModelDetail.failureMessage}
								<div class="col-span-2 sm:col-span-3">
									<p class="text-muted-foreground">Failure Reason</p>
									<p class="text-red-600 dark:text-red-400 text-xs">{customModelDetail.failureMessage}</p>
								</div>
							{/if}
						</div>
						{#if customModelDetail.trainingMetrics}
							<div>
								<p class="text-sm font-medium mb-2">Training Metrics</p>
								<div class="rounded-md bg-muted/30 px-4 py-3 text-sm">
									Training Loss: <span class="font-mono">{customModelDetail.trainingMetrics.trainingLoss?.toFixed(4) ?? '—'}</span>
								</div>
							</div>
						{/if}
						{#if customModelDetail.hyperParameters && Object.keys(customModelDetail.hyperParameters).length > 0}
							<div>
								<p class="text-sm font-medium mb-2">Hyperparameters</p>
								<div class="rounded-lg border overflow-hidden">
									<table class="w-full text-sm">
										<thead class="bg-muted/50">
											<tr>
												<th class="px-3 py-2 text-left font-medium">Key</th>
												<th class="px-3 py-2 text-left font-medium">Value</th>
											</tr>
										</thead>
										<tbody class="divide-y">
											{#each Object.entries(customModelDetail.hyperParameters) as [k, v]}
												<tr>
													<td class="px-3 py-2 font-mono text-xs">{k}</td>
													<td class="px-3 py-2 font-mono text-xs">{v}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							</div>
						{/if}
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Guardrails Tab -->
	{#if activeTab === 'guardrails'}
		<div class="flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search guardrails..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateGuardrail = !showCreateGuardrail)}
				class="flex items-center gap-2 rounded-md bg-primary text-primary-foreground px-3 py-2 text-sm hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Guardrail
			</button>
		</div>

		{#if showCreateGuardrail}
			<div class="rounded-lg border p-5 space-y-4 bg-muted/20">
				<h3 class="font-semibold flex items-center gap-2">
					<Shield class="h-4 w-4" />
					Create Guardrail
				</h3>
				<div class="grid gap-3">
					<div>
						<label class="text-sm font-medium" for="guardrail-name">Name *</label>
						<input
							id="guardrail-name"
							type="text"
							placeholder="my-guardrail"
							bind:value={newGuardrailName}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
					<div>
						<label class="text-sm font-medium" for="guardrail-desc">Description</label>
						<input
							id="guardrail-desc"
							type="text"
							placeholder="Optional description"
							bind:value={newGuardrailDesc}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
					<div>
						<label class="text-sm font-medium" for="guardrail-blocked-input">Blocked Input Message</label>
						<input
							id="guardrail-blocked-input"
							type="text"
							bind:value={newGuardrailBlockedInput}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
					<div>
						<label class="text-sm font-medium" for="guardrail-blocked-output">Blocked Output Message</label>
						<input
							id="guardrail-blocked-output"
							type="text"
							bind:value={newGuardrailBlockedOutput}
							class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				</div>
				<div class="flex gap-2">
					<button
						onclick={createGuardrail}
						disabled={creatingGuardrail}
						class="flex items-center gap-2 rounded-md bg-primary text-primary-foreground px-4 py-2 text-sm hover:bg-primary/90 disabled:opacity-50"
					>
						{#if creatingGuardrail}
							<RefreshCw class="h-4 w-4 animate-spin" />
						{:else}
							<Plus class="h-4 w-4" />
						{/if}
						Create
					</button>
					<button
						onclick={() => (showCreateGuardrail = false)}
						class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
					>
						Cancel
					</button>
				</div>
			</div>
		{/if}

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredGuardrails.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No guardrails found</p>
				<p class="text-sm">Create a guardrail to control model inputs and outputs</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Version</th>
							<th class="px-4 py-3 text-left font-medium">Updated</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredGuardrails as g}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3">
									<div class="font-medium">{g.name}</div>
									<div class="text-xs text-muted-foreground font-mono">{g.id ?? ''}</div>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(g.status)}">
										{g.status ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{g.version ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{g.updatedAt ? new Date(g.updatedAt).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteGuardrail(g.id ?? '', g.name ?? '')}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete guardrail"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

		<!-- Playground Tab -->
		{#if activeTab === 'playground'}
			<div class="grid gap-4 lg:grid-cols-2">
				<div class="rounded-lg border p-5 space-y-4">
					<h3 class="font-semibold flex items-center gap-2"><MessageSquare class="h-4 w-4 text-violet-500" /> Invoke Model</h3>
					<div>
						<label class="text-sm font-medium" for="pg-model">Model ID</label>
						{#if foundationModels.length > 0}
							<select id="pg-model" bind:value={playgroundModelId} class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
								<option value="">Select a model…</option>
								{#each foundationModels as m}
									<option value={m.modelId}>{m.modelName} ({m.modelId})</option>
								{/each}
							</select>
						{:else}
							<input id="pg-model" type="text" bind:value={playgroundModelId} placeholder="anthropic.claude-v2" class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
						{/if}
					</div>
					<div>
						<div class="flex items-center justify-between">
							<label class="text-sm font-medium" for="pg-prompt">Prompt</label>
							<div class="flex gap-1">
								{#each SAMPLE_PROMPTS as p, i}
									<button type="button" onclick={() => (playgroundPrompt = p)} class="rounded bg-muted px-1.5 py-0.5 text-xs hover:bg-accent" title={p}>Sample {i + 1}</button>
								{/each}
							</div>
						</div>
						<textarea id="pg-prompt" bind:value={playgroundPrompt} rows="5" class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
					</div>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="text-sm font-medium" for="pg-max">Max tokens</label>
							<input id="pg-max" type="number" min="1" max="4096" bind:value={playgroundMaxTokens} class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
						</div>
						<div>
							<label class="text-sm font-medium" for="pg-temp">Temperature</label>
							<input id="pg-temp" type="number" min="0" max="1" step="0.1" bind:value={playgroundTemperature} class="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
						</div>
					</div>
					<button onclick={invokeModel} disabled={invoking} class="flex items-center gap-2 rounded-md bg-primary text-primary-foreground px-4 py-2 text-sm hover:bg-primary/90 disabled:opacity-50">
						{#if invoking}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Play class="h-4 w-4" />{/if}
						Invoke
					</button>
				</div>
				<div class="rounded-lg border p-5 space-y-3">
					<h3 class="font-semibold flex items-center gap-2"><Sparkles class="h-4 w-4 text-violet-500" /> Response</h3>
					{#if playgroundOutput}
						<div class="rounded-md bg-muted/40 p-3 text-sm whitespace-pre-wrap">{playgroundOutput}</div>
						{#if playgroundRaw}
							<details class="text-xs">
								<summary class="cursor-pointer text-muted-foreground">Raw response JSON</summary>
								<pre class="mt-2 overflow-x-auto rounded-md bg-muted/40 p-3 font-mono">{playgroundRaw}</pre>
							</details>
						{/if}
					{:else}
						<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
							<MessageSquare class="h-12 w-12 mb-3 opacity-30" />
							<p class="text-sm">Invoke a model to see the response</p>
						</div>
					{/if}
				</div>
			</div>
		{/if}
</div>
