<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSupportClient } from '$lib/aws-client';
	import {
		DescribeCasesCommand,
		DescribeSeverityLevelsCommand,
		DescribeServicesCommand,
		DescribeCommunicationsCommand,
		CreateCaseCommand,
		AddCommunicationToCaseCommand,
		type CaseDetails,
		type SeverityLevel,
		type Service,
		type Communication
	} from '@aws-sdk/client-support';
	import { toast } from 'svelte-sonner';
	import {
		LifeBuoy,
		RefreshCw,
		Search,
		AlertTriangle,
		CheckCircle,
		Clock,
		MessageSquare,
		Plus,
		X,
		Send,
		ChevronDown,
		ChevronUp
	} from 'lucide-svelte';

	const support = regionalClient(getSupportClient);

	let loading = $state(false);
	let activeTab = $state<'cases' | 'services' | 'create'>('cases');
	let searchQuery = $state('');
	let cases = $state<CaseDetails[]>([]);
	let severityLevels = $state<SeverityLevel[]>([]);
	let services = $state<Service[]>([]);
	let includedResolved = $state(false);

	// Case detail / thread viewer
	let selectedCase = $state<CaseDetails | null>(null);
	let communications = $state<Communication[]>([]);
	let loadingComms = $state(false);
	let expandedThread = $state(false);
	let replyBody = $state('');
	let sendingReply = $state(false);

	// Create case form
	let createSubject = $state('');
	let createBody = $state('');
	let createServiceCode = $state('');
	let createCategoryCode = $state('');
	let createSeverityCode = $state('');
	let createLanguage = $state('en');
	let creating = $state(false);

	const filteredCases = $derived(
		cases.filter((c) =>
			((c.subject ?? '') + (c.caseId ?? '')).toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredServices = $derived(
		services.filter((s) => (s.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const openCases = $derived(cases.filter((c) => c.status !== 'resolved').length);
	const resolvedCases = $derived(cases.filter((c) => c.status === 'resolved').length);

	const selectedServiceCategories = $derived(
		services.find((s) => s.code === createServiceCode)?.categories ?? []
	);

	async function loadData() {
		loading = true;
		// `includedResolved` is read with `untrack` so it never becomes a
		// dependency of the `onRegionChange` effect below -- the "include
		// resolved" checkbox's onchange already calls loadData directly.
		const currentIncludedResolved = untrack(() => includedResolved);
		try {
			const [caseResp, sevResp, svcResp] = await Promise.all([
				support().send(new DescribeCasesCommand({ includeResolvedCases: currentIncludedResolved })),
				support().send(new DescribeSeverityLevelsCommand({})),
				support().send(new DescribeServicesCommand({}))
			]);
			cases = caseResp.cases ?? [];
			severityLevels = sevResp.severityLevels ?? [];
			services = svcResp.services ?? [];
		} catch (e) {
			toast.error('Failed to load Support data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectCase(c: CaseDetails) {
		selectedCase = c;
		expandedThread = true;
		communications = [];
		replyBody = '';
		await loadCommunications(c.caseId ?? '');
	}

	function closeThread() {
		selectedCase = null;
		communications = [];
		expandedThread = false;
	}

	async function loadCommunications(caseId: string) {
		loadingComms = true;
		try {
			const resp = await support().send(new DescribeCommunicationsCommand({ caseId }));
			communications = resp.communications ?? [];
		} catch (e) {
			toast.error('Failed to load communications: ' + String(e));
		} finally {
			loadingComms = false;
		}
	}

	async function sendReply() {
		if (!selectedCase?.caseId || !replyBody.trim()) return;
		sendingReply = true;
		try {
			await support().send(
				new AddCommunicationToCaseCommand({
					caseId: selectedCase.caseId,
					communicationBody: replyBody.trim()
				})
			);
			toast.success('Reply sent');
			replyBody = '';
			await loadCommunications(selectedCase.caseId);
		} catch (e) {
			toast.error('Failed to send reply: ' + String(e));
		} finally {
			sendingReply = false;
		}
	}

	async function createCase() {
		if (!createSubject.trim() || !createBody.trim() || !createServiceCode || !createSeverityCode) {
			toast.error('Please fill in all required fields');
			return;
		}
		creating = true;
		try {
			await support().send(
				new CreateCaseCommand({
					subject: createSubject.trim(),
					communicationBody: createBody.trim(),
					serviceCode: createServiceCode,
					categoryCode: createCategoryCode || undefined,
					severityCode: createSeverityCode,
					language: createLanguage
				})
			);
			toast.success('Support case created');
			createSubject = '';
			createBody = '';
			createServiceCode = '';
			createCategoryCode = '';
			createSeverityCode = '';
			activeTab = 'cases';
			await loadData();
		} catch (e) {
			toast.error('Failed to create case: ' + String(e));
		} finally {
			creating = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<LifeBuoy class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Support</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Manage support cases and access AWS Support resources
				</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => {
					activeTab = 'create';
					selectedCase = null;
				}}
				class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm font-medium"
			>
				<Plus class="w-4 h-4" /> New Case
			</button>
			<button
				onclick={loadData}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<!-- Status Index -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<LifeBuoy class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{cases.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Total Cases</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<AlertTriangle class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{openCases}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Open</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{resolvedCases}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Resolved</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Clock class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{severityLevels.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Severity Levels</p>
			</div>
		</div>
	</div>

	<!-- Create Case Form -->
	{#if activeTab === 'create'}
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700"
		>
			<div
				class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between"
			>
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Support Case</h2>
				<button
					onclick={() => (activeTab = 'cases')}
					class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
				>
					<X class="w-5 h-5" />
				</button>
			</div>
			<div class="p-4 space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Subject <span class="text-red-500">*</span></label
					>
					<input
						bind:value={createSubject}
						placeholder="Briefly describe your issue"
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>

				<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Service <span class="text-red-500">*</span></label
						>
						<select
							bind:value={createServiceCode}
							onchange={() => (createCategoryCode = '')}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select service...</option>
							{#each services as svc}
								<option value={svc.code}>{svc.name}</option>
							{/each}
						</select>
					</div>

					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Category</label
						>
						<select
							bind:value={createCategoryCode}
							disabled={!createServiceCode}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white disabled:opacity-50"
						>
							<option value="">Select category...</option>
							{#each selectedServiceCategories as cat}
								<option value={cat.code}>{cat.name}</option>
							{/each}
						</select>
					</div>

					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Severity <span class="text-red-500">*</span></label
						>
						<select
							bind:value={createSeverityCode}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select severity...</option>
							{#each severityLevels as sev}
								<option value={sev.code}>{sev.name}</option>
							{/each}
						</select>
					</div>

					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Language</label
						>
						<select
							bind:value={createLanguage}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="en">English</option>
							<option value="ja">Japanese</option>
							<option value="zh">Chinese</option>
							<option value="ko">Korean</option>
						</select>
					</div>
				</div>

				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Description <span class="text-red-500">*</span></label
					>
					<textarea
						bind:value={createBody}
						rows={6}
						placeholder="Describe your issue in detail..."
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"
					></textarea>
				</div>

				<div class="flex justify-end gap-2">
					<button
						onclick={() => (activeTab = 'cases')}
						class="px-4 py-2 rounded-lg text-sm border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
					>
						Cancel
					</button>
					<button
						onclick={createCase}
						disabled={creating}
						class="flex items-center gap-2 px-4 py-2 rounded-lg text-sm bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
					>
						<Send class="w-4 h-4" />
						{creating ? 'Creating...' : 'Create Case'}
					</button>
				</div>
			</div>
		</div>
	{/if}

	<!-- Communication Thread Viewer -->
	{#if selectedCase && expandedThread}
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700"
		>
			<div
				class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between"
			>
				<div class="flex items-center gap-3">
					<MessageSquare class="w-5 h-5 text-blue-500" />
					<div>
						<p class="font-semibold text-gray-900 dark:text-white">{selectedCase.subject}</p>
						<p class="text-xs text-gray-500 dark:text-gray-400">
							{selectedCase.caseId} · {selectedCase.serviceCode} ·
							<span
								class={selectedCase.status === 'resolved'
									? 'text-green-600 dark:text-green-400'
									: 'text-orange-600 dark:text-orange-400'}>{selectedCase.status}</span
							>
						</p>
					</div>
				</div>
				<div class="flex items-center gap-2">
					<button
						onclick={() => (expandedThread = !expandedThread)}
						class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
						title={expandedThread ? 'Collapse' : 'Expand'}
					>
						{#if expandedThread}
							<ChevronUp class="w-5 h-5" />
						{:else}
							<ChevronDown class="w-5 h-5" />
						{/if}
					</button>
					<button
						onclick={closeThread}
						class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
			</div>

			<div class="p-4 space-y-4">
				{#if loadingComms}
					<div class="text-center py-6 text-gray-500 dark:text-gray-400">
						Loading communications...
					</div>
				{:else if communications.length === 0}
					<div class="text-center py-6 text-gray-500 dark:text-gray-400">
						No communications yet.
					</div>
				{:else}
					<div class="space-y-3 max-h-96 overflow-y-auto">
						{#each communications as comm}
							<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 space-y-1">
								<div class="flex items-center justify-between">
									<span class="text-xs font-medium text-blue-600 dark:text-blue-400"
										>{comm.submittedBy ?? 'AWS Support'}</span
									>
									<span class="text-xs text-gray-400 dark:text-gray-500"
										>{comm.timeCreated ?? ''}</span
									>
								</div>
								<p class="text-sm text-gray-800 dark:text-gray-200 whitespace-pre-wrap">
									{comm.body ?? ''}
								</p>
								{#if (comm.attachmentSet ?? []).length > 0}
									<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
										{(comm.attachmentSet ?? []).length} attachment(s)
									</p>
								{/if}
							</div>
						{/each}
					</div>
				{/if}

				{#if selectedCase.status !== 'resolved'}
					<div class="flex gap-2 pt-2 border-t border-slate-200 dark:border-slate-700">
						<textarea
							bind:value={replyBody}
							rows={3}
							placeholder="Type your reply..."
							class="flex-1 px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"
						></textarea>
						<button
							onclick={sendReply}
							disabled={sendingReply || !replyBody.trim()}
							class="self-end flex items-center gap-2 px-4 py-2 rounded-lg text-sm bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
						>
							<Send class="w-4 h-4" />
							{sendingReply ? 'Sending...' : 'Reply'}
						</button>
					</div>
				{/if}
			</div>
		</div>
	{/if}

	<!-- Cases / Services Tab Panel -->
	{#if activeTab !== 'create'}
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700"
		>
			<div
				class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center"
			>
				<div class="flex gap-2">
					{#each [['cases', 'Support Cases'], ['services', 'AWS Services']] as [tab, label]}
						<button
							onclick={() => {
								// eslint-disable-next-line @typescript-eslint/no-explicit-any
								activeTab = tab as any;
								searchQuery = '';
							}}
							class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab
								? 'bg-blue-600 text-white'
								: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
						>
							{label}
						</button>
					{/each}
				</div>
				<div class="flex gap-2 items-center">
					{#if activeTab === 'cases'}
						<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400">
							<input
								type="checkbox"
								bind:checked={includedResolved}
								onchange={loadData}
								class="rounded"
							/>
							Include resolved
						</label>
					{/if}
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
						<input
							bind:value={searchQuery}
							placeholder="Search..."
							class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48"
						/>
					</div>
				</div>
			</div>
			<div class="p-4">
				{#if loading}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if activeTab === 'cases'}
					{#if filteredCases.length === 0}
						<div class="text-center py-8 text-gray-500 dark:text-gray-400">
							No support cases found.
							<button
								onclick={() => (activeTab = 'create')}
								class="ml-2 text-blue-600 hover:underline text-sm">Create one</button
							>
						</div>
					{:else}
						<div class="space-y-2">
							{#each filteredCases as c}
								<button
									onclick={() => selectCase(c)}
									class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left transition-colors {selectedCase?.caseId === c.caseId ? 'ring-2 ring-blue-500' : ''}"
								>
									<div class="flex items-center gap-3">
										<LifeBuoy class="w-5 h-5 text-blue-500 shrink-0" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">{c.subject}</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">
												{c.caseId} · {c.serviceCode} · severity: {c.severityCode}
											</p>
										</div>
									</div>
									<div class="flex items-center gap-2 shrink-0 ml-4">
										<span
											class="text-xs px-2 py-1 rounded-full {c.status === 'resolved'
												? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
												: 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400'}"
											>{c.status}</span
										>
										<MessageSquare class="w-4 h-4 text-gray-400" />
									</div>
								</button>
							{/each}
						</div>
					{/if}
				{:else if activeTab === 'services'}
					{#if filteredServices.length === 0}
						<div class="text-center py-8 text-gray-500 dark:text-gray-400">No services found</div>
					{:else}
						<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
							{#each filteredServices as svc}
								<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
									<p class="font-medium text-gray-900 dark:text-white text-sm">{svc.name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{svc.code}</p>
									{#if (svc.categories ?? []).length > 0}
										<p class="text-xs text-gray-400 dark:text-gray-500 mt-1">
											{(svc.categories ?? []).length} categories
										</p>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				{/if}
			</div>
		</div>
	{/if}
</div>
