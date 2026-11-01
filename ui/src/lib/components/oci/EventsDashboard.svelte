<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Zap, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		ACTION_TYPES,
		DELIVERY_REFRESH_MS,
		actionTypeLabel,
		createRule,
		deleteRule,
		listCompartments,
		listDeliveries,
		listFunctionOptions,
		listRules,
		listStreamOptions,
		listTopicOptions,
		prettyCondition,
		readStoredEventsCompartment,
		setRuleEnabled,
		summarizeCondition,
		validateCondition,
		writeStoredEventsCompartment,
		type ActionType,
		type EventsDelivery,
		type EventsRule,
		type TargetOption
	} from '#lib/ocievents.ts';

	const TABS = [
		{ id: 'rules', label: 'Rules' },
		{ id: 'deliveries', label: 'Deliveries' }
	];
	const CONDITION_SAMPLE = '{\n  "eventType": [\n    "com.oraclecloud.objectstorage.createobject"\n  ]\n}';

	type DraftAction = { type: ActionType; targetId: string; enabled: boolean };

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredEventsCompartment());
	let rules = $state<EventsRule[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('rules');
	let deliveries = $state<EventsDelivery[]>([]);
	let deliveryError = $state('');
	let autoRefresh = $state(true);

	let showCreate = $state(false);
	let name = $state('');
	let description = $state('');
	let condition = $state(CONDITION_SAMPLE);
	let ruleEnabled = $state(true);
	let createCompartment = $state('');
	let draft = $state<DraftAction[]>([{ type: 'ONS', targetId: '', enabled: true }]);
	let createError = $state('');
	let submitting = $state(false);
	let topics = $state<TargetOption[]>([]);
	let streams = $state<TargetOption[]>([]);
	let functions = $state<TargetOption[]>([]);

	const conditionError = $derived(validateCondition(condition));
	const detail = $derived(rules.find((r) => r.id === selected) ?? null);
	const ruleNames = $derived(Object.fromEntries(rules.map((r) => [r.id, r.name])));

	const options = (t: ActionType): TargetOption[] =>
		t === 'ONS' ? topics : t === 'OSS' ? streams : functions;

	async function loadRules() {
		if (!compartment) {
			rules = [];
			return;
		}
		try {
			rules = await listRules(compartment);
			error = '';
			if (!rules.some((r) => r.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await Promise.all([loadRules(), loadDeliveries()]);
		loading = false;
	}

	async function loadDeliveries() {
		try {
			deliveries = await listDeliveries();
			deliveryError = '';
		} catch (e) {
			deliveryError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredEventsCompartment(compartment);
		selected = '';
		void loadRules();
	}

	async function openCreate() {
		showCreate = !showCreate;
		if (!showCreate) return;
		createCompartment = compartment;
		createError = '';
		[topics, streams, functions] = await Promise.all([
			loadTopics(),
			listStreamOptions(),
			listFunctionOptions()
		]);
	}

	const loadTopics = (): Promise<TargetOption[]> =>
		createCompartment.trim() ? listTopicOptions(createCompartment.trim()) : Promise.resolve([]);

	async function refreshTopics() {
		topics = await loadTopics();
	}

	function addAction() {
		draft = [...draft, { type: 'ONS', targetId: '', enabled: true }];
	}

	function removeAction(i: number) {
		draft = draft.filter((_, idx) => idx !== i);
	}

	async function create() {
		createError = '';
		if (conditionError) return;
		const target = createCompartment.trim();
		submitting = true;
		try {
			await createRule(target, {
				name,
				description,
				condition,
				enabled: ruleEnabled,
				actions: draft.map((a) => ({ type: a.type, targetId: a.targetId, enabled: a.enabled }))
			});
		} catch (e) {
			createError = describeGcpError(e);
			return;
		} finally {
			submitting = false;
		}
		showCreate = false;
		name = description = '';
		condition = CONDITION_SAMPLE;
		draft = [{ type: 'ONS', targetId: '', enabled: true }];
		compartment = target;
		writeStoredEventsCompartment(compartment);
		toast.success('Rule created');
		await load();
	}

	async function toggle(r: EventsRule) {
		const ok = await attempt(
			() => setRuleEnabled(r.id, !r.enabled),
			r.enabled ? `Disabled ${r.name}` : `Enabled ${r.name}`,
			'Failed to update rule'
		);
		if (ok) await loadRules();
	}

	async function remove(r: EventsRule) {
		const ok = await confirmedAttempt(
			{ title: 'Delete rule', message: `Delete rule "${r.name}"?`, confirmLabel: 'Delete' },
			() => deleteRule(r.id),
			`Deleted ${r.name}`,
			`Failed to delete ${r.name}`
		);
		if (ok) await load();
	}

	function select(r: EventsRule) {
		selected = selected === r.id ? '' : r.id;
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' || s === 'DELIVERED'
			? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'
			: s === 'FAILED'
				? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
				: 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';

	$effect(() => {
		if (tab !== 'deliveries' || !autoRefresh) return;
		const timer = setInterval(() => void loadDeliveries(), DELIVERY_REFRESH_MS);
		return () => clearInterval(timer);
	});

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="events-dashboard">
	<PageHeader icon={Zap} title="Events" description="Oracle Cloud Events rules, actions and the delivery log" color="red" onRefresh={load} />

	{#if error && compartments.length === 0 && rules.length === 0 && !loading}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="events-error">
			<p class="font-medium">Could not load rules</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'rules'}
			<div class="flex flex-wrap items-center justify-between gap-2">
				<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
					Compartment
					<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="events-compartment">
						{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
					</select>
				</label>
				<button class={GCP_MINI} onclick={openCreate} aria-expanded={showCreate} data-testid="events-create-toggle">Create rule</button>
			</div>

			{#if showCreate}
				<form class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="events-create">
					<div class="grid gap-3 sm:grid-cols-2">
						<label class="block text-sm"><span class="mb-1 block font-medium">Name</span>
							<input class={GCP_INPUT} type="text" bind:value={name} required data-testid="events-name" />
						</label>
						<label class="block text-sm"><span class="mb-1 block font-medium">Compartment OCID</span>
							<input class="{GCP_INPUT} font-mono text-xs" type="text" bind:value={createCompartment} onchange={refreshTopics} required data-testid="events-create-compartment" />
						</label>
					</div>
					<label class="block text-sm"><span class="mb-1 block font-medium">Description</span>
						<input class={GCP_INPUT} type="text" bind:value={description} data-testid="events-description" />
					</label>
					<label class="block text-sm"><span class="mb-1 block font-medium">Condition (JSON)</span>
						<textarea class="{GCP_INPUT} font-mono text-xs" rows="6" spellcheck="false" bind:value={condition} aria-invalid={conditionError ? 'true' : 'false'} data-testid="events-condition"></textarea>
					</label>
					{#if conditionError}<p class="text-xs text-red-600" role="alert" data-testid="events-condition-error">{conditionError}</p>{/if}

					<fieldset class="space-y-2" data-testid="events-actions">
						<legend class="mb-1 text-sm font-medium">Actions</legend>
						{#each draft as a, i (i)}
							<div class="flex flex-wrap items-center gap-2" data-testid="events-action-row">
								<select class="{GCP_INPUT} !w-52 !py-1" bind:value={a.type} onchange={() => (a.targetId = '')} aria-label="Action type" data-testid="events-action-type">
									{#each ACTION_TYPES as t (t.id)}<option value={t.id}>{t.label}</option>{/each}
								</select>
								{#if options(a.type).length > 0}
									<select class="{GCP_INPUT} !w-80 !py-1 font-mono text-xs" bind:value={a.targetId} aria-label="Action target" data-testid="events-action-target">
										<option value="">Select a target</option>
										{#each options(a.type) as o (o.id)}<option value={o.id}>{o.label} ({shortOcid(o.id)})</option>{/each}
									</select>
								{:else}
									<input class="{GCP_INPUT} !w-80 !py-1 font-mono text-xs" type="text" placeholder="Target OCID" bind:value={a.targetId} aria-label="Action target OCID" data-testid="events-action-target-text" />
								{/if}
								<label class="flex items-center gap-1 text-xs"><input type="checkbox" bind:checked={a.enabled} data-testid="events-action-enabled" /> Enabled</label>
								<button type="button" class={GCP_MINI} onclick={() => removeAction(i)} disabled={draft.length === 1} data-testid="events-action-remove">Remove</button>
							</div>
						{/each}
						<button type="button" class={GCP_MINI} onclick={addAction} disabled={draft.length >= 10} data-testid="events-action-add">Add action</button>
					</fieldset>

					<div class="flex flex-wrap items-center gap-4 text-sm">
						<label class="flex items-center gap-1"><input type="checkbox" bind:checked={ruleEnabled} data-testid="events-enabled" /> Rule enabled</label>
						<button type="submit" class={GCP_PRIMARY} disabled={submitting || !!conditionError || !name.trim() || !createCompartment.trim()} data-testid="events-create-submit">Create</button>
					</div>
					{#if createError}<p class="text-sm text-red-600" role="alert" data-testid="events-create-error">{createError}</p>{/if}
				</form>
			{/if}

			{#if error}<p class="text-sm text-red-600" role="alert" data-testid="events-list-error">{error}</p>{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="events-rules">
					<thead class={thead}>
						<tr><th class={cell}>Rule</th><th class={cell}>State</th><th class={cell}>Enabled</th><th class={cell}>Condition</th><th class={cell}>Actions</th><th class="{cell} text-right">Operations</th></tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each rules as r (r.id)}
							<tr class={selected === r.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="events-rule">
								<td class="{cell} font-mono text-xs" title={r.id}><button class="underline decoration-dotted" onclick={() => select(r)} aria-expanded={selected === r.id} data-testid="events-select">{r.name}</button></td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.state)}" data-testid="events-state">{r.state || '—'}</span></td>
								<td class="{cell} text-xs" data-testid="events-rule-enabled">{r.enabled ? 'Yes' : 'No'}</td>
								<td class="{cell} font-mono text-xs" data-testid="events-summary">{summarizeCondition(r.condition)}</td>
								<td class="{cell} text-xs">
									{#each r.actions as a (a.id)}
										<div data-testid="events-rule-action"><span class="font-medium">{a.type}</span> <span class="font-mono" title={a.targetId}>{shortOcid(a.targetId)}</span> <span class="text-gray-500">({a.enabled ? 'enabled' : 'disabled'})</span></div>
									{/each}
								</td>
								<td class="{cell} text-right whitespace-nowrap">
									<button class={GCP_MINI} onclick={() => toggle(r)} data-testid="events-toggle">{r.enabled ? 'Disable' : 'Enable'}</button>
									<button class={GCP_MINI} onclick={() => remove(r)} data-testid="events-delete">Delete</button>
								</td>
							</tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="events-none">No rules in this compartment</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if detail}
				<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="events-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name}</h2>
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
						<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
						<dt class="text-gray-500">State</dt><dd>{detail.state}</dd>
						<dt class="text-gray-500">Enabled</dt><dd>{detail.enabled ? 'Yes' : 'No'}</dd>
						<dt class="text-gray-500">Description</dt><dd>{detail.description || '—'}</dd>
						<dt class="text-gray-500">Created</dt><dd>{detail.created}</dd>
					</dl>
					<div>
						<h3 class="mb-1 text-sm font-medium">Condition</h3>
						<pre class="overflow-x-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="events-detail-condition">{prettyCondition(detail.condition)}</pre>
					</div>
					<div class={box}>
						<table class="w-full text-sm" data-testid="events-detail-actions">
							<thead class={thead}><tr><th class={cell}>Type</th><th class={cell}>Target</th><th class={cell}>State</th><th class={cell}>Enabled</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each detail.actions as a (a.id)}
									<tr data-testid="events-detail-action"><td class="{cell} text-xs">{actionTypeLabel(a.type)}</td><td class="{cell} font-mono text-xs break-all">{a.targetId}</td><td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(a.state)}">{a.state}</span></td><td class="{cell} text-xs">{a.enabled ? 'Yes' : 'No'}</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{:else}
			<div class="flex flex-wrap items-center justify-between gap-2">
				<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
					<input type="checkbox" bind:checked={autoRefresh} data-testid="events-auto-refresh" /> Auto-refresh every {DELIVERY_REFRESH_MS / 1000}s
				</label>
				<button class={GCP_MINI} onclick={loadDeliveries} data-testid="events-deliveries-refresh">Refresh</button>
			</div>
			{#if deliveryError}<p class="text-sm text-red-600" role="alert" data-testid="events-delivery-error">{deliveryError}</p>{/if}
			<div class={box}>
				<table class="w-full text-sm" data-testid="events-deliveries">
					<thead class={thead}>
						<tr><th class={cell}>Time</th><th class={cell}>Rule</th><th class={cell}>Event type</th><th class={cell}>Action</th><th class={cell}>Target</th><th class={cell}>Status</th><th class={cell}>Error</th></tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each deliveries as d (d.eventId + d.actionId)}
							<tr data-testid="events-delivery">
								<td class="{cell} text-xs whitespace-nowrap">{d.time}</td>
								<td class="{cell} font-mono text-xs" title={d.ruleId}>{ruleNames[d.ruleId] ?? shortOcid(d.ruleId)}</td>
								<td class="{cell} font-mono text-xs">{d.eventType}</td>
								<td class="{cell} text-xs">{d.actionType}</td>
								<td class="{cell} font-mono text-xs" title={d.targetId}>{shortOcid(d.targetId)}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(d.status)}" data-testid="events-delivery-status">{d.status}</span></td>
								<td class="{cell} text-xs text-red-600" data-testid="events-delivery-error-text">{d.error}</td>
							</tr>
						{:else}
							<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="events-deliveries-none">No deliveries yet</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
