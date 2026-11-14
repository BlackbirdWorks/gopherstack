<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import {
		MAX_RULE_PRIORITY,
		MIN_RULE_PRIORITY,
		RULE_PROTOCOLS,
		deleteRule,
		emptyRule,
		listNsgs,
		putRule,
		validateRule,
		type Nsg,
		type RuleInput,
		type SecurityRule
	} from '#lib/azurenetwork.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	let nsgs = $state<Nsg[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let draft = $state<RuleInput>(emptyRule());

	const current = $derived(nsgs.find((n) => n.name === selected) ?? nsgs[0]);
	const draftError = $derived(validateRule(draft, current?.rules.map((r) => r.name) ?? []));

	async function load() {
		loading = true;
		try {
			nsgs = await listNsgs(subscription, group);
			error = '';
			if (!nsgs.some((n) => n.name === selected)) selected = nsgs[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function addRule(e: Event) {
		e.preventDefault();
		if (!current || draftError) return;
		if (await attempt(() => putRule(subscription, group, current.name, draft), `Rule ${draft.name} saved`, 'Failed to save rule')) {
			draft = emptyRule();
			await load();
		}
	}

	async function removeRule(n: Nsg, r: SecurityRule) {
		const ok = await confirmedAttempt({ title: 'Delete rule', message: `Delete rule "${r.name}" from "${n.name}"?`, confirmLabel: 'Delete' }, () => deleteRule(subscription, group, n.name, r.name), `Rule ${r.name} deleted`, `Failed to delete ${r.name}`);
		if (ok) await load();
	}

	const cell = 'px-3 py-2';
	const join = (v: string[]) => v.join(', ') || '—';
	const accessPill = (a: string) => (a === 'Allow' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300');

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-4" data-testid="net-nsgs">
	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="net-nsgs-error">Could not load network security groups: {error}</p>
	{/if}
	{#if loading && nsgs.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if nsgs.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="net-nsgs-empty">No network security groups in this resource group.</p>
	{:else if current}
		<div class="flex items-center gap-2 text-xs text-gray-500">
			<label for="net-nsg-select">Network security group</label>
			<select id="net-nsg-select" class="{GCP_INPUT} !w-64 !py-1 text-xs" bind:value={selected} data-testid="net-nsg-select">
				{#each nsgs as n (n.id)}<option value={n.name}>{n.name} ({n.location})</option>{/each}
			</select>
		</div>

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="net-rule-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Priority</th><th class={cell}>Name</th><th class={cell}>Direction</th><th class={cell}>Access</th><th class={cell}>Protocol</th><th class={cell}>Source</th><th class={cell}>Source ports</th><th class={cell}>Destination</th><th class={cell}>Dest. ports</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each current.rules as r (r.name)}
						<tr data-testid="net-rule">
							<td class="{cell} text-xs" data-testid="net-rule-priority">{r.priority}</td>
							<td class="{cell} font-mono text-xs" data-testid="net-rule-name">{r.name}</td>
							<td class="{cell} text-xs">{r.direction}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {accessPill(r.access)}">{r.access}</span></td>
							<td class="{cell} text-xs">{r.protocol}</td>
							<td class="{cell} font-mono text-xs">{join(r.sources)}</td>
							<td class="{cell} font-mono text-xs">{join(r.sourcePorts)}</td>
							<td class="{cell} font-mono text-xs">{join(r.destinations)}</td>
							<td class="{cell} font-mono text-xs">{join(r.destinationPorts)}</td>
							<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeRule(current, r)} data-testid="net-rule-delete">Delete</button></td>
						</tr>
					{:else}
						<tr><td class="{cell} text-gray-500" colspan="10" data-testid="net-rules-empty">No custom rules.</td></tr>
					{/each}
					{#each current.defaultRules as r (r.name)}
						<tr class="text-gray-500" data-testid="net-default-rule">
							<td class="{cell} text-xs">{r.priority}</td>
							<td class="{cell} font-mono text-xs">{r.name}</td>
							<td class="{cell} text-xs">{r.direction}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {accessPill(r.access)}">{r.access}</span></td>
							<td class="{cell} text-xs">{r.protocol}</td>
							<td class="{cell} font-mono text-xs">{join(r.sources)}</td>
							<td class="{cell} font-mono text-xs">{join(r.sourcePorts)}</td>
							<td class="{cell} font-mono text-xs">{join(r.destinations)}</td>
							<td class="{cell} font-mono text-xs">{join(r.destinationPorts)}</td>
							<td class="{cell} text-right text-xs">default</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		<form class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={addRule} data-testid="net-rule-form">
			<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Add security rule to {current.name}</h3>
			<div class="flex flex-wrap items-end gap-2">
				<label class="flex flex-col gap-1 text-xs">Name<input class="{GCP_INPUT} !w-36" bind:value={draft.name} data-testid="net-rule-name-input" /></label>
				<label class="flex flex-col gap-1 text-xs">Priority ({MIN_RULE_PRIORITY}–{MAX_RULE_PRIORITY})<input class="{GCP_INPUT} !w-24" inputmode="numeric" bind:value={draft.priority} data-testid="net-rule-priority-input" /></label>
				<label class="flex flex-col gap-1 text-xs">Direction
					<select class="{GCP_INPUT} !w-28" bind:value={draft.direction} data-testid="net-rule-direction"><option>Inbound</option><option>Outbound</option></select>
				</label>
				<label class="flex flex-col gap-1 text-xs">Access
					<select class="{GCP_INPUT} !w-24" bind:value={draft.access} data-testid="net-rule-access"><option>Allow</option><option>Deny</option></select>
				</label>
				<label class="flex flex-col gap-1 text-xs">Protocol
					<select class="{GCP_INPUT} !w-24" bind:value={draft.protocol} data-testid="net-rule-protocol">{#each RULE_PROTOCOLS as p (p)}<option>{p}</option>{/each}</select>
				</label>
				<label class="flex flex-col gap-1 text-xs">Source prefixes<input class="{GCP_INPUT} !w-36" bind:value={draft.sources} data-testid="net-rule-sources" /></label>
				<label class="flex flex-col gap-1 text-xs">Source ports<input class="{GCP_INPUT} !w-28" bind:value={draft.sourcePorts} data-testid="net-rule-source-ports" /></label>
				<label class="flex flex-col gap-1 text-xs">Destination prefixes<input class="{GCP_INPUT} !w-36" bind:value={draft.destinations} data-testid="net-rule-destinations" /></label>
				<label class="flex flex-col gap-1 text-xs">Dest. ports<input class="{GCP_INPUT} !w-28" placeholder="22, 8000-8100" bind:value={draft.destinationPorts} data-testid="net-rule-dest-ports" /></label>
				<label class="flex flex-col gap-1 text-xs">Description<input class="{GCP_INPUT} !w-44" bind:value={draft.description} data-testid="net-rule-description" /></label>
				<button class={GCP_PRIMARY} type="submit" disabled={draftError !== null} data-testid="net-rule-add">Add rule</button>
			</div>
			{#if draft.name && draftError}<p class="text-xs text-amber-700 dark:text-amber-400" data-testid="net-rule-hint">{draftError}</p>{/if}
		</form>
	{/if}
</div>
