<script lang="ts">
	import { onMount } from 'svelte';
	import { ExternalLink, Plus, RefreshCw, Trash2, Upload } from 'lucide-svelte';
	import FunctionForm from '#lib/components/gcp/FunctionForm.svelte';
	import FunctionInvoke from '#lib/components/gcp/FunctionInvoke.svelte';
	import RunBadge from '#lib/components/gcp/RunBadge.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_MINI, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import {
		createFunction,
		deleteFunction,
		listFunctions,
		listRuntimes,
		runtimeUnavailable,
		updateSource,
		type FnScope,
		type FunctionInfo,
		type NewFunction,
		type RuntimeInfo,
		type SourceInput
	} from '#lib/gcpfunctions.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listTopics } from '#lib/gcppubsub.ts';

	type Props = { scope: FnScope; tick: number; autoRefresh: boolean; onRuntime: (down: boolean) => void };
	let { scope, tick, autoRefresh, onRuntime }: Props = $props();

	const REFRESH_MS = 3000;
	const TONES = { active: 'ok', failed: 'failed', deploying: 'pending', deleting: 'pending', unknown: 'pending' } as const;
	const LABELS = { active: 'Active', failed: 'Failed', deploying: 'Deploying', deleting: 'Deleting', unknown: 'Unknown' } as const;
	let fns = $state<FunctionInfo[]>([]);
	let runtimes = $state<RuntimeInfo[]>([]);
	let topics = $state<string[]>([]);
	let expanded = $state<string | null>(null);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');
	let form = $state<FunctionForm | null>(null);
	const cell = 'px-4 py-3';

	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');

	async function load() {
		try {
			fns = (await listFunctions(scope)).toSorted((a, b) => a.id.localeCompare(b.id));
			onRuntime(runtimeUnavailable(fns.flatMap((f) => f.messages)));
			loadError = '';
		} catch (e) {
			loadError = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChoices() {
		runtimes = await listRuntimes(scope).catch(() => []);
		topics = (await listTopics(scope.project).catch(() => [])).map((t) => t.name);
	}

	async function act(label: string, fn: () => Promise<unknown>, ok: string, fail: string): Promise<boolean> {
		busy = label;
		const done = await attempt(fn, ok, fail);
		busy = '';
		if (done) await load();
		return done;
	}

	async function create(n: NewFunction) {
		const ok = await act('create', () => createFunction(scope, n), `Creating ${n.id}`, `Failed to create ${n.id}`);
		if (ok) form?.close();
	}

	async function update(f: FunctionInfo, src: SourceInput) {
		const ok = await act('update', () => updateSource(scope, f.id, src), `Updating ${f.id}`, `Failed to update ${f.id}`);
		if (ok) form?.close();
	}

	async function remove(f: FunctionInfo) {
		if (!(await confirmDestructive({ title: 'Delete function', message: `Delete function "${f.id}"?` }))) return;
		if (expanded === f.id) expanded = null;
		await act(`del-${f.id}`, () => deleteFunction(scope, f.id), `Deleting ${f.id}`, `Failed to delete ${f.id}`);
	}

	function openCreate() {
		void loadChoices().then(() => form?.openCreate());
	}

	$effect(() => {
		void tick;
		void scope;
		void load();
	});

	$effect(() => {
		if (!autoRefresh) return;
		const timer = setInterval(() => void load(), REFRESH_MS);
		return () => clearInterval(timer);
	});

	onMount(() => () => onRuntime(false));
</script>

<div class="space-y-4" data-testid="fn-list-root">
	<div class="flex justify-end">
		<button class="flex items-center gap-2 {GCP_PRIMARY}" onclick={openCreate} disabled={loading || Boolean(loadError)} data-testid="fn-new"><Plus class="h-4 w-4" /> Create function</button>
	</div>

	{#if loadError && fns.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="fn-error">
			<p class="font-medium">Could not reach Cloud Functions</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if fns.length === 0}
		<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="fn-empty">No functions in {scope.location}</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700" data-testid="fn-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Function</th><th class={cell}>Runtime</th><th class={cell}>Entry point</th><th class={cell}>State</th><th class={cell}>Trigger</th><th class={cell}>Updated</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each fns as f (f.name)}
						<tr data-testid="fn-row">
							<td class={cell}>
								<button class="font-mono text-xs underline decoration-dotted" onclick={() => (expanded = expanded === f.id ? null : f.id)} aria-expanded={expanded === f.id} data-testid="fn-toggle">{f.id}</button>
							</td>
							<td class="{cell} font-mono text-xs">{f.runtime}</td>
							<td class="{cell} font-mono text-xs">{f.entryPoint}</td>
							<td class="{cell} text-xs"><RunBadge tone={TONES[f.state]} label={LABELS[f.state]} testId="fn-state" /></td>
							<td class="{cell} text-xs">
								{#if f.trigger === 'pubsub'}
									<span class="font-mono" title={f.topic} data-testid="fn-trigger">Pub/Sub: {f.topic.split('/').pop()}</span>
								{:else if f.url}
									<a class="inline-flex items-center gap-1 break-all text-blue-600 underline dark:text-blue-400" href={f.url} target="_blank" rel="noopener noreferrer" data-testid="fn-url">{f.url}<ExternalLink class="h-3 w-3 shrink-0" /></a>
								{:else}—{/if}
							</td>
							<td class="{cell} text-xs">{when(f.updateTime)}</td>
							<td class="{cell} space-x-1 whitespace-nowrap text-right">
								<button class={GCP_MINI} disabled={f.state === 'deploying' || f.state === 'deleting'} onclick={() => form?.openUpdate(f)} data-testid="fn-update"><Upload class="inline h-3.5 w-3.5" /> Source</button>
								<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${f.id}`} onclick={() => remove(f)} aria-label="Delete function {f.id}" data-testid="fn-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
						{#if expanded === f.id}
							<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="fn-detail">
								<td colspan="7" class="space-y-3 px-4 py-3 text-xs">
									<p class="break-all font-mono">{f.name}</p>
									{#each f.messages as m (m)}<p class="text-gray-600 dark:text-gray-300">{m}</p>{/each}
									{#if f.trigger === 'http'}
										{#if f.state === 'active'}
											<FunctionInvoke {scope} fn={f} />
										{:else}
											<p class="text-gray-500">Invoke is available once the function is Active.</p>
										{/if}
									{:else}
										<p class="text-gray-500">Event-triggered: publish to <span class="font-mono">{f.topic}</span> to run it.</p>
									{/if}
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

<FunctionForm bind:this={form} busy={busy === 'create' || busy === 'update'} {runtimes} {topics} onCreate={create} onUpdate={update} />
