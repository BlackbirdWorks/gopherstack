<script lang="ts">
	import { onMount } from 'svelte';
	import { ExternalLink, Layers, Plus, RefreshCw, Rocket, Split, Trash2 } from 'lucide-svelte';
	import RunBadge from '#lib/components/gcp/RunBadge.svelte';
	import RunServiceForm from '#lib/components/gcp/RunServiceForm.svelte';
	import RunTrafficBar from '#lib/components/gcp/RunTrafficBar.svelte';
	import RunTrafficForm from '#lib/components/gcp/RunTrafficForm.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_MINI, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createService,
		deleteRevision,
		deployRevision,
		deleteService,
		listRevisions,
		listServices,
		newestFirst,
		runtimeUnavailable,
		serviceMessages,
		setTraffic,
		type EnvVar,
		type RevisionInfo,
		type RunScope,
		type ServiceInfo,
		type TrafficInput
	} from '#lib/gcprun.ts';

	type Props = { scope: RunScope; tick: number; autoRefresh: boolean; onRuntime: (down: boolean) => void };
	let { scope, tick, autoRefresh, onRuntime }: Props = $props();

	const REFRESH_MS = 3000;
	let services = $state<ServiceInfo[]>([]);
	let revisions = $state<RevisionInfo[]>([]);
	let expanded = $state<string | null>(null);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');
	let form = $state<RunServiceForm | null>(null);
	let split = $state<RunTrafficForm | null>(null);
	const cell = 'px-4 py-3';

	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');
	const tone = (s: ServiceInfo) => (s.state === 'ok' ? 'Ready' : s.state === 'failed' ? 'Failed' : 'Deploying');
	const sharePct = (id: string, svc: ServiceInfo) => svc.traffic.filter((t) => t.revision === id).reduce((a, t) => a + t.percent, 0);

	async function loadRevisions() {
		if (!expanded) return;
		try {
			revisions = newestFirst(await listRevisions(scope, expanded));
		} catch {
			revisions = [];
		}
	}

	async function load() {
		try {
			services = (await listServices(scope)).toSorted((a, b) => a.id.localeCompare(b.id));
			onRuntime(runtimeUnavailable(services.flatMap((s) => serviceMessages(s))));
			loadError = '';
			await loadRevisions();
		} catch (e) {
			loadError = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function act(label: string, fn: () => Promise<unknown>, ok: string, fail: string): Promise<boolean> {
		busy = label;
		const done = await attempt(fn, ok, fail);
		busy = '';
		if (done) await load();
		return done;
	}

	async function create(id: string, image: string, port: string, env: EnvVar[]) {
		const ok = await act('create', () => createService(scope, { id, image, port, env }), `Creating service ${id}`, `Failed to create ${id}`);
		if (ok) form?.close();
	}

	async function deploy(svc: ServiceInfo, image: string, env: EnvVar[]) {
		const ok = await act('deploy', () => deployRevision(scope, svc, image, env), `Deploying ${svc.id}`, `Failed to deploy ${svc.id}`);
		if (ok) form?.close();
	}

	async function saveTraffic(svc: ServiceInfo, rows: TrafficInput[]) {
		const ok = await act('traffic', () => setTraffic(scope, svc.id, rows), `Updated traffic of ${svc.id}`, `Failed to update traffic of ${svc.id}`);
		if (ok) split?.close();
	}

	async function remove(svc: ServiceInfo) {
		if (!(await confirmDestructive({ title: 'Delete service', message: `Delete service "${svc.id}" and all its revisions?` }))) return;
		if (expanded === svc.id) expanded = null;
		await act(`del-${svc.id}`, () => deleteService(scope, svc.id), `Deleted ${svc.id}`, `Failed to delete ${svc.id}`);
	}

	async function removeRevision(svc: ServiceInfo, rev: RevisionInfo) {
		if (!(await confirmDestructive({ title: 'Delete revision', message: `Delete revision "${rev.id}"?` }))) return;
		await act(`delrev-${rev.id}`, () => deleteRevision(scope, svc.id, rev.id), `Deleted ${rev.id}`, `Failed to delete ${rev.id}`);
	}

	async function openSplit(svc: ServiceInfo) {
		const revs = await listRevisions(scope, svc.id).catch(() => []);
		split?.open(svc, revs);
	}

	function toggle(svc: ServiceInfo) {
		expanded = expanded === svc.id ? null : svc.id;
		revisions = [];
		void loadRevisions();
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

<div class="space-y-4" data-testid="run-services">
	<div class="flex justify-end">
		<button class="flex items-center gap-2 {GCP_PRIMARY}" onclick={() => form?.openCreate()} disabled={loading || Boolean(loadError)} data-testid="run-new-service"><Plus class="h-4 w-4" /> Create service</button>
	</div>

	{#if loadError && services.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="run-error">
			<p class="font-medium">Could not reach Cloud Run</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if services.length === 0}
		<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="run-no-services">No services in {scope.location}</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700" data-testid="run-service-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Service</th><th class={cell}>URL</th><th class={cell}>Latest ready revision</th><th class={cell}>Traffic</th><th class={cell}>Status</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each services as s (s.name)}
						<tr data-testid="run-service-row">
							<td class="{cell} max-w-[12rem]">
								<button class="block max-w-full truncate text-left font-mono text-xs underline decoration-dotted" title={s.id} onclick={() => toggle(s)} aria-expanded={expanded === s.id} data-testid="run-service-toggle">{s.id}</button>
								<span class="block truncate font-mono text-[11px] text-gray-500 dark:text-gray-400" title={s.image}>{s.image}</span>
							</td>
							<td class="{cell} text-xs">
								{#if s.uri}<a class="inline-flex items-center gap-1 break-all text-blue-600 underline dark:text-blue-400" href={s.uri} target="_blank" rel="noopener noreferrer" data-testid="run-service-url">{s.uri}<ExternalLink class="h-3 w-3 shrink-0" /></a>{:else}—{/if}
							</td>
							<td class="{cell} font-mono text-xs" data-testid="run-latest-ready">{s.latestReady || '—'}</td>
							<td class={cell}><RunTrafficBar traffic={s.traffic} /></td>
							<td class="{cell} text-xs"><RunBadge tone={s.state} label={tone(s)} testId="run-service-state" /></td>
							<td class="{cell} space-x-1 whitespace-nowrap text-right">
								<button class={GCP_MINI} onclick={() => form?.openDeploy(s)} data-testid="run-deploy"><Rocket class="inline h-3.5 w-3.5" /> Deploy</button>
								<button class={GCP_MINI} onclick={() => openSplit(s)} data-testid="run-split"><Split class="inline h-3.5 w-3.5" /> Traffic</button>
								<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${s.id}`} onclick={() => remove(s)} aria-label="Delete service {s.id}" data-testid="run-service-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
						{#if expanded === s.id}
							<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="run-service-detail">
								<td colspan="6" class="space-y-3 px-4 py-3 text-xs">
									<p class="break-all font-mono">{s.name}</p>
									<div>
										<h4 class="mb-1 font-semibold text-gray-700 dark:text-gray-300">Conditions</h4>
										<ul class="space-y-0.5" data-testid="run-conditions">
											{#each s.conditions as c (c.type)}
												<li class="flex flex-wrap items-center gap-2"><RunBadge tone={c.state} label={c.type} /><span class="text-gray-600 dark:text-gray-300">{c.message}</span></li>
											{/each}
										</ul>
									</div>
									{#if s.env.length > 0}
										<p><span class="font-semibold text-gray-700 dark:text-gray-300">Environment</span> {s.env.map((e) => `${e.name}=${e.value}`).join(', ')}</p>
									{/if}
									<div>
										<h4 class="mb-1 flex items-center gap-1 font-semibold text-gray-700 dark:text-gray-300"><Layers class="h-3.5 w-3.5" /> Revisions</h4>
										{#if revisions.length === 0}
											<p class="text-gray-500">No revisions.</p>
										{:else}
											<table class="w-full" data-testid="run-revisions">
												<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-3">Revision</th><th class="pr-3">Image</th><th class="pr-3">Status</th><th class="pr-3">Traffic</th><th class="pr-3">Created</th><th></th></tr></thead>
												<tbody>
													{#each revisions as r (r.name)}
														{@const pct = sharePct(r.id, s)}
														<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="run-revision-row">
															<td class="py-1 pr-3 font-mono">{r.id}</td>
															<td class="pr-3 font-mono">{r.image}</td>
															<td class="pr-3"><RunBadge tone={r.state} label={r.state === 'ok' ? 'Ready' : r.state === 'failed' ? 'Failed' : 'Starting'} /></td>
															<td class="pr-3">{pct}%</td>
															<td class="pr-3">{when(r.createTime)}</td>
															<td class="text-right"><button class="{GCP_MINI} text-red-600" disabled={pct > 0 || busy === `delrev-${r.id}`} title={pct > 0 ? 'Revision is serving traffic' : 'Delete revision'} onclick={() => removeRevision(s, r)} aria-label="Delete revision {r.id}"><Trash2 class="inline h-3.5 w-3.5" /></button></td>
														</tr>
													{/each}
												</tbody>
											</table>
										{/if}
									</div>
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

<RunServiceForm bind:this={form} busy={busy === 'create' || busy === 'deploy'} onCreate={create} onDeploy={deploy} />
<RunTrafficForm bind:this={split} busy={busy === 'traffic'} onSave={saveTraffic} />
