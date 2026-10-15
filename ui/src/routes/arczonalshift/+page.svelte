<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '#lib/region-effect.svelte.ts';
	import { getARCZonalShiftClient } from '#lib/aws-client.ts';
	import { describeError, statusBadgeClass } from '#lib/arc.ts';
	import {
		ListManagedResourcesCommand,
		GetManagedResourceCommand,
		ListZonalShiftsCommand,
		ListAutoshiftsCommand,
		StartZonalShiftCommand,
		CancelZonalShiftCommand,
		StartPracticeRunCommand,
		CancelPracticeRunCommand,
		CreatePracticeRunConfigurationCommand,
		UpdatePracticeRunConfigurationCommand,
		DeletePracticeRunConfigurationCommand,
		UpdateZonalAutoshiftConfigurationCommand,
		type ManagedResourceSummary,
		type ZonalShiftSummary,
		type AutoshiftSummary,
		type PracticeRunConfiguration
	} from '@aws-sdk/client-arc-zonal-shift';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { createTabLoader } from '#lib/tab-loader.svelte.ts';
	import { formatDate } from '#lib/format.ts';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '#lib/components/Tabs.svelte';
	import SearchInput from '#lib/components/SearchInput.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { Shuffle, X } from 'lucide-svelte';

	const client = regionalClient(getARCZonalShiftClient);

	type TabId = 'resources' | 'shifts' | 'practice' | 'autoshifts';
	const tabs: TabDef[] = [
		{ id: 'resources', label: 'Managed Resources' },
		{ id: 'shifts', label: 'Zonal Shifts' },
		{ id: 'practice', label: 'Practice Runs' },
		{ id: 'autoshifts', label: 'Autoshifts' }
	];

	const cancelBtn =
		'rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800';
	const primaryBtn =
		'rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50';
	const rowBtn =
		'px-2 py-1 rounded-lg border border-sky-600 text-sky-700 dark:text-sky-400 hover:bg-sky-50 dark:hover:bg-sky-900/20 text-xs';
	const input =
		'px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full';
	const th = 'px-4 py-2 font-medium';
	const td = 'px-4 py-3 text-gray-700 dark:text-gray-300';

	let activeTab = $state<TabId>('resources');
	let searchQuery = $state('');
	let resources = $state<ManagedResourceSummary[]>([]);
	let shifts = $state<ZonalShiftSummary[]>([]);
	let autoshifts = $state<AutoshiftSummary[]>([]);

	async function fetchResources(): Promise<void> {
		const out: ManagedResourceSummary[] = [];
		let token: string | undefined;
		do {
			const r = await client().send(new ListManagedResourcesCommand({ nextToken: token }));
			out.push(...(r.items ?? []));
			token = r.nextToken;
		} while (token);
		resources = out;
	}

	async function fetchShifts(): Promise<void> {
		const out: ZonalShiftSummary[] = [];
		let token: string | undefined;
		do {
			const r = await client().send(new ListZonalShiftsCommand({ nextToken: token }));
			out.push(...(r.items ?? []));
			token = r.nextToken;
		} while (token);
		shifts = out;
	}

	async function fetchAutoshifts(): Promise<void> {
		const out: AutoshiftSummary[] = [];
		let token: string | undefined;
		do {
			const r = await client().send(new ListAutoshiftsCommand({ nextToken: token }));
			out.push(...(r.items ?? []));
			token = r.nextToken;
		} while (token);
		autoshifts = out;
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		resources: () => fetchResources().catch(rethrowDescribed),
		shifts: () => fetchShifts().catch(rethrowDescribed),
		practice: () => fetchShifts().catch(rethrowDescribed),
		autoshifts: () => fetchAutoshifts().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	onRegionChange(() => {
		untrack(() => {
			void tabLoader.refresh(activeTab);
		});
	});

	const q = $derived(searchQuery.toLowerCase());
	const fResources = $derived(
		resources.filter((r) => `${r.name ?? ''} ${r.arn ?? ''}`.toLowerCase().includes(q))
	);
	const isPractice = (s: ZonalShiftSummary) => s.shiftType === 'PRACTICE_RUN';
	const fShifts = $derived(
		shifts.filter((s) => !isPractice(s) && (s.resourceIdentifier ?? '').toLowerCase().includes(q))
	);
	const fPractice = $derived(
		shifts.filter((s) => isPractice(s) && (s.resourceIdentifier ?? '').toLowerCase().includes(q))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	function resourceName(arn: string | undefined): string {
		return resources.find((r) => r.arn === arn)?.name ?? arn?.split('/').at(-2) ?? arn ?? '';
	}

	// --- shift / practice run ---

	let shiftModal = $state<Modal | null>(null);
	let shiftMode = $state<'zonal' | 'practice'>('zonal');
	let shiftResource = $state<ManagedResourceSummary | null>(null);
	let awayFrom = $state('');
	let expiresIn = $state('1h');
	let comment = $state('');
	let saving = $state(false);
	let modalError = $state<string | null>(null);

	function openShift(r: ManagedResourceSummary, mode: 'zonal' | 'practice'): void {
		shiftResource = r;
		shiftMode = mode;
		awayFrom = r.availabilityZones?.[0] ?? '';
		comment = '';
		modalError = null;
		shiftModal?.open();
	}

	async function submitShift(): Promise<void> {
		if (!shiftResource?.arn) return;
		saving = true;
		modalError = null;
		try {
			if (shiftMode === 'zonal') {
				await client().send(
					new StartZonalShiftCommand({
						resourceIdentifier: shiftResource.arn,
						awayFrom,
						expiresIn,
						comment
					})
				);
				toast.success('Zonal shift started');
			} else {
				await client().send(
					new StartPracticeRunCommand({
						resourceIdentifier: shiftResource.arn,
						awayFrom,
						comment
					})
				);
				toast.success('Practice run started');
			}
			shiftModal?.close();
			await Promise.all([tabLoader.refresh('resources'), fetchShifts()]);
		} catch (e) {
			modalError = describeError(e);
			toast.error(modalError);
		} finally {
			saving = false;
		}
	}

	async function cancelShift(s: ZonalShiftSummary): Promise<void> {
		const practice = isPractice(s);
		const ok = await confirmDestructive({
			title: practice ? 'Cancel practice run' : 'Cancel zonal shift',
			message: `Cancel ${practice ? 'practice run' : 'zonal shift'} ${s.zonalShiftId}?`
		});
		if (!ok || !s.zonalShiftId) return;
		try {
			if (practice) {
				await client().send(new CancelPracticeRunCommand({ zonalShiftId: s.zonalShiftId }));
			} else {
				await client().send(new CancelZonalShiftCommand({ zonalShiftId: s.zonalShiftId }));
			}
			toast.success('Canceled');
			await Promise.all([fetchShifts(), fetchResources()]);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- practice configuration / autoshift ---

	let configModal = $state<Modal | null>(null);
	let configResource = $state<ManagedResourceSummary | null>(null);
	let existingConfig = $state<PracticeRunConfiguration | undefined>();
	let outcomeAlarm = $state('');
	let blockingAlarm = $state('');

	async function openConfig(r: ManagedResourceSummary): Promise<void> {
		configResource = r;
		existingConfig = undefined;
		outcomeAlarm = '';
		blockingAlarm = '';
		modalError = null;
		configModal?.open();
		try {
			const d = await client().send(GetManagedResourceCmd(r));
			existingConfig = d.practiceRunConfiguration;
			outcomeAlarm = existingConfig?.outcomeAlarms?.[0]?.alarmIdentifier ?? '';
			blockingAlarm = existingConfig?.blockingAlarms?.[0]?.alarmIdentifier ?? '';
		} catch (e) {
			modalError = describeError(e);
		}
	}

	function GetManagedResourceCmd(r: ManagedResourceSummary): GetManagedResourceCommand {
		return new GetManagedResourceCommand({ resourceIdentifier: r.arn });
	}

	async function applyConfig(work: () => Promise<unknown>, ok: string): Promise<void> {
		saving = true;
		modalError = null;
		try {
			await work();
			toast.success(ok);
			configModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			modalError = describeError(e);
			toast.error(modalError);
		} finally {
			saving = false;
		}
	}

	function alarms() {
		return {
			outcomeAlarms: [{ type: 'CLOUDWATCH' as const, alarmIdentifier: outcomeAlarm }],
			blockingAlarms: blockingAlarm
				? [{ type: 'CLOUDWATCH' as const, alarmIdentifier: blockingAlarm }]
				: []
		};
	}

	const saveConfig = () =>
		applyConfig(
			() =>
				existingConfig
					? client().send(
							new UpdatePracticeRunConfigurationCommand({
								resourceIdentifier: configResource?.arn,
								...alarms()
							})
						)
					: client().send(
							new CreatePracticeRunConfigurationCommand({
								resourceIdentifier: configResource?.arn,
								...alarms()
							})
						),
			'Practice run configuration saved'
		);

	const removeConfig = () =>
		applyConfig(
			() =>
				client().send(
					new DeletePracticeRunConfigurationCommand({ resourceIdentifier: configResource?.arn })
				),
			'Practice run configuration deleted'
		);

	const setAutoshift = (status: 'ENABLED' | 'DISABLED') =>
		applyConfig(
			() =>
				client().send(
					new UpdateZonalAutoshiftConfigurationCommand({
						resourceIdentifier: configResource?.arn,
						zonalAutoshiftStatus: status
					})
				),
			`Zonal autoshift ${status.toLowerCase()}`
		);
</script>

{#snippet badge(value: string | undefined)}
	<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(value)}">{value ?? '—'}</span>
{/snippet}

{#snippet shiftTable(rows: ZonalShiftSummary[], empty: string, practice: boolean)}
	{#if rows.length === 0}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">{empty}</div>
	{:else}
		<div class="overflow-x-auto"><table class="w-full text-sm">
			<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
				<th class={th}>Shift ID</th><th class={th}>Resource</th><th class={th}>Away from</th>
				<th class={th}>Status</th>{#if practice}<th class={th}>Outcome</th>{/if}
				<th class={th}>Started</th><th class={th}>Expires</th><th class={th}>Comment</th><th class={th}></th>
			</tr></thead>
			<tbody>
				{#each rows as s (s.zonalShiftId)}
					<tr class="border-b border-gray-100 dark:border-gray-800">
						<td class="{td} font-mono text-xs">{s.zonalShiftId}</td>
						<td class={td}>{resourceName(s.resourceIdentifier)}</td>
						<td class={td}>{s.awayFrom}</td>
						<td class={td}>{@render badge(s.status)}</td>
						{#if practice}<td class={td}>{s.practiceRunOutcome ?? '—'}</td>{/if}
						<td class={td}>{formatDate(s.startTime)}</td>
						<td class={td}>{formatDate(s.expiryTime)}</td>
						<td class={td}>{s.comment || '—'}</td>
						<td class={td}>
							{#if s.status === 'ACTIVE'}
								<button onclick={() => cancelShift(s)} title="Cancel" aria-label="Cancel {practice ? 'practice run' : 'zonal shift'} {s.zonalShiftId}" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
							{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table></div>
	{/if}
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={Shuffle}
		title="ARC Zonal Shift"
		description="Shift traffic away from an Availability Zone and rehearse it with practice runs"
		onRefresh={() => tabLoader.refresh(activeTab)}
		color="sky"
	/>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="sky" />
			<SearchInput bind:value={searchQuery} />
		</div>
		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if tabLoader.isLoading(activeTab)}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'resources'}
				{#if fResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No managed resources found</div>
				{:else}
					<div class="overflow-x-auto"><table class="w-full text-sm">
						<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
							<th class={th}>Resource</th><th class={th}>Zones (weight)</th><th class={th}>Active shifts</th>
							<th class={th}>Autoshift</th><th class={th}>Practice runs</th><th class={th}>Actions</th>
						</tr></thead>
						<tbody>
							{#each fResources as r (r.arn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}><div class="font-medium">{r.name}</div><div class="text-xs text-gray-400 break-all">{r.arn}</div></td>
									<td class={td}>
										{#each r.availabilityZones ?? [] as az (az)}
											<div class="text-xs">{az} ({r.appliedWeights?.[az] ?? 1})</div>
										{/each}
									</td>
									<td class={td}>{(r.zonalShifts ?? []).length}</td>
									<td class={td}>{@render badge(r.zonalAutoshiftStatus)}</td>
									<td class={td}>{@render badge(r.practiceRunStatus)}</td>
									<td class={td}>
										<div class="flex gap-2 flex-wrap">
											<button class={rowBtn} onclick={() => openShift(r, 'zonal')} aria-label="Start zonal shift for {r.name}">Shift</button>
											<button class={rowBtn} onclick={() => openShift(r, 'practice')} aria-label="Start practice run for {r.name}">Practice run</button>
											<button class={rowBtn} onclick={() => openConfig(r)} aria-label="Configure {r.name}">Configure</button>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table></div>
				{/if}
			{:else if activeTab === 'shifts'}
				{@render shiftTable(fShifts, 'No zonal shifts found', false)}
			{:else if activeTab === 'practice'}
				{@render shiftTable(fPractice, 'No practice runs found', true)}
			{:else if autoshifts.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No autoshifts found</div>
			{:else}
				<div class="overflow-x-auto"><table class="w-full text-sm">
					<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class={th}>Away from</th><th class={th}>Status</th><th class={th}>Started</th><th class={th}>Ended</th>
					</tr></thead>
					<tbody>
						{#each autoshifts as a (`${a.awayFrom}-${a.startTime}`)}
							<tr class="border-b border-gray-100 dark:border-gray-800">
								<td class={td}>{a.awayFrom}</td><td class={td}>{@render badge(a.status)}</td>
								<td class={td}>{formatDate(a.startTime)}</td><td class={td}>{formatDate(a.endTime)}</td>
							</tr>
						{/each}
					</tbody>
				</table></div>
			{/if}
		</div>
	</div>
</div>

{#snippet errorLine()}
	{#if modalError}<p class="text-sm text-red-600 dark:text-red-400">{modalError}</p>{/if}
{/snippet}

<Modal bind:this={shiftModal} title={shiftMode === 'zonal' ? 'Start Zonal Shift' : 'Start Practice Run'}>
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm break-all">{shiftResource?.name}</p>
			<label class="block text-sm" for="shift-away">Away from</label>
			<select id="shift-away" class={input} bind:value={awayFrom}>
				{#each shiftResource?.availabilityZones ?? [] as az (az)}<option value={az}>{az}</option>{/each}
			</select>
			{#if shiftMode === 'zonal'}
				<label class="block text-sm" for="shift-expires">Expires in</label>
				<input id="shift-expires" class={input} bind:value={expiresIn} placeholder="e.g. 30m, 2h, 3d" />
			{/if}
			<label class="block text-sm" for="shift-comment">Comment</label>
			<input id="shift-comment" class={input} bind:value={comment} />
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => shiftModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitShift} disabled={saving || !awayFrom || !comment} class={primaryBtn}>{saving ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={configModal} title="Practice Runs and Autoshift">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm break-all">{configResource?.name}</p>
			<label class="block text-sm" for="cfg-outcome">Outcome alarm ARN</label>
			<input id="cfg-outcome" class={input} bind:value={outcomeAlarm} />
			<label class="block text-sm" for="cfg-blocking">Blocking alarm ARN (optional)</label>
			<input id="cfg-blocking" class={input} bind:value={blockingAlarm} />
			<div class="flex gap-2 flex-wrap">
				<button type="button" class={rowBtn} onclick={() => setAutoshift('ENABLED')} disabled={saving || !existingConfig}>Enable autoshift</button>
				<button type="button" class={rowBtn} onclick={() => setAutoshift('DISABLED')} disabled={saving}>Disable autoshift</button>
				{#if existingConfig}
					<button type="button" class={rowBtn} onclick={removeConfig} disabled={saving}>Delete configuration</button>
				{/if}
			</div>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => configModal?.close()} class={cancelBtn}>Close</button>
		<button type="button" onclick={saveConfig} disabled={saving || !outcomeAlarm} class={primaryBtn}>{existingConfig ? 'Update configuration' : 'Create configuration'}</button>
	{/snippet}
</Modal>
