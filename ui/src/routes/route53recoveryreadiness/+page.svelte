<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '#lib/region-effect.svelte.ts';
	import { getRoute53RecoveryReadinessClient } from '#lib/aws-client.ts';
	import { describeError, statusBadgeClass } from '#lib/arc.ts';
	import {
		ListCellsCommand,
		CreateCellCommand,
		DeleteCellCommand,
		GetCellReadinessSummaryCommand,
		ListRecoveryGroupsCommand,
		CreateRecoveryGroupCommand,
		DeleteRecoveryGroupCommand,
		GetRecoveryGroupReadinessSummaryCommand,
		ListResourceSetsCommand,
		CreateResourceSetCommand,
		DeleteResourceSetCommand,
		ListReadinessChecksCommand,
		CreateReadinessCheckCommand,
		DeleteReadinessCheckCommand,
		GetReadinessCheckStatusCommand,
		type CellOutput,
		type RecoveryGroupOutput,
		type ResourceSetOutput,
		type ReadinessCheckOutput,
		type ResourceResult,
		type Message
	} from '@aws-sdk/client-route53-recovery-readiness';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { createTabLoader } from '#lib/tab-loader.svelte.ts';
	import { formatDate } from '#lib/format.ts';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '#lib/components/Tabs.svelte';
	import SearchInput from '#lib/components/SearchInput.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { ShieldCheck, Plus, Trash2, Eye } from 'lucide-svelte';

	const client = regionalClient(getRoute53RecoveryReadinessClient);

	type TabId = 'groups' | 'cells' | 'sets' | 'checks';
	const tabs: TabDef[] = [
		{ id: 'groups', label: 'Recovery Groups' },
		{ id: 'cells', label: 'Cells' },
		{ id: 'sets', label: 'Resource Sets' },
		{ id: 'checks', label: 'Readiness Checks' }
	];

	const RESOURCE_SET_TYPES = [
		'AWS::ApiGateway::Stage',
		'AWS::ApiGatewayV2::Stage',
		'AWS::AutoScaling::AutoScalingGroup',
		'AWS::CloudWatch::Alarm',
		'AWS::DynamoDB::Table',
		'AWS::EC2::CustomerGateway',
		'AWS::EC2::Volume',
		'AWS::EC2::VPC',
		'AWS::EC2::VPNConnection',
		'AWS::EC2::VPNGateway',
		'AWS::ElasticLoadBalancing::LoadBalancer',
		'AWS::ElasticLoadBalancingV2::LoadBalancer',
		'AWS::Lambda::Function',
		'AWS::MSK::Cluster',
		'AWS::RDS::DBCluster',
		'AWS::Route53::HealthCheck',
		'AWS::SNS::Topic',
		'AWS::SQS::Queue'
	];

	const btn =
		'flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm';
	const cancelBtn =
		'rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800';
	const primaryBtn =
		'rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50';
	const input =
		'px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full';
	const th = 'px-4 py-2 font-medium';
	const td = 'px-4 py-3 text-gray-700 dark:text-gray-300';

	let activeTab = $state<TabId>('groups');
	let searchQuery = $state('');
	let groups = $state<RecoveryGroupOutput[]>([]);
	let cells = $state<CellOutput[]>([]);
	let sets = $state<ResourceSetOutput[]>([]);
	let checks = $state<ReadinessCheckOutput[]>([]);
	let readiness = $state<Record<string, string>>({});

	async function listAll<T>(
		page: (token?: string) => Promise<{ items?: T[]; next?: string }>
	): Promise<T[]> {
		const out: T[] = [];
		let token: string | undefined;
		do {
			const r = await page(token);
			out.push(...(r.items ?? []));
			token = r.next;
		} while (token);
		return out;
	}

	function track(key: string, work: () => Promise<{ Readiness?: string }>): void {
		void work()
			.then((r) => {
				readiness[key] = r.Readiness ?? 'UNKNOWN';
			})
			.catch(() => {
				readiness[key] = 'UNKNOWN';
			});
	}

	async function fetchCells(): Promise<void> {
		cells = await listAll(async (NextToken) => {
			const r = await client().send(new ListCellsCommand({ NextToken }));
			return { items: r.Cells, next: r.NextToken };
		});
		for (const c of cells) {
			track(`cell:${c.CellName}`, () =>
				client().send(new GetCellReadinessSummaryCommand({ CellName: c.CellName }))
			);
		}
	}

	async function fetchGroups(): Promise<void> {
		groups = await listAll(async (NextToken) => {
			const r = await client().send(new ListRecoveryGroupsCommand({ NextToken }));
			return { items: r.RecoveryGroups, next: r.NextToken };
		});
		for (const g of groups) {
			track(`group:${g.RecoveryGroupName}`, () =>
				client().send(
					new GetRecoveryGroupReadinessSummaryCommand({ RecoveryGroupName: g.RecoveryGroupName })
				)
			);
		}
		if (cells.length === 0) await fetchCells();
	}

	async function fetchSets(): Promise<void> {
		sets = await listAll(async (NextToken) => {
			const r = await client().send(new ListResourceSetsCommand({ NextToken }));
			return { items: r.ResourceSets, next: r.NextToken };
		});
		if (cells.length === 0) await fetchCells();
	}

	async function fetchChecks(): Promise<void> {
		checks = await listAll(async (NextToken) => {
			const r = await client().send(new ListReadinessChecksCommand({ NextToken }));
			return { items: r.ReadinessChecks, next: r.NextToken };
		});
		for (const c of checks) {
			track(`check:${c.ReadinessCheckName}`, () =>
				client().send(new GetReadinessCheckStatusCommand({ ReadinessCheckName: c.ReadinessCheckName }))
			);
		}
		if (sets.length === 0) await fetchSets();
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		groups: () => fetchGroups().catch(rethrowDescribed),
		cells: () => fetchCells().catch(rethrowDescribed),
		sets: () => fetchSets().catch(rethrowDescribed),
		checks: () => fetchChecks().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	onRegionChange(() => {
		untrack(() => {
			cells = [];
			sets = [];
			void tabLoader.refresh(activeTab);
		});
	});

	const q = $derived(searchQuery.toLowerCase());
	const fGroups = $derived(groups.filter((g) => (g.RecoveryGroupName ?? '').toLowerCase().includes(q)));
	const fCells = $derived(cells.filter((c) => (c.CellName ?? '').toLowerCase().includes(q)));
	const fSets = $derived(sets.filter((s) => (s.ResourceSetName ?? '').toLowerCase().includes(q)));
	const fChecks = $derived(checks.filter((c) => (c.ReadinessCheckName ?? '').toLowerCase().includes(q)));
	const activeTabError = $derived(tabLoader.getError(activeTab));

	function cellName(arn: string): string {
		return cells.find((c) => c.CellArn === arn)?.CellName ?? arn.split('/').at(-1) ?? arn;
	}

	// --- create ---

	let groupModal = $state<Modal | null>(null);
	let cellModal = $state<Modal | null>(null);
	let setModal = $state<Modal | null>(null);
	let checkModal = $state<Modal | null>(null);
	let name = $state('');
	let pickedCells = $state<string[]>([]);
	let setType = $state(RESOURCE_SET_TYPES[0]);
	let setArns = $state('');
	let setScope = $state('');
	let checkSet = $state('');
	let saving = $state(false);
	let modalError = $state<string | null>(null);

	function openModal(m: Modal | null): void {
		modalError = null;
		name = '';
		pickedCells = [];
		setArns = '';
		setScope = cells[0]?.CellArn ?? '';
		checkSet = sets[0]?.ResourceSetName ?? '';
		m?.open();
	}

	async function submit(modal: Modal | null, tab: TabId, work: () => Promise<unknown>, ok: string): Promise<void> {
		saving = true;
		modalError = null;
		try {
			await work();
			toast.success(ok);
			modal?.close();
			await tabLoader.refresh(tab);
		} catch (e) {
			modalError = describeError(e);
			toast.error(modalError);
		} finally {
			saving = false;
		}
	}

	const submitCell = () =>
		submit(cellModal, 'cells', () => client().send(new CreateCellCommand({ CellName: name })), 'Cell created');
	const submitGroup = () =>
		submit(
			groupModal,
			'groups',
			() => client().send(new CreateRecoveryGroupCommand({ RecoveryGroupName: name, Cells: pickedCells })),
			'Recovery group created'
		);
	const submitSet = () =>
		submit(
			setModal,
			'sets',
			() =>
				client().send(
					new CreateResourceSetCommand({
						ResourceSetName: name,
						ResourceSetType: setType,
						Resources: setArns
							.split('\n')
							.map((a) => a.trim())
							.filter(Boolean)
							.map((ResourceArn) => ({
								ResourceArn,
								ReadinessScopes: setScope ? [setScope] : []
							}))
					})
				),
			'Resource set created'
		);
	const submitCheck = () =>
		submit(
			checkModal,
			'checks',
			() => client().send(new CreateReadinessCheckCommand({ ReadinessCheckName: name, ResourceSetName: checkSet })),
			'Readiness check created'
		);

	async function remove(title: string, message: string, tab: TabId, work: () => Promise<unknown>): Promise<void> {
		if (!(await confirmDestructive({ title, message }))) return;
		try {
			await work();
			toast.success(`${title} succeeded`);
			await tabLoader.refresh(tab);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	const delCell = (c: CellOutput) =>
		remove('Delete cell', `Delete cell ${c.CellName}?`, 'cells', () =>
			client().send(new DeleteCellCommand({ CellName: c.CellName }))
		);
	const delGroup = (g: RecoveryGroupOutput) =>
		remove('Delete recovery group', `Delete recovery group ${g.RecoveryGroupName}?`, 'groups', () =>
			client().send(new DeleteRecoveryGroupCommand({ RecoveryGroupName: g.RecoveryGroupName }))
		);
	const delSet = (s: ResourceSetOutput) =>
		remove('Delete resource set', `Delete resource set ${s.ResourceSetName}?`, 'sets', () =>
			client().send(new DeleteResourceSetCommand({ ResourceSetName: s.ResourceSetName }))
		);
	const delCheck = (c: ReadinessCheckOutput) =>
		remove('Delete readiness check', `Delete readiness check ${c.ReadinessCheckName}?`, 'checks', () =>
			client().send(new DeleteReadinessCheckCommand({ ReadinessCheckName: c.ReadinessCheckName }))
		);

	// --- readiness check detail ---

	let detailModal = $state<Modal | null>(null);
	let detailCheck = $state<ReadinessCheckOutput | null>(null);
	let detailResources = $state<ResourceResult[]>([]);
	let detailMessages = $state<Message[]>([]);
	let detailReadiness = $state('');
	let detailError = $state<string | null>(null);

	async function openDetail(c: ReadinessCheckOutput): Promise<void> {
		detailCheck = c;
		detailResources = [];
		detailMessages = [];
		detailReadiness = '';
		detailError = null;
		detailModal?.open();
		try {
			const r = await client().send(
				new GetReadinessCheckStatusCommand({ ReadinessCheckName: c.ReadinessCheckName })
			);
			detailResources = r.Resources ?? [];
			detailMessages = r.Messages ?? [];
			detailReadiness = r.Readiness ?? '';
		} catch (e) {
			detailError = describeError(e);
		}
	}

	function pick(list: string[], v: string, on: boolean): string[] {
		return on ? [...list.filter((x) => x !== v), v] : list.filter((x) => x !== v);
	}
</script>

{#snippet badge(value: string | undefined)}
	<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(value)}">{value ?? '—'}</span>
{/snippet}

{#snippet delBtn(label: string, onclick: () => void)}
	<button {onclick} title="Delete" aria-label={label} class="text-gray-400 hover:text-red-500"
		><Trash2 class="w-4 h-4" /></button
	>
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldCheck}
		title="Route 53 Recovery Readiness"
		description="Recovery groups, cells, resource sets and readiness checks"
		onRefresh={() => tabLoader.refresh(activeTab)}
		color="teal"
	>
		{#snippet actions()}
			{#if activeTab === 'groups'}
				<button onclick={() => openModal(groupModal)} class={btn}><Plus class="w-4 h-4" /> Create recovery group</button>
			{:else if activeTab === 'cells'}
				<button onclick={() => openModal(cellModal)} class={btn}><Plus class="w-4 h-4" /> Create cell</button>
			{:else if activeTab === 'sets'}
				<button onclick={() => openModal(setModal)} class={btn}><Plus class="w-4 h-4" /> Create resource set</button>
			{:else}
				<button onclick={() => openModal(checkModal)} class={btn}><Plus class="w-4 h-4" /> Create readiness check</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="teal" />
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
			{:else if activeTab === 'groups'}
				{#if fGroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No recovery groups found</div>
				{:else}
					<div class="overflow-x-auto"><table class="w-full text-sm">
						<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
							<th class={th}>Name</th><th class={th}>Readiness</th><th class={th}>Cells</th><th class={th}></th>
						</tr></thead>
						<tbody>
							{#each fGroups as g (g.RecoveryGroupArn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}><div class="font-medium">{g.RecoveryGroupName}</div><div class="text-xs text-gray-400 break-all">{g.RecoveryGroupArn}</div></td>
									<td class={td}><span data-testid="readiness-group-{g.RecoveryGroupName}">{@render badge(readiness[`group:${g.RecoveryGroupName}`])}</span></td>
									<td class={td}>{(g.Cells ?? []).map(cellName).join(', ') || '—'}</td>
									<td class={td}>{@render delBtn(`Delete recovery group ${g.RecoveryGroupName}`, () => delGroup(g))}</td>
								</tr>
							{/each}
						</tbody>
					</table></div>
				{/if}
			{:else if activeTab === 'cells'}
				{#if fCells.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No cells found</div>
				{:else}
					<div class="overflow-x-auto"><table class="w-full text-sm">
						<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
							<th class={th}>Name</th><th class={th}>Readiness</th><th class={th}>Nested cells</th><th class={th}>Parent groups</th><th class={th}></th>
						</tr></thead>
						<tbody>
							{#each fCells as c (c.CellArn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}><div class="font-medium">{c.CellName}</div><div class="text-xs text-gray-400 break-all">{c.CellArn}</div></td>
									<td class={td}><span data-testid="readiness-cell-{c.CellName}">{@render badge(readiness[`cell:${c.CellName}`])}</span></td>
									<td class={td}>{(c.Cells ?? []).map(cellName).join(', ') || '—'}</td>
									<td class="{td} text-xs break-all">{(c.ParentReadinessScopes ?? []).join(', ') || '—'}</td>
									<td class={td}>{@render delBtn(`Delete cell ${c.CellName}`, () => delCell(c))}</td>
								</tr>
							{/each}
						</tbody>
					</table></div>
				{/if}
			{:else if activeTab === 'sets'}
				{#if fSets.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource sets found</div>
				{:else}
					<div class="overflow-x-auto"><table class="w-full text-sm">
						<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
							<th class={th}>Name</th><th class={th}>Type</th><th class={th}>Resources</th><th class={th}></th>
						</tr></thead>
						<tbody>
							{#each fSets as s (s.ResourceSetArn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}><div class="font-medium">{s.ResourceSetName}</div><div class="text-xs text-gray-400 break-all">{s.ResourceSetArn}</div></td>
									<td class={td}>{s.ResourceSetType}</td>
									<td class="{td} text-xs">
										{#each s.Resources ?? [] as r (r.ResourceArn ?? r.ComponentId)}
											<div class="break-all">{r.ResourceArn ?? r.ComponentId}
												<span class="text-gray-400">scopes: {(r.ReadinessScopes ?? []).map(cellName).join(', ') || '—'}</span></div>
										{/each}
									</td>
									<td class={td}>{@render delBtn(`Delete resource set ${s.ResourceSetName}`, () => delSet(s))}</td>
								</tr>
							{/each}
						</tbody>
					</table></div>
				{/if}
			{:else if fChecks.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No readiness checks found</div>
			{:else}
				<div class="overflow-x-auto"><table class="w-full text-sm">
					<thead><tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class={th}>Name</th><th class={th}>Readiness</th><th class={th}>Resource set</th><th class={th}></th>
					</tr></thead>
					<tbody>
						{#each fChecks as c (c.ReadinessCheckArn)}
							<tr class="border-b border-gray-100 dark:border-gray-800">
								<td class={td}><div class="font-medium">{c.ReadinessCheckName}</div><div class="text-xs text-gray-400 break-all">{c.ReadinessCheckArn}</div></td>
								<td class={td}><span data-testid="readiness-check-{c.ReadinessCheckName}">{@render badge(readiness[`check:${c.ReadinessCheckName}`])}</span></td>
								<td class={td}>{c.ResourceSet}</td>
								<td class={td}>
									<div class="flex items-center gap-2 justify-end">
										<button onclick={() => openDetail(c)} title="View" aria-label="View readiness check {c.ReadinessCheckName}" class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button>
										{@render delBtn(`Delete readiness check ${c.ReadinessCheckName}`, () => delCheck(c))}
									</div>
								</td>
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

<Modal bind:this={cellModal} title="Create Cell">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="cell-name">Cell name</label>
			<input id="cell-name" class={input} bind:value={name} />
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => cellModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitCell} disabled={saving || !name} class={primaryBtn}>{saving ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={groupModal} title="Create Recovery Group">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="group-name">Recovery group name</label>
			<input id="group-name" class={input} bind:value={name} />
			<fieldset>
				<legend class="text-sm">Cells</legend>
				{#each cells as c (c.CellArn)}
					<label class="flex items-center gap-2 text-sm">
						<input type="checkbox" checked={pickedCells.includes(c.CellArn ?? '')} onchange={(e) => (pickedCells = pick(pickedCells, c.CellArn ?? '', (e.target as HTMLInputElement).checked))} />
						{c.CellName}
					</label>
				{/each}
			</fieldset>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => groupModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitGroup} disabled={saving || !name} class={primaryBtn}>{saving ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={setModal} title="Create Resource Set">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="set-name">Resource set name</label>
			<input id="set-name" class={input} bind:value={name} />
			<label class="block text-sm" for="set-type">Resource type</label>
			<select id="set-type" class={input} bind:value={setType}>
				{#each RESOURCE_SET_TYPES as t (t)}<option value={t}>{t}</option>{/each}
			</select>
			<label class="block text-sm" for="set-arns">Resource ARNs (one per line)</label>
			<textarea id="set-arns" rows="3" class={input} bind:value={setArns}></textarea>
			<label class="block text-sm" for="set-scope">Readiness scope (cell)</label>
			<select id="set-scope" class={input} bind:value={setScope}>
				<option value="">None</option>
				{#each cells as c (c.CellArn)}<option value={c.CellArn}>{c.CellName}</option>{/each}
			</select>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => setModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitSet} disabled={saving || !name || !setArns.trim()} class={primaryBtn}>{saving ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={checkModal} title="Create Readiness Check">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="check-name">Readiness check name</label>
			<input id="check-name" class={input} bind:value={name} />
			<label class="block text-sm" for="check-set">Resource set</label>
			<select id="check-set" class={input} bind:value={checkSet}>
				{#each sets as s (s.ResourceSetArn)}<option value={s.ResourceSetName}>{s.ResourceSetName}</option>{/each}
			</select>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => checkModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitCheck} disabled={saving || !name || !checkSet} class={primaryBtn}>{saving ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Readiness Check Status">
	{#snippet children()}
		{#if detailCheck}
			<div class="space-y-3 text-sm">
				<p class="font-medium">{detailCheck.ReadinessCheckName} {@render badge(detailReadiness)}</p>
				{#if detailError}<p class="text-red-600 dark:text-red-400">{detailError}</p>{/if}
				{#each detailResources as r (r.ResourceArn ?? r.ComponentId)}
					<div class="border-b border-gray-100 dark:border-gray-800 pb-2">
						<div class="break-all">{r.ResourceArn ?? r.ComponentId}</div>
						<div class="text-xs text-gray-500">{@render badge(r.Readiness)} checked {formatDate(r.LastCheckedTimestamp)}</div>
					</div>
				{/each}
				{#each detailMessages as m, i (i)}
					<p class="text-xs text-amber-700 dark:text-amber-400">{m.MessageText}</p>
				{/each}
			</div>
		{/if}
	{/snippet}
</Modal>
