<script lang="ts">
	// Replication Configuration Templates (services/mgn/PARITY.md family D's
	// account-level half, 4 ops: Create/Delete/Describe/Update). Unlike its
	// Launch Configuration Template sibling (nothing required),
	// CreateReplicationConfigurationTemplate has 11 REQUIRED fields --
	// confirmed by direct SDK read, PARITY.md flags this asymmetry
	// explicitly -- so this form fills them all with defensible defaults
	// rather than silently omitting any.
	import {
		DescribeReplicationConfigurationTemplatesCommand,
		CreateReplicationConfigurationTemplateCommand,
		UpdateReplicationConfigurationTemplateCommand,
		DeleteReplicationConfigurationTemplateCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type ReplicationConfigurationTemplate,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let templates = $state<ReplicationConfigurationTemplate[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchTemplates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeReplicationConfigurationTemplatesCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		templates = reset ? (resp.items ?? []) : [...templates, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchTemplates(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchTemplates(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		templates.filter((t) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (t.replicationConfigurationTemplateID ?? '').toLowerCase().includes(q);
		})
	);

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newSubnetId = $state('');
	let newInstanceType = $state('m5.large');
	let newSecurityGroupIDs = $state('');
	let newAssociateDefaultSg = $state(true);
	let newUseDedicated = $state(false);
	let newDefaultLargeStagingDiskType = $state<'GP2' | 'GP3' | 'ST1'>('GP3');
	let newEbsEncryption = $state<'DEFAULT' | 'CUSTOM'>('DEFAULT');
	let newBandwidthThrottling = $state(0);
	let newDataPlaneRouting = $state<'PRIVATE_IP' | 'PUBLIC_IP'>('PRIVATE_IP');
	let newCreatePublicIP = $state(false);

	function openCreate(): void {
		createError = null;
		newSubnetId = '';
		newInstanceType = 'm5.large';
		newSecurityGroupIDs = '';
		newAssociateDefaultSg = true;
		newUseDedicated = false;
		newDefaultLargeStagingDiskType = 'GP3';
		newEbsEncryption = 'DEFAULT';
		newBandwidthThrottling = 0;
		newDataPlaneRouting = 'PRIVATE_IP';
		newCreatePublicIP = false;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!newSubnetId.trim()) {
			createError = 'Staging area subnet ID is required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			await client().send(
				new CreateReplicationConfigurationTemplateCommand({
					stagingAreaSubnetId: newSubnetId.trim(),
					associateDefaultSecurityGroup: newAssociateDefaultSg,
					replicationServersSecurityGroupsIDs: newSecurityGroupIDs
						.split(/[\n,]/)
						.map((s) => s.trim())
						.filter(Boolean),
					replicationServerInstanceType: newInstanceType.trim() || 'm5.large',
					useDedicatedReplicationServer: newUseDedicated,
					defaultLargeStagingDiskType: newDefaultLargeStagingDiskType,
					ebsEncryption: newEbsEncryption,
					bandwidthThrottling: newBandwidthThrottling,
					dataPlaneRouting: newDataPlaneRouting,
					createPublicIP: newCreatePublicIP,
					stagingAreaTags: {}
				})
			);
			toast.success('Replication configuration template created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
			toast.error(createError);
		} finally {
			creating = false;
		}
	}

	async function deleteTemplate(t: ReplicationConfigurationTemplate): Promise<void> {
		if (!t.replicationConfigurationTemplateID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete replication configuration template',
			message: `Delete template ${t.replicationConfigurationTemplateID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteReplicationConfigurationTemplateCommand({ replicationConfigurationTemplateID: t.replicationConfigurationTemplateID })
			);
			toast.success('Template deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<ReplicationConfigurationTemplate | null>(null);
	let editError = $state<string | null>(null);
	let editInstanceType = $state('');
	let editBandwidthThrottling = $state(0);
	let editCreatePublicIP = $state(false);

	function openDetail(t: ReplicationConfigurationTemplate): void {
		viewed = t;
		editError = null;
		editInstanceType = t.replicationServerInstanceType ?? '';
		editBandwidthThrottling = t.bandwidthThrottling ?? 0;
		editCreatePublicIP = t.createPublicIP ?? false;
		detailModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!viewed?.replicationConfigurationTemplateID) return;
		editError = null;
		try {
			viewed = await client().send(
				new UpdateReplicationConfigurationTemplateCommand({
					replicationConfigurationTemplateID: viewed.replicationConfigurationTemplateID,
					replicationServerInstanceType: editInstanceType.trim() || undefined,
					bandwidthThrottling: editBandwidthThrottling,
					createPublicIP: editCreatePublicIP
				})
			);
			toast.success('Template updated');
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.replicationConfigurationTemplateID) return;
		const arn = taggableArn('replication-configuration-template', viewed.replicationConfigurationTemplateID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.replicationConfigurationTemplateID) return;
		const arn = taggableArn('replication-configuration-template', viewed.replicationConfigurationTemplateID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const columns = defineColumns<ReplicationConfigurationTemplate>([
		{ key: 'replicationConfigurationTemplateID', label: 'Template ID' },
		{ key: 'replicationServerInstanceType', label: 'Instance Type' },
		{ key: 'dataPlaneRouting', label: 'Data Plane Routing' }
	]);
</script>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Create template</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(t: ReplicationConfigurationTemplate)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(t)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteTemplate(t)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(t) => t.replicationConfigurationTemplateID ?? ''}
	columns={[...columns, { key: 'actions', label: '', render: rowActions }]}
	{loading}
	emptyMessage="No replication configuration templates found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Replication Configuration Template">
	{#snippet children()}
		<div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
			<label class="flex flex-col gap-1 text-sm">Staging area subnet ID
				<input bind:value={newSubnetId} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Replication server instance type
				<input bind:value={newInstanceType} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Replication server security group IDs (comma or newline separated)
				<textarea bind:value={newSecurityGroupIDs} rows="2" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</label>
			<label class="flex flex-col gap-1 text-sm">Default large staging disk type
				<select bind:value={newDefaultLargeStagingDiskType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="GP2">GP2</option>
					<option value="GP3">GP3</option>
					<option value="ST1">ST1</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm">EBS encryption
				<select bind:value={newEbsEncryption} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="DEFAULT">DEFAULT</option>
					<option value="CUSTOM">CUSTOM</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm">Data plane routing
				<select bind:value={newDataPlaneRouting} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="PRIVATE_IP">PRIVATE_IP</option>
					<option value="PUBLIC_IP">PUBLIC_IP</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm">Bandwidth throttling (Mbps, 0 = unlimited)
				<input type="number" min="0" bind:value={newBandwidthThrottling} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newAssociateDefaultSg} /> Associate default security group</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newUseDedicated} /> Use dedicated replication server</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newCreatePublicIP} /> Create public IP</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Template {viewed?.replicationConfigurationTemplateID ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<div class="grid grid-cols-2 gap-2 text-sm">
					<label class="flex flex-col gap-1">Instance type
						<input bind:value={editInstanceType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex flex-col gap-1">Bandwidth throttling (Mbps)
						<input type="number" min="0" bind:value={editBandwidthThrottling} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex items-center gap-2"><input type="checkbox" bind:checked={editCreatePublicIP} /> Create public IP</label>
				</div>
				<button onclick={submitEdit} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Save</button>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.tags ?? {}} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
