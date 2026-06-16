<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getRoute53Client } from '$lib/aws-client';
	import {
		ListHostedZonesCommand,
		ListResourceRecordSetsCommand,
		CreateHostedZoneCommand,
		DeleteHostedZoneCommand,
		ChangeResourceRecordSetsCommand,
		type HostedZone,
		type ResourceRecordSet
	} from '@aws-sdk/client-route-53';
	import { toast } from 'svelte-sonner';
	import { Globe, Search, RefreshCw, Plus, Trash2, ChevronRight } from 'lucide-svelte';

	const r53 = getRoute53Client();

	let loading = $state(false);
	let zones = $state<HostedZone[]>([]);
	let selectedZone = $state<HostedZone | null>(null);
	let records = $state<ResourceRecordSet[]>([]);
	let loadingRecords = $state(false);
	let searchQuery = $state('');
	let recordSearch = $state('');

	// Create Zone
	let showCreateZone = $state(false);
	let creatingZone = $state(false);
	let newZoneName = $state('');
	let newZonePrivate = $state(false);
	let newZoneComment = $state('');

	// Create Record
	let showCreateRecord = $state(false);
	let creatingRecord = $state(false);
	let newRecordName = $state('');
	let newRecordType = $state<'A' | 'AAAA' | 'CNAME' | 'MX' | 'TXT' | 'NS' | 'SOA' | 'PTR' | 'SRV' | 'CAA' | 'DS' | 'HTTPS' | 'SVCB' | 'SSHFP' | 'TLSA' | 'NAPTR' | 'SPF'>('A');
	let newRecordTTL = $state(300);
	let newRecordValues = $state('');

	const filteredZones = $derived(
		zones.filter((z) => !searchQuery || (z.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredRecords = $derived(
		records.filter((r) => !recordSearch || (r.Name ?? '').toLowerCase().includes(recordSearch.toLowerCase()) || (r.Type ?? '').includes(recordSearch.toUpperCase()))
	);

	const recordTypeColor = (type: string) => {
		const colors: Record<string, string> = {
			A: 'blue', AAAA: 'purple', CNAME: 'green', MX: 'yellow', TXT: 'gray',
			NS: 'orange', SOA: 'red', PTR: 'teal', SRV: 'pink',
			CAA: 'indigo', DS: 'violet', HTTPS: 'cyan', SVCB: 'rose',
			SSHFP: 'amber', TLSA: 'lime', NAPTR: 'sky', SPF: 'emerald'
		};
		return colors[type] ?? 'gray';
	};

	async function loadZones() {
		loading = true;
		try {
			const resp = await r53.send(new ListHostedZonesCommand({ MaxItems: 50 }));
			zones = resp.HostedZones ?? [];
		} catch (e) {
			toast.error('Failed to load hosted zones: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectZone(zone: HostedZone) {
		selectedZone = zone;
		recordSearch = '';
		loadingRecords = true;
		try {
			const zoneId = (zone.Id ?? '').replace('/hostedzone/', '');
			const resp = await r53.send(new ListResourceRecordSetsCommand({ HostedZoneId: zoneId, MaxItems: 300 }));
			records = resp.ResourceRecordSets ?? [];
		} catch (e) {
			toast.error('Failed to load records: ' + String(e));
		} finally {
			loadingRecords = false;
		}
	}

	async function createZone() {
		if (!newZoneName.trim()) return;
		creatingZone = true;
		try {
			await r53.send(new CreateHostedZoneCommand({
				Name: newZoneName.trim(),
				CallerReference: `create-${Date.now()}`,
				HostedZoneConfig: {
					Comment: newZoneComment.trim() || undefined,
					PrivateZone: newZonePrivate
				}
			}));
			toast.success(`Hosted zone "${newZoneName}" created`);
			showCreateZone = false;
			newZoneName = '';
			newZoneComment = '';
			newZonePrivate = false;
			await loadZones();
		} catch (e) {
			toast.error('Failed to create zone: ' + String(e));
		} finally {
			creatingZone = false;
		}
	}

	async function deleteZone(zoneId: string, zoneName: string) {
		if (!await confirmDestructive({ title: 'Delete Hosted Zone', message: `Delete hosted zone "${zoneName}"? All DNS records will be permanently removed.` })) return;
		try {
			const id = zoneId.replace('/hostedzone/', '');
			await r53.send(new DeleteHostedZoneCommand({ Id: id }));
			toast.success(`Zone "${zoneName}" deleted`);
			if (selectedZone?.Id === zoneId) selectedZone = null;
			await loadZones();
		} catch (e) {
			toast.error('Failed to delete zone: ' + String(e));
		}
	}

	async function createRecord() {
		if (!selectedZone || !newRecordName.trim() || !newRecordValues.trim()) return;
		creatingRecord = true;
		try {
			const zoneId = (selectedZone.Id ?? '').replace('/hostedzone/', '');
			const valueLines = newRecordValues.split('\n').filter((v) => v.trim()).map((v) => ({ Value: v.trim() }));
			await r53.send(new ChangeResourceRecordSetsCommand({
				HostedZoneId: zoneId,
				ChangeBatch: {
					Changes: [{
						Action: 'CREATE',
						ResourceRecordSet: {
							Name: newRecordName.trim(),
							Type: newRecordType,
							TTL: newRecordTTL,
							ResourceRecords: valueLines
						}
					}]
				}
			}));
			toast.success(`Record "${newRecordName}" created`);
			showCreateRecord = false;
			newRecordName = '';
			newRecordValues = '';
			await selectZone(selectedZone);
		} catch (e) {
			toast.error('Failed to create record: ' + String(e));
		} finally {
			creatingRecord = false;
		}
	}

	async function deleteRecord(record: ResourceRecordSet) {
		if (!selectedZone || !await confirmDestructive({ title: 'Delete DNS Record', message: `Delete ${record.Type} record "${record.Name}"? DNS resolution for this record will stop immediately.` })) return;
		try {
			const zoneId = (selectedZone.Id ?? '').replace('/hostedzone/', '');
			await r53.send(new ChangeResourceRecordSetsCommand({
				HostedZoneId: zoneId,
				ChangeBatch: {
					Changes: [{
						Action: 'DELETE',
						ResourceRecordSet: record
					}]
				}
			}));
			toast.success(`Record "${record.Name}" deleted`);
			await selectZone(selectedZone);
		} catch (e) {
			toast.error('Failed to delete record: ' + String(e));
		}
	}

	const recordTypes = ['A', 'AAAA', 'CNAME', 'MX', 'TXT', 'NS', 'SOA', 'PTR', 'SRV', 'CAA', 'DS', 'HTTPS', 'SVCB', 'SSHFP', 'TLSA', 'NAPTR', 'SPF'];

	onMount(loadZones);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Route 53</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Scalable DNS and domain management</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadZones} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateZone = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Hosted Zone
			</button>
		</div>
	</div>

	{#if selectedZone}
		<!-- Zone Detail -->
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-2 text-sm">
				<button onclick={() => { selectedZone = null; records = []; }} class="text-indigo-600 hover:underline">Hosted Zones</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedZone.Name}</span>
				<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium ${selectedZone.Config?.PrivateZone ? 'bg-purple-100 text-purple-700' : 'bg-blue-100 text-blue-700'}`}>
					{selectedZone.Config?.PrivateZone ? 'Private' : 'Public'}
				</span>
			</div>
			<div class="flex items-center gap-2">
				<button onclick={() => (showCreateRecord = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Record
				</button>
				<button onclick={() => deleteZone(selectedZone?.Id ?? '', selectedZone?.Name ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete Zone</button>
			</div>
		</div>

		<!-- Zone Info -->
		<div class="grid grid-cols-3 gap-4">
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">Zone ID</div>
				<div class="text-sm font-mono mt-1">{(selectedZone.Id ?? '').replace('/hostedzone/', '')}</div>
			</div>
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">Record Count</div>
				<div class="text-sm font-semibold mt-1">{selectedZone.ResourceRecordSetCount ?? records.length}</div>
			</div>
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">Comment</div>
				<div class="text-sm mt-1">{selectedZone.Config?.Comment ?? '-'}</div>
			</div>
		</div>

		<!-- Record Search -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={recordSearch} type="text" placeholder="Search records by name or type..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loadingRecords}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">TTL</th>
							<th class="px-4 py-3 text-left">Values</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredRecords as record}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
								<td class="px-4 py-3 font-mono text-xs">{record.Name}</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-mono font-medium bg-${recordTypeColor(record.Type ?? '')}-100 text-${recordTypeColor(record.Type ?? '')}-700`}>{record.Type}</span>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">{record.TTL ?? 'Alias'}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">
									{#if record.AliasTarget}
										<span class="text-indigo-600">→ {record.AliasTarget.DNSName}</span>
									{:else}
										{(record.ResourceRecords ?? []).map((r) => r.Value).join(', ')}
									{/if}
								</td>
								<td class="px-4 py-3">
									{#if record.Type !== 'NS' && record.Type !== 'SOA'}
										<button onclick={() => deleteRecord(record)} class="text-red-500 hover:text-red-700 p-1">
											<Trash2 class="w-4 h-4" />
										</button>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

	{:else}
		<!-- Zone List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search hosted zones..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredZones.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No hosted zones found</p>
				<p class="text-sm mt-1">Create a hosted zone to manage DNS records</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Zone Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Record Count</th>
							<th class="px-4 py-3 text-left">Comment</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredZones as zone}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectZone(zone)} class="text-indigo-600 dark:text-indigo-400 hover:underline font-medium">{zone.Name}</button>
								</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium ${zone.Config?.PrivateZone ? 'bg-purple-100 text-purple-700' : 'bg-blue-100 text-blue-700'}`}>
										{zone.Config?.PrivateZone ? 'Private' : 'Public'}
									</span>
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{zone.ResourceRecordSetCount}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{zone.Config?.Comment ?? '-'}</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteZone(zone.Id ?? '', zone.Name ?? '')} class="text-red-500 hover:text-red-700 p-1">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Hosted Zone Modal -->
{#if showCreateZone}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Hosted Zone</h2>
			<div>
				<label for="zone-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Domain Name</label>
				<input id="zone-name" bind:value={newZoneName} type="text" placeholder="example.com" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="zone-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment (optional)</label>
				<input id="zone-comment" bind:value={newZoneComment} type="text" placeholder="Zone for production environment" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex items-center gap-3">
				<input id="zone-private" bind:checked={newZonePrivate} type="checkbox" class="rounded" />
				<label for="zone-private" class="text-sm text-gray-700 dark:text-gray-300">Private hosted zone</label>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateZone = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createZone} disabled={creatingZone || !newZoneName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingZone ? 'Creating...' : 'Create Zone'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Record Modal -->
{#if showCreateRecord && selectedZone}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Record</h2>
			<p class="text-sm text-gray-500">Zone: {selectedZone.Name}</p>
			<div>
				<label for="record-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Record Name</label>
				<input id="record-name" bind:value={newRecordName} type="text" placeholder="subdomain.example.com" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="record-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Type</label>
					<select id="record-type" bind:value={newRecordType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each recordTypes as t}<option value={t}>{t}</option>{/each}
					</select>
				</div>
				<div>
					<label for="record-ttl" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">TTL (seconds)</label>
					<input id="record-ttl" bind:value={newRecordTTL} type="number" min="1" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div>
				<label for="record-values" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Values (one per line)</label>
				<textarea id="record-values" bind:value={newRecordValues} rows={4} placeholder="192.0.2.1" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateRecord = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={createRecord} disabled={creatingRecord || !newRecordName.trim() || !newRecordValues.trim()} class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingRecord ? 'Creating...' : 'Create Record'}
				</button>
			</div>
		</div>
	</div>
{/if}
