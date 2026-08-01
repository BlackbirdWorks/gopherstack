<script lang="ts">
	// IAM Roles Anywhere has three CRUD-able resource families -- Trust
	// Anchors, Profiles, CRLs -- plus a fourth, read-only Subjects family.
	// Subjects have no Create/Delete/Update operation in the real API at all
	// (GetSubject/ListSubjects only): a subject row is materialized by the
	// mTLS-authenticated CreateSession data-plane API whenever a certificate
	// first authenticates, which gopherstack's backend never populates
	// (services/rolesanywhere/PARITY.md gaps: "GetSubject/ListSubjects:
	// subjects store is never populated -- there is no CreateSession
	// endpoint in this service"). So the Subjects tab is intentionally
	// list-only with no create button -- that mirrors the real API's shape,
	// not a UI omission.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRolesAnywhereClient } from '$lib/aws-client';
	import {
		ListCrlsCommand,
		ListProfilesCommand,
		ListSubjectsCommand,
		ListTrustAnchorsCommand,
		GetTrustAnchorCommand,
		GetProfileCommand,
		GetCrlCommand,
		CreateTrustAnchorCommand,
		CreateProfileCommand,
		ImportCrlCommand,
		UpdateTrustAnchorCommand,
		UpdateProfileCommand,
		UpdateCrlCommand,
		DeleteTrustAnchorCommand,
		DeleteProfileCommand,
		DeleteCrlCommand,
		EnableTrustAnchorCommand,
		DisableTrustAnchorCommand,
		EnableProfileCommand,
		DisableProfileCommand,
		EnableCrlCommand,
		DisableCrlCommand,
		type CrlDetail,
		type ProfileDetail,
		type SubjectSummary,
		type TrustAnchorDetail
	} from '@aws-sdk/client-rolesanywhere';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { KeyRound, Plus, Trash2, Eye, Pencil, Power, PowerOff } from 'lucide-svelte';

	const client = regionalClient(getRolesAnywhereClient);

	type TabId = 'anchors' | 'profiles' | 'crls' | 'subjects';

	const tabs: TabDef[] = [
		{ id: 'anchors', label: 'Trust Anchors' },
		{ id: 'profiles', label: 'Profiles' },
		{ id: 'crls', label: 'CRLs' },
		{ id: 'subjects', label: 'Subjects' }
	];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function enabledClass(enabled: boolean | undefined): string {
		return enabled
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('anchors');
	let searchQuery = $state('');

	let anchors = $state<TrustAnchorDetail[]>([]);
	let profiles = $state<ProfileDetail[]>([]);
	let crls = $state<CrlDetail[]>([]);
	let subjects = $state<SubjectSummary[]>([]);

	async function fetchAnchors(): Promise<void> {
		const resp = await client().send(new ListTrustAnchorsCommand({}));
		anchors = resp.trustAnchors ?? [];
	}
	async function fetchProfiles(): Promise<void> {
		const resp = await client().send(new ListProfilesCommand({}));
		profiles = resp.profiles ?? [];
	}
	async function fetchCrls(): Promise<void> {
		const resp = await client().send(new ListCrlsCommand({}));
		crls = resp.crls ?? [];
	}
	async function fetchSubjects(): Promise<void> {
		const resp = await client().send(new ListSubjectsCommand({}));
		subjects = resp.subjects ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		anchors: () => fetchAnchors().catch(rethrowDescribed),
		profiles: () => fetchProfiles().catch(rethrowDescribed),
		crls: () => fetchCrls().catch(rethrowDescribed),
		subjects: () => fetchSubjects().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// A trust anchor/profile/CRL id is only unique within a region -- any
	// selected-resource drill-down state must not survive a region switch.
	onRegionChange(() => {
		detailModal?.close();
		viewedAnchor = null;
		viewedProfile = null;
		viewedCrl = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	const filteredAnchors = $derived(
		anchors.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (a.name ?? '').toLowerCase().includes(q) || (a.trustAnchorId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredProfiles = $derived(
		profiles.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (p.name ?? '').toLowerCase().includes(q) || (p.profileId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredCrls = $derived(
		crls.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.name ?? '').toLowerCase().includes(q) || (c.crlId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredSubjects = $derived(
		subjects.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.x509Subject ?? '').toLowerCase().includes(q) || (s.subjectId ?? '').toLowerCase().includes(q);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Trust Anchor create ---

	let createAnchorModal = $state<Modal | null>(null);
	let creatingAnchor = $state(false);
	let createAnchorError = $state<string | null>(null);
	let newAnchorName = $state('');
	let newAnchorSourceType = $state<'AWS_ACM_PCA' | 'CERTIFICATE_BUNDLE'>('CERTIFICATE_BUNDLE');
	let newAnchorAcmPcaArn = $state('');
	let newAnchorCertData = $state('');
	let newAnchorEnabled = $state(true);

	function openCreateAnchorModal(): void {
		createAnchorError = null;
		newAnchorName = '';
		newAnchorSourceType = 'CERTIFICATE_BUNDLE';
		newAnchorAcmPcaArn = '';
		newAnchorCertData = '';
		newAnchorEnabled = true;
		createAnchorModal?.open();
	}

	async function submitCreateAnchor(): Promise<void> {
		if (!newAnchorName) {
			createAnchorError = 'Name is required.';
			return;
		}
		if (newAnchorSourceType === 'AWS_ACM_PCA' && !newAnchorAcmPcaArn) {
			createAnchorError = 'ACM PCA ARN is required for source type AWS_ACM_PCA.';
			return;
		}
		if (newAnchorSourceType === 'CERTIFICATE_BUNDLE' && !newAnchorCertData) {
			createAnchorError = 'Certificate data (PEM) is required for source type CERTIFICATE_BUNDLE.';
			return;
		}
		creatingAnchor = true;
		createAnchorError = null;
		try {
			await client().send(
				new CreateTrustAnchorCommand({
					name: newAnchorName,
					enabled: newAnchorEnabled,
					source: {
						sourceType: newAnchorSourceType,
						sourceData:
							newAnchorSourceType === 'AWS_ACM_PCA'
								? { acmPcaArn: newAnchorAcmPcaArn }
								: { x509CertificateData: newAnchorCertData }
					}
				})
			);
			toast.success('Trust anchor created');
			createAnchorModal?.close();
			await tabLoader.refresh('anchors');
		} catch (e) {
			const msg = describeError(e);
			createAnchorError = msg;
			toast.error(msg);
		} finally {
			creatingAnchor = false;
		}
	}

	async function deleteAnchor(a: TrustAnchorDetail): Promise<void> {
		if (!a.trustAnchorId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete trust anchor',
			message: `Delete trust anchor "${a.name ?? a.trustAnchorId}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTrustAnchorCommand({ trustAnchorId: a.trustAnchorId }));
			toast.success('Trust anchor deleted');
			await tabLoader.refresh('anchors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleAnchorEnabled(a: TrustAnchorDetail): Promise<void> {
		if (!a.trustAnchorId) return;
		try {
			if (a.enabled) {
				await client().send(new DisableTrustAnchorCommand({ trustAnchorId: a.trustAnchorId }));
				toast.success('Trust anchor disabled');
			} else {
				await client().send(new EnableTrustAnchorCommand({ trustAnchorId: a.trustAnchorId }));
				toast.success('Trust anchor enabled');
			}
			await tabLoader.refresh('anchors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Profile create ---

	let createProfileModal = $state<Modal | null>(null);
	let creatingProfile = $state(false);
	let createProfileError = $state<string | null>(null);
	let newProfileName = $state('');
	let newProfileRoleArns = $state('');
	let newProfileDurationSeconds = $state(3600);
	let newProfileEnabled = $state(true);

	function openCreateProfileModal(): void {
		createProfileError = null;
		newProfileName = '';
		newProfileRoleArns = '';
		newProfileDurationSeconds = 3600;
		newProfileEnabled = true;
		createProfileModal?.open();
	}

	async function submitCreateProfile(): Promise<void> {
		const roleArns = newProfileRoleArns
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
		if (!newProfileName || roleArns.length === 0) {
			createProfileError = 'Name and at least one role ARN are required.';
			return;
		}
		creatingProfile = true;
		createProfileError = null;
		try {
			await client().send(
				new CreateProfileCommand({
					name: newProfileName,
					roleArns,
					durationSeconds: newProfileDurationSeconds,
					enabled: newProfileEnabled
				})
			);
			toast.success('Profile created');
			createProfileModal?.close();
			await tabLoader.refresh('profiles');
		} catch (e) {
			const msg = describeError(e);
			createProfileError = msg;
			toast.error(msg);
		} finally {
			creatingProfile = false;
		}
	}

	async function deleteProfile(p: ProfileDetail): Promise<void> {
		if (!p.profileId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete profile',
			message: `Delete profile "${p.name ?? p.profileId}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteProfileCommand({ profileId: p.profileId }));
			toast.success('Profile deleted');
			await tabLoader.refresh('profiles');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleProfileEnabled(p: ProfileDetail): Promise<void> {
		if (!p.profileId) return;
		try {
			if (p.enabled) {
				await client().send(new DisableProfileCommand({ profileId: p.profileId }));
				toast.success('Profile disabled');
			} else {
				await client().send(new EnableProfileCommand({ profileId: p.profileId }));
				toast.success('Profile enabled');
			}
			await tabLoader.refresh('profiles');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- CRL import (create) ---

	let createCrlModal = $state<Modal | null>(null);
	let creatingCrl = $state(false);
	let createCrlError = $state<string | null>(null);
	let newCrlName = $state('');
	let newCrlTrustAnchorArn = $state('');
	let newCrlData = $state('');
	let newCrlEnabled = $state(true);

	function openCreateCrlModal(): void {
		createCrlError = null;
		newCrlName = '';
		newCrlTrustAnchorArn = '';
		newCrlData = '';
		newCrlEnabled = true;
		createCrlModal?.open();
	}

	async function submitCreateCrl(): Promise<void> {
		if (!newCrlName || !newCrlTrustAnchorArn || !newCrlData) {
			createCrlError = 'Name, trust anchor ARN, and CRL data (PEM) are required.';
			return;
		}
		creatingCrl = true;
		createCrlError = null;
		try {
			await client().send(
				new ImportCrlCommand({
					name: newCrlName,
					trustAnchorArn: newCrlTrustAnchorArn,
					crlData: new TextEncoder().encode(newCrlData),
					enabled: newCrlEnabled
				})
			);
			toast.success('CRL imported');
			createCrlModal?.close();
			await tabLoader.refresh('crls');
		} catch (e) {
			const msg = describeError(e);
			createCrlError = msg;
			toast.error(msg);
		} finally {
			creatingCrl = false;
		}
	}

	async function deleteCrl(c: CrlDetail): Promise<void> {
		if (!c.crlId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete CRL',
			message: `Delete CRL "${c.name ?? c.crlId}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCrlCommand({ crlId: c.crlId }));
			toast.success('CRL deleted');
			await tabLoader.refresh('crls');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleCrlEnabled(c: CrlDetail): Promise<void> {
		if (!c.crlId) return;
		try {
			if (c.enabled) {
				await client().send(new DisableCrlCommand({ crlId: c.crlId }));
				toast.success('CRL disabled');
			} else {
				await client().send(new EnableCrlCommand({ crlId: c.crlId }));
				toast.success('CRL enabled');
			}
			await tabLoader.refresh('crls');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail / edit (shared modal per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'anchor' | 'profile' | 'crl' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedAnchor = $state<TrustAnchorDetail | null>(null);
	let viewedProfile = $state<ProfileDetail | null>(null);
	let viewedCrl = $state<CrlDetail | null>(null);

	async function openAnchorDetail(a: TrustAnchorDetail): Promise<void> {
		detailKind = 'anchor';
		viewedAnchor = null;
		detailError = null;
		detailModal?.open();
		if (!a.trustAnchorId) return;
		detailLoading = true;
		try {
			const resp = await client().send(new GetTrustAnchorCommand({ trustAnchorId: a.trustAnchorId }));
			viewedAnchor = resp.trustAnchor ?? null;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openProfileDetail(p: ProfileDetail): Promise<void> {
		detailKind = 'profile';
		viewedProfile = null;
		detailError = null;
		detailModal?.open();
		if (!p.profileId) return;
		detailLoading = true;
		try {
			const resp = await client().send(new GetProfileCommand({ profileId: p.profileId }));
			viewedProfile = resp.profile ?? null;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openCrlDetail(c: CrlDetail): Promise<void> {
		detailKind = 'crl';
		viewedCrl = null;
		detailError = null;
		detailModal?.open();
		if (!c.crlId) return;
		detailLoading = true;
		try {
			const resp = await client().send(new GetCrlCommand({ crlId: c.crlId }));
			viewedCrl = resp.crl ?? null;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	// --- Edit (name/source or name/roles or name -- per family) ---

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editAnchorId = $state('');
	let editAnchorName = $state('');
	let editProfileId = $state('');
	let editProfileName = $state('');
	let editProfileRoleArns = $state('');
	let editCrlId = $state('');
	let editCrlName = $state('');

	function openEditAnchor(a: TrustAnchorDetail): void {
		editError = null;
		editAnchorId = a.trustAnchorId ?? '';
		editAnchorName = a.name ?? '';
		editModal?.open();
	}

	async function submitEditAnchor(): Promise<void> {
		if (!editAnchorId || !editAnchorName) {
			editError = 'Name is required.';
			return;
		}
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateTrustAnchorCommand({ trustAnchorId: editAnchorId, name: editAnchorName }));
			toast.success('Trust anchor updated');
			editModal?.close();
			await tabLoader.refresh('anchors');
			if (viewedAnchor?.trustAnchorId === editAnchorId) await openAnchorDetail({ trustAnchorId: editAnchorId });
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}

	function openEditProfile(p: ProfileDetail): void {
		editError = null;
		editProfileId = p.profileId ?? '';
		editProfileName = p.name ?? '';
		editProfileRoleArns = (p.roleArns ?? []).join(', ');
		editModal?.open();
	}

	async function submitEditProfile(): Promise<void> {
		const roleArns = editProfileRoleArns
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
		if (!editProfileId || !editProfileName || roleArns.length === 0) {
			editError = 'Name and at least one role ARN are required.';
			return;
		}
		editing = true;
		editError = null;
		try {
			await client().send(
				new UpdateProfileCommand({ profileId: editProfileId, name: editProfileName, roleArns })
			);
			toast.success('Profile updated');
			editModal?.close();
			await tabLoader.refresh('profiles');
			if (viewedProfile?.profileId === editProfileId) await openProfileDetail({ profileId: editProfileId });
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}

	function openEditCrl(c: CrlDetail): void {
		editError = null;
		editCrlId = c.crlId ?? '';
		editCrlName = c.name ?? '';
		editModal?.open();
	}

	async function submitEditCrl(): Promise<void> {
		if (!editCrlId || !editCrlName) {
			editError = 'Name is required.';
			return;
		}
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateCrlCommand({ crlId: editCrlId, name: editCrlName }));
			toast.success('CRL updated');
			editModal?.close();
			await tabLoader.refresh('crls');
			if (viewedCrl?.crlId === editCrlId) await openCrlDetail({ crlId: editCrlId });
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={KeyRound}
		title="IAM Roles Anywhere"
		description="X.509-based access for non-AWS workloads"
		onRefresh={handleRefresh}
		color="emerald"
	>
		{#snippet actions()}
			{#if activeTab === 'anchors'}
				<button
					onclick={openCreateAnchorModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create trust anchor
				</button>
			{:else if activeTab === 'profiles'}
				<button
					onclick={openCreateProfileModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create profile
				</button>
			{:else if activeTab === 'crls'}
				<button
					onclick={openCreateCrlModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Import CRL
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="emerald" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'anchors'}
				{#snippet anchorEnabledCell(a: TrustAnchorDetail)}
					<span class="text-xs px-2 py-1 rounded-full {enabledClass(a.enabled)}">{a.enabled ? 'Enabled' : 'Disabled'}</span>
				{/snippet}
				{#snippet anchorActionsCell(a: TrustAnchorDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAnchorDetail(a)} title="View" aria-label="View trust anchor {a.name}" class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => toggleAnchorEnabled(a)} title={a.enabled ? 'Disable' : 'Enable'} aria-label="{a.enabled ? 'Disable' : 'Enable'} trust anchor {a.name}" class="text-gray-400 hover:text-emerald-500">
							{#if a.enabled}<PowerOff class="w-4 h-4" />{:else}<Power class="w-4 h-4" />{/if}
						</button>
						<button onclick={() => deleteAnchor(a)} title="Delete" aria-label="Delete trust anchor {a.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const anchorColumns = defineColumns<TrustAnchorDetail>([
					{ key: 'name', label: 'Name' },
					{ key: 'trustAnchorId', label: 'ID' },
					{ key: 'enabled', label: 'Status', render: anchorEnabledCell },
					{ key: 'actions', label: '', render: anchorActionsCell }
				])}
				<DataTable rows={filteredAnchors} rowKey={(a) => a.trustAnchorId ?? ''} columns={anchorColumns} loading={tabLoader.isLoading('anchors')} emptyMessage="No trust anchors found" />
			{:else if activeTab === 'profiles'}
				{#snippet profileEnabledCell(p: ProfileDetail)}
					<span class="text-xs px-2 py-1 rounded-full {enabledClass(p.enabled)}">{p.enabled ? 'Enabled' : 'Disabled'}</span>
				{/snippet}
				{#snippet profileActionsCell(p: ProfileDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openProfileDetail(p)} title="View" aria-label="View profile {p.name}" class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => toggleProfileEnabled(p)} title={p.enabled ? 'Disable' : 'Enable'} aria-label="{p.enabled ? 'Disable' : 'Enable'} profile {p.name}" class="text-gray-400 hover:text-emerald-500">
							{#if p.enabled}<PowerOff class="w-4 h-4" />{:else}<Power class="w-4 h-4" />{/if}
						</button>
						<button onclick={() => deleteProfile(p)} title="Delete" aria-label="Delete profile {p.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const profileColumns = defineColumns<ProfileDetail>([
					{ key: 'name', label: 'Name' },
					{ key: 'profileId', label: 'ID' },
					{ key: 'enabled', label: 'Status', render: profileEnabledCell },
					{ key: 'actions', label: '', render: profileActionsCell }
				])}
				<DataTable rows={filteredProfiles} rowKey={(p) => p.profileId ?? ''} columns={profileColumns} loading={tabLoader.isLoading('profiles')} emptyMessage="No profiles found" />
			{:else if activeTab === 'crls'}
				{#snippet crlEnabledCell(c: CrlDetail)}
					<span class="text-xs px-2 py-1 rounded-full {enabledClass(c.enabled)}">{c.enabled ? 'Enabled' : 'Disabled'}</span>
				{/snippet}
				{#snippet crlActionsCell(c: CrlDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCrlDetail(c)} title="View" aria-label="View CRL {c.name}" class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => toggleCrlEnabled(c)} title={c.enabled ? 'Disable' : 'Enable'} aria-label="{c.enabled ? 'Disable' : 'Enable'} CRL {c.name}" class="text-gray-400 hover:text-emerald-500">
							{#if c.enabled}<PowerOff class="w-4 h-4" />{:else}<Power class="w-4 h-4" />{/if}
						</button>
						<button onclick={() => deleteCrl(c)} title="Delete" aria-label="Delete CRL {c.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const crlColumns = defineColumns<CrlDetail>([
					{ key: 'name', label: 'Name' },
					{ key: 'crlId', label: 'ID' },
					{ key: 'enabled', label: 'Status', render: crlEnabledCell },
					{ key: 'actions', label: '', render: crlActionsCell }
				])}
				<DataTable rows={filteredCrls} rowKey={(c) => c.crlId ?? ''} columns={crlColumns} loading={tabLoader.isLoading('crls')} emptyMessage="No CRLs found" />
			{:else if activeTab === 'subjects'}
				<p class="text-xs text-slate-500 dark:text-slate-400">
					Subjects are populated automatically when a certificate authenticates via CreateSession -- there is no
					create, update, or delete operation for this resource in the real API.
				</p>
				{#snippet subjectEnabledCell(s: SubjectSummary)}
					<span class="text-xs px-2 py-1 rounded-full {enabledClass(s.enabled)}">{s.enabled ? 'Enabled' : 'Disabled'}</span>
				{/snippet}
				{@const subjectColumns = defineColumns<SubjectSummary>([
					{ key: 'x509Subject', label: 'Subject' },
					{ key: 'subjectId', label: 'ID' },
					{ key: 'enabled', label: 'Status', render: subjectEnabledCell }
				])}
				<DataTable rows={filteredSubjects} rowKey={(s) => s.subjectId ?? ''} columns={subjectColumns} loading={tabLoader.isLoading('subjects')} emptyMessage="No subjects found" />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createAnchorModal} title="Create Trust Anchor">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ra-anchor-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ra-anchor-name" bind:value={newAnchorName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ra-anchor-source-type" class="text-sm text-slate-600 dark:text-slate-300">Source type</label>
				<select id="ra-anchor-source-type" bind:value={newAnchorSourceType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="CERTIFICATE_BUNDLE">CERTIFICATE_BUNDLE</option>
					<option value="AWS_ACM_PCA">AWS_ACM_PCA</option>
				</select>
			</div>
			{#if newAnchorSourceType === 'AWS_ACM_PCA'}
				<div>
					<label for="ra-anchor-acmpca" class="text-sm text-slate-600 dark:text-slate-300">ACM PCA ARN</label>
					<input id="ra-anchor-acmpca" bind:value={newAnchorAcmPcaArn} placeholder="arn:aws:acm-pca:..." class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{:else}
				<div>
					<label for="ra-anchor-certdata" class="text-sm text-slate-600 dark:text-slate-300">Certificate data (PEM)</label>
					<textarea id="ra-anchor-certdata" bind:value={newAnchorCertData} rows={4} class="mt-1 w-full font-mono text-xs px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" placeholder="-----BEGIN CERTIFICATE-----"></textarea>
				</div>
			{/if}
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newAnchorEnabled} /> Enabled
			</label>
			{#if createAnchorError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAnchorError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createAnchorModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateAnchor} disabled={creatingAnchor} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{creatingAnchor ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createProfileModal} title="Create Profile">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ra-profile-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ra-profile-name" bind:value={newProfileName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ra-profile-rolearns" class="text-sm text-slate-600 dark:text-slate-300">Role ARNs (comma-separated)</label>
				<input id="ra-profile-rolearns" bind:value={newProfileRoleArns} placeholder="arn:aws:iam::123456789012:role/MyRole" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ra-profile-duration" class="text-sm text-slate-600 dark:text-slate-300">Duration (seconds)</label>
				<input id="ra-profile-duration" type="number" bind:value={newProfileDurationSeconds} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newProfileEnabled} /> Enabled
			</label>
			{#if createProfileError}
				<p class="text-sm text-red-600 dark:text-red-400">{createProfileError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createProfileModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateProfile} disabled={creatingProfile} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{creatingProfile ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createCrlModal} title="Import CRL">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ra-crl-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ra-crl-name" bind:value={newCrlName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ra-crl-anchor" class="text-sm text-slate-600 dark:text-slate-300">Trust anchor ARN</label>
				<select id="ra-crl-anchor" bind:value={newCrlTrustAnchorArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a trust anchor…</option>
					{#each anchors as a (a.trustAnchorId)}
						<option value={a.trustAnchorArn}>{a.name} ({a.trustAnchorId})</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="ra-crl-data" class="text-sm text-slate-600 dark:text-slate-300">CRL data (PEM)</label>
				<textarea id="ra-crl-data" bind:value={newCrlData} rows={4} class="mt-1 w-full font-mono text-xs px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" placeholder="-----BEGIN X509 CRL-----"></textarea>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newCrlEnabled} /> Enabled
			</label>
			{#if createCrlError}
				<p class="text-sm text-red-600 dark:text-red-400">{createCrlError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createCrlModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCrl} disabled={creatingCrl} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{creatingCrl ? 'Importing…' : 'Import'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title={detailKind === 'anchor' ? 'Trust Anchor' : detailKind === 'profile' ? 'Profile' : 'CRL'}>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'anchor' && viewedAnchor}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedAnchor.trustAnchorId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedAnchor.trustAnchorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedAnchor.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Source type</dt><dd class="text-slate-900 dark:text-white">{viewedAnchor.source?.sourceType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedAnchor.enabled ? 'Enabled' : 'Disabled'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAnchor.createdAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAnchor.updatedAt)}</dd></div>
			</dl>
		{:else if detailKind === 'profile' && viewedProfile}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedProfile.profileId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProfile.profileArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedProfile.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARNs</dt><dd class="text-slate-900 dark:text-white">{(viewedProfile.roleArns ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Duration (s)</dt><dd class="text-slate-900 dark:text-white">{viewedProfile.durationSeconds ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedProfile.enabled ? 'Enabled' : 'Disabled'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedProfile.createdAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedProfile.updatedAt)}</dd></div>
			</dl>
		{:else if detailKind === 'crl' && viewedCrl}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedCrl.crlId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedCrl.crlArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedCrl.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Trust anchor ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedCrl.trustAnchorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedCrl.enabled ? 'Enabled' : 'Disabled'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedCrl.createdAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedCrl.updatedAt)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'anchor' && viewedAnchor}
			<button type="button" onclick={() => viewedAnchor && openEditAnchor(viewedAnchor)} class="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'profile' && viewedProfile}
			<button type="button" onclick={() => viewedProfile && openEditProfile(viewedProfile)} class="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'crl' && viewedCrl}
			<button type="button" onclick={() => viewedCrl && openEditCrl(viewedCrl)} class="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={editModal} title={detailKind === 'anchor' ? 'Edit Trust Anchor' : detailKind === 'profile' ? 'Edit Profile' : 'Edit CRL'}>
	{#snippet children()}
		{#if detailKind === 'anchor'}
			<div class="space-y-3">
				<div>
					<label for="ra-edit-anchor-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
					<input id="ra-edit-anchor-name" bind:value={editAnchorName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{:else if detailKind === 'profile'}
			<div class="space-y-3">
				<div>
					<label for="ra-edit-profile-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
					<input id="ra-edit-profile-name" bind:value={editProfileName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ra-edit-profile-rolearns" class="text-sm text-slate-600 dark:text-slate-300">Role ARNs (comma-separated)</label>
					<input id="ra-edit-profile-rolearns" bind:value={editProfileRoleArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{:else if detailKind === 'crl'}
			<div class="space-y-3">
				<div>
					<label for="ra-edit-crl-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
					<input id="ra-edit-crl-name" bind:value={editCrlName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		{#if detailKind === 'anchor'}
			<button type="button" onclick={submitEditAnchor} disabled={editing} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{:else if detailKind === 'profile'}
			<button type="button" onclick={submitEditProfile} disabled={editing} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{:else if detailKind === 'crl'}
			<button type="button" onclick={submitEditCrl} disabled={editing} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{/if}
	{/snippet}
</Modal>
