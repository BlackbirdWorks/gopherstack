<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAccountClient } from '$lib/aws-client';
	import {
		GetContactInformationCommand,
		PutContactInformationCommand,
		GetAlternateContactCommand,
		PutAlternateContactCommand,
		DeleteAlternateContactCommand,
		ListRegionsCommand,
		EnableRegionCommand,
		DisableRegionCommand,
		GetAccountInformationCommand,
		PutAccountNameCommand,
		GetPrimaryEmailCommand,
		StartPrimaryEmailUpdateCommand,
		AcceptPrimaryEmailUpdateCommand,
		type ContactInformation,
		type AlternateContact,
		type Region
	} from '@aws-sdk/client-account';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { UserCog, Pencil, Trash2, Power, PowerOff } from 'lucide-svelte';

	// AWS Account Management is mostly get/put of a single account's own
	// settings, not CRUD over a collection of independent resources -- the
	// real API shape drives the page here rather than a generic list+CRUD
	// template:
	//  - Contact Information: exactly one record per account (Get/Put only
	//    -- there is no Delete for the primary contact anywhere in the API).
	//  - Alternate Contacts: the closest thing to a CRUD-shaped resource --
	//    exactly three fixed keys (BILLING/OPERATIONS/SECURITY), each with
	//    real Get/Put/Delete. Modeled as a 3-row table rather than a form,
	//    since it genuinely has per-key create/update/delete semantics.
	//  - Regions: listable, but regions are not created or deleted -- only
	//    Enable/Disable (opt-in status) is ever mutated.
	//  - Account & primary email: GetAccountInformation/PutAccountName
	//    operate on the caller's own account. GetPrimaryEmail/
	//    StartPrimaryEmailUpdate/AcceptPrimaryEmailUpdate are different: all
	//    three require an explicit target AccountId and can ONLY be called
	//    by an organization's management/delegated-admin identity against a
	//    *member* account -- the docs explicitly say "the management
	//    account can't specify its own AccountId" for these three. The UI
	//    below takes that AccountId as an explicit input rather than
	//    silently reusing GetAccountInformation's own AccountId, which
	//    would misrepresent what these three operations actually do.
	const client = regionalClient(getAccountClient);

	type TabId = 'contact' | 'alternate' | 'regions' | 'settings';
	const tabs: TabDef[] = [
		{ id: 'contact', label: 'Contact Information' },
		{ id: 'alternate', label: 'Alternate Contacts' },
		{ id: 'regions', label: 'Regions' },
		{ id: 'settings', label: 'Account & Email' }
	];
	let activeTab = $state<TabId>('contact');
	let searchQuery = $state('');

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

	function isNotFound(e: unknown): boolean {
		return !!e && typeof e === 'object' && (e as { name?: unknown }).name === 'ResourceNotFoundException';
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function statusClass(status: string | undefined): string {
		if (status === 'ENABLED' || status === 'ENABLED_BY_DEFAULT') {
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Contact information ---

	let contactInfo = $state<ContactInformation | null>(null);

	async function fetchContactInfo(): Promise<void> {
		try {
			const resp = await client().send(new GetContactInformationCommand({}));
			contactInfo = resp.ContactInformation ?? null;
		} catch (e) {
			if (isNotFound(e)) {
				contactInfo = null;
				return;
			}
			rethrowDescribed(e);
		}
	}

	// --- Alternate contacts ---

	const CONTACT_TYPES = ['BILLING', 'OPERATIONS', 'SECURITY'] as const;
	type AltContactType = (typeof CONTACT_TYPES)[number];
	type AltContactRow = { type: AltContactType; contact: AlternateContact | null };

	let alternateContacts = $state<AltContactRow[]>(CONTACT_TYPES.map((t) => ({ type: t, contact: null })));

	async function fetchAlternateContacts(): Promise<void> {
		const rows = await Promise.all(
			CONTACT_TYPES.map(async (t): Promise<AltContactRow> => {
				try {
					const resp = await client().send(new GetAlternateContactCommand({ AlternateContactType: t }));
					return { type: t, contact: resp.AlternateContact ?? null };
				} catch (e) {
					if (isNotFound(e)) return { type: t, contact: null };
					rethrowDescribed(e);
				}
			})
		);
		alternateContacts = rows;
	}

	// --- Regions ---

	let regions = $state<Region[]>([]);

	async function fetchRegions(): Promise<void> {
		const resp = await client().send(new ListRegionsCommand({}));
		regions = resp.Regions ?? [];
	}

	// --- Account info + primary email ---

	let accountInfo = $state<{ AccountId?: string; AccountName?: string; AccountCreatedDate?: Date; AccountState?: string } | null>(null);

	async function fetchAccountInfo(): Promise<void> {
		const resp = await client().send(new GetAccountInformationCommand({}));
		accountInfo = resp;
	}

	const tabLoader = createTabLoader<TabId>({
		// fetchContactInfo already funnels non-not-found errors through
		// describeError/rethrowDescribed internally (see its own try/catch) --
		// wrapping it again here would double-describe the message (e.g.
		// "Error: TooManyRequestsException (HTTP 429): ..." instead of
		// "TooManyRequestsException (HTTP 429): ...").
		contact: () => fetchContactInfo(),
		alternate: () => fetchAlternateContacts(),
		regions: () => fetchRegions().catch(rethrowDescribed),
		settings: () => fetchAccountInfo().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Nothing selected here is resource-id-keyed the way a drill-down page
	// would be (regions/alternate-contact types/account settings are all
	// singletons per account+region), but the underlying values are still
	// region-scoped -- reload whichever tab is active on region change.
	onRegionChange(() => {
		tabLoader.refresh(untrack(() => activeTab));
	});

	const filteredRegions = $derived(
		regions.filter((r) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (r.RegionName ?? '').toLowerCase().includes(q) || (r.RegionOptStatus ?? '').toLowerCase().includes(q);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Contact information edit ---

	let contactModal = $state<Modal | null>(null);
	let savingContact = $state(false);
	let contactError = $state<string | null>(null);
	let formFullName = $state('');
	let formAddressLine1 = $state('');
	let formCity = $state('');
	let formCountryCode = $state('');
	let formPhoneNumber = $state('');
	let formPostalCode = $state('');
	let formCompanyName = $state('');
	let formStateOrRegion = $state('');

	function openContactModal(): void {
		contactError = null;
		formFullName = contactInfo?.FullName ?? '';
		formAddressLine1 = contactInfo?.AddressLine1 ?? '';
		formCity = contactInfo?.City ?? '';
		formCountryCode = contactInfo?.CountryCode ?? '';
		formPhoneNumber = contactInfo?.PhoneNumber ?? '';
		formPostalCode = contactInfo?.PostalCode ?? '';
		formCompanyName = contactInfo?.CompanyName ?? '';
		formStateOrRegion = contactInfo?.StateOrRegion ?? '';
		contactModal?.open();
	}

	async function submitContact(): Promise<void> {
		const required = [
			['Full name', formFullName],
			['Address line 1', formAddressLine1],
			['City', formCity],
			['Country code', formCountryCode],
			['Phone number', formPhoneNumber],
			['Postal code', formPostalCode]
		];
		const missing = required.find(([, v]) => !v.trim());
		if (missing) {
			contactError = `${missing[0]} is required.`;
			return;
		}
		savingContact = true;
		contactError = null;
		try {
			await client().send(
				new PutContactInformationCommand({
					ContactInformation: {
						FullName: formFullName.trim(),
						AddressLine1: formAddressLine1.trim(),
						City: formCity.trim(),
						CountryCode: formCountryCode.trim(),
						PhoneNumber: formPhoneNumber.trim(),
						PostalCode: formPostalCode.trim(),
						CompanyName: formCompanyName.trim() || undefined,
						StateOrRegion: formStateOrRegion.trim() || undefined
					}
				})
			);
			toast.success('Contact information saved');
			contactModal?.close();
			await tabLoader.refresh('contact');
		} catch (e) {
			const msg = describeError(e);
			contactError = msg;
			toast.error(msg);
		} finally {
			savingContact = false;
		}
	}

	// --- Alternate contact edit / delete ---

	let altModal = $state<Modal | null>(null);
	let savingAlt = $state(false);
	let altError = $state<string | null>(null);
	let altType = $state<AltContactType>('BILLING');
	let altName = $state('');
	let altTitle = $state('');
	let altEmail = $state('');
	let altPhone = $state('');

	function openAltModal(row: AltContactRow): void {
		altError = null;
		altType = row.type;
		altName = row.contact?.Name ?? '';
		altTitle = row.contact?.Title ?? '';
		altEmail = row.contact?.EmailAddress ?? '';
		altPhone = row.contact?.PhoneNumber ?? '';
		altModal?.open();
	}

	async function submitAlt(): Promise<void> {
		if (!altName.trim() || !altTitle.trim() || !altEmail.trim() || !altPhone.trim()) {
			altError = 'Name, title, email, and phone number are all required.';
			return;
		}
		savingAlt = true;
		altError = null;
		try {
			await client().send(
				new PutAlternateContactCommand({
					AlternateContactType: altType,
					Name: altName.trim(),
					Title: altTitle.trim(),
					EmailAddress: altEmail.trim(),
					PhoneNumber: altPhone.trim()
				})
			);
			toast.success(`${altType} contact saved`);
			altModal?.close();
			await tabLoader.refresh('alternate');
		} catch (e) {
			const msg = describeError(e);
			altError = msg;
			toast.error(msg);
		} finally {
			savingAlt = false;
		}
	}

	async function deleteAlt(row: AltContactRow): Promise<void> {
		if (!row.contact) return;
		const confirmed = await confirmDestructive({
			title: 'Delete alternate contact',
			message: `Delete the ${row.type} alternate contact?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAlternateContactCommand({ AlternateContactType: row.type }));
			toast.success(`${row.type} contact deleted`);
			await tabLoader.refresh('alternate');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Region enable/disable ---

	async function toggleRegion(r: Region): Promise<void> {
		if (!r.RegionName || r.RegionOptStatus === 'ENABLED_BY_DEFAULT') return;
		try {
			if (r.RegionOptStatus === 'ENABLED') {
				await client().send(new DisableRegionCommand({ RegionName: r.RegionName }));
				toast.success(`${r.RegionName} disabled`);
			} else {
				await client().send(new EnableRegionCommand({ RegionName: r.RegionName }));
				toast.success(`${r.RegionName} enabled`);
			}
			await tabLoader.refresh('regions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Account name edit ---

	let nameModal = $state<Modal | null>(null);
	let savingName = $state(false);
	let nameError = $state<string | null>(null);
	let newAccountName = $state('');

	function openNameModal(): void {
		nameError = null;
		newAccountName = accountInfo?.AccountName ?? '';
		nameModal?.open();
	}

	async function submitName(): Promise<void> {
		if (!newAccountName.trim()) {
			nameError = 'Account name is required.';
			return;
		}
		savingName = true;
		nameError = null;
		try {
			await client().send(new PutAccountNameCommand({ AccountName: newAccountName.trim() }));
			toast.success('Account name updated');
			nameModal?.close();
			await tabLoader.refresh('settings');
		} catch (e) {
			const msg = describeError(e);
			nameError = msg;
			toast.error(msg);
		} finally {
			savingName = false;
		}
	}

	// --- Primary email (member account) ---

	let emailAccountId = $state('');
	let primaryEmail = $state<string | null>(null);
	let emailLookupError = $state<string | null>(null);
	let lookingUpEmail = $state(false);

	async function lookupPrimaryEmail(): Promise<void> {
		if (!emailAccountId.trim()) return;
		lookingUpEmail = true;
		emailLookupError = null;
		try {
			const resp = await client().send(new GetPrimaryEmailCommand({ AccountId: emailAccountId.trim() }));
			primaryEmail = resp.PrimaryEmail ?? null;
		} catch (e) {
			emailLookupError = describeError(e);
		} finally {
			lookingUpEmail = false;
		}
	}

	let startEmailModal = $state<Modal | null>(null);
	let startingEmailUpdate = $state(false);
	let startEmailError = $state<string | null>(null);
	let newPrimaryEmail = $state('');
	let startedEmailStatus = $state<string | null>(null);

	function openStartEmailModal(): void {
		startEmailError = null;
		newPrimaryEmail = '';
		startEmailModal?.open();
	}

	async function submitStartEmail(): Promise<void> {
		if (!emailAccountId.trim() || !newPrimaryEmail.trim()) {
			startEmailError = 'Account ID and new primary email are both required.';
			return;
		}
		startingEmailUpdate = true;
		startEmailError = null;
		try {
			const resp = await client().send(
				new StartPrimaryEmailUpdateCommand({ AccountId: emailAccountId.trim(), PrimaryEmail: newPrimaryEmail.trim() })
			);
			startedEmailStatus = resp.Status ?? null;
			toast.success('Primary email update started');
			startEmailModal?.close();
			acceptEmail = newPrimaryEmail.trim();
		} catch (e) {
			const msg = describeError(e);
			startEmailError = msg;
			toast.error(msg);
		} finally {
			startingEmailUpdate = false;
		}
	}

	let acceptEmailModal = $state<Modal | null>(null);
	let acceptingEmail = $state(false);
	let acceptEmailError = $state<string | null>(null);
	let acceptEmail = $state('');
	let acceptOtp = $state('');

	function openAcceptEmailModal(): void {
		acceptEmailError = null;
		acceptOtp = '';
		acceptEmailModal?.open();
	}

	async function submitAcceptEmail(): Promise<void> {
		if (!emailAccountId.trim() || !acceptEmail.trim() || !acceptOtp.trim()) {
			acceptEmailError = 'Account ID, primary email, and OTP are all required.';
			return;
		}
		acceptingEmail = true;
		acceptEmailError = null;
		try {
			const resp = await client().send(
				new AcceptPrimaryEmailUpdateCommand({
					AccountId: emailAccountId.trim(),
					PrimaryEmail: acceptEmail.trim(),
					Otp: acceptOtp.trim()
				})
			);
			startedEmailStatus = resp.Status ?? null;
			toast.success('Primary email update accepted');
			acceptEmailModal?.close();
			await lookupPrimaryEmail();
		} catch (e) {
			const msg = describeError(e);
			acceptEmailError = msg;
			toast.error(msg);
		} finally {
			acceptingEmail = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={UserCog}
		title="AWS Account"
		description="Account settings, contacts and regions"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'contact'}
				<button onclick={openContactModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Pencil class="w-4 h-4" /> {contactInfo ? 'Edit' : 'Set'} contact info
				</button>
			{:else if activeTab === 'settings'}
				<button onclick={openNameModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Pencil class="w-4 h-4" /> Edit account name
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			{#if activeTab === 'regions'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'contact'}
				{#if tabLoader.isLoading('contact')}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if !contactInfo}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No contact information set</div>
				{:else}
					<dl class="text-sm grid grid-cols-1 sm:grid-cols-2 gap-4">
						<div><dt class="text-slate-500 dark:text-slate-400">Full name</dt><dd class="text-slate-900 dark:text-white">{contactInfo.FullName}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Company</dt><dd class="text-slate-900 dark:text-white">{contactInfo.CompanyName ?? '—'}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Address</dt><dd class="text-slate-900 dark:text-white">{contactInfo.AddressLine1}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">City</dt><dd class="text-slate-900 dark:text-white">{contactInfo.City}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">State / region</dt><dd class="text-slate-900 dark:text-white">{contactInfo.StateOrRegion ?? '—'}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Postal code</dt><dd class="text-slate-900 dark:text-white">{contactInfo.PostalCode}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Country code</dt><dd class="text-slate-900 dark:text-white">{contactInfo.CountryCode}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Phone</dt><dd class="text-slate-900 dark:text-white">{contactInfo.PhoneNumber}</dd></div>
					</dl>
				{/if}
			{:else if activeTab === 'alternate'}
				{#snippet altContactCell(row: AltContactRow)}
					<span class="text-slate-900 dark:text-white">{row.contact ? `${row.contact.Name} (${row.contact.EmailAddress})` : '(not set)'}</span>
				{/snippet}
				{#snippet altActionsCell(row: AltContactRow)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAltModal(row)} title="Edit" aria-label="Edit {row.type} contact" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						{#if row.contact}
							<button onclick={() => deleteAlt(row)} title="Delete" aria-label="Delete {row.type} contact" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const altColumns = defineColumns<AltContactRow>([
					{ key: 'type', label: 'Type' },
					{ key: 'contact', label: 'Contact', render: altContactCell },
					{ key: 'actions', label: '', render: altActionsCell }
				])}
				<DataTable
					rows={alternateContacts}
					rowKey={(row) => row.type}
					columns={altColumns}
					loading={tabLoader.isLoading('alternate')}
					emptyMessage="No alternate contact types"
				/>
			{:else if activeTab === 'regions'}
				{#snippet regionStatusCell(r: Region)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(r.RegionOptStatus)}">{r.RegionOptStatus ?? '—'}</span>
				{/snippet}
				{#snippet regionActionsCell(r: Region)}
					{#if r.RegionOptStatus !== 'ENABLED_BY_DEFAULT'}
						<div class="flex justify-end">
							{#if r.RegionOptStatus === 'ENABLED'}
								<button onclick={() => toggleRegion(r)} title="Disable" aria-label="Disable {r.RegionName}" class="text-gray-400 hover:text-red-500"><PowerOff class="w-4 h-4" /></button>
							{:else}
								<button onclick={() => toggleRegion(r)} title="Enable" aria-label="Enable {r.RegionName}" class="text-gray-400 hover:text-green-500"><Power class="w-4 h-4" /></button>
							{/if}
						</div>
					{/if}
				{/snippet}
				{@const regionColumns = defineColumns<Region>([
					{ key: 'RegionName', label: 'Region' },
					{ key: 'RegionOptStatus', label: 'Opt status', render: regionStatusCell },
					{ key: 'actions', label: '', render: regionActionsCell }
				])}
				<DataTable
					rows={filteredRegions}
					rowKey={(r) => r.RegionName ?? ''}
					columns={regionColumns}
					loading={tabLoader.isLoading('regions')}
					emptyMessage="No regions found"
				/>
			{:else if activeTab === 'settings'}
				{#if tabLoader.isLoading('settings')}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if accountInfo}
					<dl class="text-sm grid grid-cols-1 sm:grid-cols-2 gap-4">
						<div><dt class="text-slate-500 dark:text-slate-400">Account ID</dt><dd class="text-slate-900 dark:text-white">{accountInfo.AccountId ?? '—'}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Account name</dt><dd class="text-slate-900 dark:text-white">{accountInfo.AccountName ?? '—'}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{accountInfo.AccountState ?? '—'}</dd></div>
						<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{accountInfo.AccountCreatedDate ? new Date(accountInfo.AccountCreatedDate).toLocaleString() : '—'}</dd></div>
					</dl>
				{/if}

				<div class="mt-6 border-t border-slate-200 dark:border-slate-700 pt-4">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200 mb-1">Primary email (organization member account)</h3>
					<p class="text-xs text-slate-500 dark:text-slate-400 mb-3">
						GetPrimaryEmail / StartPrimaryEmailUpdate / AcceptPrimaryEmailUpdate can only target a member account of an
						organization from the management or delegated-admin account -- they cannot be used on the caller's own account.
					</p>
					<div class="flex flex-wrap items-end gap-2">
						<div>
							<label for="acct-email-account-id" class="text-sm text-slate-600 dark:text-slate-300">Member account ID</label>
							<input id="acct-email-account-id" bind:value={emailAccountId} placeholder="123456789012" class="mt-1 block px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
						<button onclick={lookupPrimaryEmail} disabled={lookingUpEmail || !emailAccountId.trim()} class="px-3 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{lookingUpEmail ? 'Looking up…' : 'Get primary email'}</button>
						<button onclick={openStartEmailModal} disabled={!emailAccountId.trim()} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-slate-700 disabled:opacity-50">Start email update</button>
						<button onclick={openAcceptEmailModal} disabled={!emailAccountId.trim()} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-slate-700 disabled:opacity-50">Accept email update</button>
					</div>
					{#if emailLookupError}
						<p class="mt-2 text-sm text-red-600 dark:text-red-400">{emailLookupError}</p>
					{:else if primaryEmail}
						<p class="mt-2 text-sm text-slate-900 dark:text-white">Primary email: {primaryEmail}</p>
					{/if}
					{#if startedEmailStatus}
						<p class="mt-2 text-sm text-slate-500 dark:text-slate-400">Last update status: {startedEmailStatus}</p>
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={contactModal} title={contactInfo ? 'Edit Contact Information' : 'Set Contact Information'}>
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="acct-full-name" class="text-sm text-slate-600 dark:text-slate-300">Full name</label>
				<input id="acct-full-name" bind:value={formFullName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="acct-address1" class="text-sm text-slate-600 dark:text-slate-300">Address line 1</label>
				<input id="acct-address1" bind:value={formAddressLine1} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="acct-city" class="text-sm text-slate-600 dark:text-slate-300">City</label>
					<input id="acct-city" bind:value={formCity} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="acct-postal" class="text-sm text-slate-600 dark:text-slate-300">Postal code</label>
					<input id="acct-postal" bind:value={formPostalCode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="acct-country" class="text-sm text-slate-600 dark:text-slate-300">Country code</label>
					<input id="acct-country" bind:value={formCountryCode} placeholder="US" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="acct-state" class="text-sm text-slate-600 dark:text-slate-300">State / region</label>
					<input id="acct-state" bind:value={formStateOrRegion} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="acct-phone" class="text-sm text-slate-600 dark:text-slate-300">Phone number</label>
				<input id="acct-phone" bind:value={formPhoneNumber} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="acct-company" class="text-sm text-slate-600 dark:text-slate-300">Company name</label>
				<input id="acct-company" bind:value={formCompanyName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if contactError}
				<p class="text-sm text-red-600 dark:text-red-400">{contactError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => contactModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitContact} disabled={savingContact} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{savingContact ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={altModal} title="Alternate Contact">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">Type: {altType}</p>
			<div>
				<label for="alt-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="alt-name" bind:value={altName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="alt-title" class="text-sm text-slate-600 dark:text-slate-300">Title</label>
				<input id="alt-title" bind:value={altTitle} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="alt-email" class="text-sm text-slate-600 dark:text-slate-300">Email address</label>
				<input id="alt-email" bind:value={altEmail} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="alt-phone" class="text-sm text-slate-600 dark:text-slate-300">Phone number</label>
				<input id="alt-phone" bind:value={altPhone} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if altError}
				<p class="text-sm text-red-600 dark:text-red-400">{altError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => altModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAlt} disabled={savingAlt} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{savingAlt ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={nameModal} title="Edit Account Name">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="acct-new-name" class="text-sm text-slate-600 dark:text-slate-300">Account name</label>
				<input id="acct-new-name" bind:value={newAccountName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if nameError}
				<p class="text-sm text-red-600 dark:text-red-400">{nameError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => nameModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitName} disabled={savingName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{savingName ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={startEmailModal} title="Start Primary Email Update">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">Member account: {emailAccountId || '—'}</p>
			<div>
				<label for="acct-new-primary-email" class="text-sm text-slate-600 dark:text-slate-300">New primary email</label>
				<input id="acct-new-primary-email" bind:value={newPrimaryEmail} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if startEmailError}
				<p class="text-sm text-red-600 dark:text-red-400">{startEmailError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => startEmailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStartEmail} disabled={startingEmailUpdate} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{startingEmailUpdate ? 'Starting…' : 'Start update'}</button>
	{/snippet}
</Modal>

<Modal bind:this={acceptEmailModal} title="Accept Primary Email Update">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">Member account: {emailAccountId || '—'}</p>
			<div>
				<label for="acct-accept-email" class="text-sm text-slate-600 dark:text-slate-300">Primary email</label>
				<input id="acct-accept-email" bind:value={acceptEmail} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="acct-accept-otp" class="text-sm text-slate-600 dark:text-slate-300">OTP</label>
				<input id="acct-accept-otp" bind:value={acceptOtp} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if acceptEmailError}
				<p class="text-sm text-red-600 dark:text-red-400">{acceptEmailError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => acceptEmailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAcceptEmail} disabled={acceptingEmail} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{acceptingEmail ? 'Accepting…' : 'Accept'}</button>
	{/snippet}
</Modal>
