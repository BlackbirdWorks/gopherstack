<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSESv2Client } from '$lib/aws-client';
	import {
		CreateEmailIdentityCommand,
		DeleteEmailIdentityCommand,
		ListEmailIdentitiesCommand,
		GetEmailIdentityCommand,
		CreateContactListCommand,
		DeleteContactListCommand,
		ListContactListsCommand,
		CreateEmailTemplateCommand,
		DeleteEmailTemplateCommand,
		ListEmailTemplatesCommand,
		SendEmailCommand,
		PutSuppressedDestinationCommand,
		DeleteSuppressedDestinationCommand,
		ListSuppressedDestinationsCommand,
		GetAccountCommand,
		ListContactsCommand,
		CreateContactCommand,
		DeleteContactCommand,
		UpdateContactCommand,
		type Contact
	} from '@aws-sdk/client-sesv2';
	import { toast } from 'svelte-sonner';
	import {
		Mail,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Send,
		FileText,
		CheckCircle,
		XCircle,
		Shield,
		Users,
		BarChart2,
		ChevronRight,
		Pencil
	} from 'lucide-svelte';

	const sesv2 = regionalClient(getSESv2Client);

	type Tab = 'identities' | 'contact-lists' | 'suppression' | 'templates' | 'send' | 'account';

	let loading = $state(false);
	let activeTab = $state<Tab>('identities');
	let searchQuery = $state('');

	// Identities
	let identities = $state<Array<{ IdentityName?: string; IdentityType?: string; SendingEnabled?: boolean }>>([]);
	let showAddIdentityModal = $state(false);
	let newIdentity = $state('');
	let addingIdentity = $state(false);

	// Contact lists
	let contactLists = $state<Array<{ ContactListName?: string; Description?: string }>>([]);
	let showAddContactListModal = $state(false);
	let newContactListName = $state('');
	let newContactListDesc = $state('');
	let addingContactList = $state(false);

	// Suppression list
	let suppressedAddresses = $state<Array<{ EmailAddress?: string; Reason?: string }>>([]);
	let showAddSuppressionModal = $state(false);
	let suppressEmail = $state('');
	let suppressReason = $state<'BOUNCE' | 'COMPLAINT'>('BOUNCE');
	let addingSuppressionEntry = $state(false);

	// Templates
	let templates = $state<Array<{ TemplateName?: string }>>([]);
	let showCreateTemplateModal = $state(false);
	let newTemplateName = $state('');
	let newTemplateSubject = $state('');
	let newTemplateHtml = $state('');
	let newTemplateText = $state('');
	let creatingTemplate = $state(false);

	// Send email
	let sendFrom = $state('');
	let sendTo = $state('');
	let sendSubject = $state('Test from GopherStack SES v2');
	let sendBody = $state('This is a test email sent via Amazon SES v2.');
	let sendHtml = $state('');
	let sending = $state(false);

	// Account
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let accountInfo = $state<any>(null);

	const filteredIdentities = $derived(
		identities.filter((id) =>
			(id.IdentityName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredContactLists = $derived(
		contactLists.filter((cl) =>
			(cl.ContactListName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredSuppressed = $derived(
		suppressedAddresses.filter((s) =>
			(s.EmailAddress ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredTemplates = $derived(
		templates.filter((t) =>
			(t.TemplateName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadIdentities() {
		loading = true;
		try {
			const res = await sesv2().send(new ListEmailIdentitiesCommand({}));
			identities = (res.EmailIdentities ?? []).map((id) => ({
				IdentityName: id.IdentityName,
				IdentityType: id.IdentityType,
				SendingEnabled: id.SendingEnabled
			}));
		} catch (e) {
			toast.error(`Failed to load identities: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadContactLists() {
		loading = true;
		try {
			const res = await sesv2().send(new ListContactListsCommand({}));
			contactLists = (res.ContactLists ?? []).map((cl) => ({
				ContactListName: cl.ContactListName,
				Description: undefined
			}));
		} catch (e) {
			toast.error(`Failed to load contact lists: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadSuppressedAddresses() {
		loading = true;
		try {
			const res = await sesv2().send(new ListSuppressedDestinationsCommand({}));
			suppressedAddresses = (res.SuppressedDestinationSummaries ?? []).map((s) => ({
				EmailAddress: s.EmailAddress,
				Reason: s.Reason
			}));
		} catch (e) {
			toast.error(`Failed to load suppression list: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadTemplates() {
		loading = true;
		try {
			const res = await sesv2().send(new ListEmailTemplatesCommand({}));
			templates = (res.TemplatesMetadata ?? []).map((t) => ({
				TemplateName: t.TemplateName
			}));
		} catch (e) {
			toast.error(`Failed to load templates: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadAccount() {
		loading = true;
		try {
			const res = await sesv2().send(new GetAccountCommand({}));
			accountInfo = res;
		} catch (e) {
			toast.error(`Failed to load account info: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function addIdentity() {
		if (!newIdentity.trim()) return;
		addingIdentity = true;
		try {
			await sesv2().send(
				new CreateEmailIdentityCommand({ EmailIdentity: newIdentity.trim() })
			);
			toast.success(`Identity "${newIdentity}" created`);
			showAddIdentityModal = false;
			newIdentity = '';
			await loadIdentities();
		} catch (e) {
			toast.error(`Failed to create identity: ${e}`);
		} finally {
			addingIdentity = false;
		}
	}

	async function deleteIdentity(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Identity',
				message: `Delete SES v2 identity "${name}"? Sending from this identity will be disabled.`
			}))
		)
			return;
		try {
			await sesv2().send(new DeleteEmailIdentityCommand({ EmailIdentity: name }));
			toast.success(`Deleted "${name}"`);
			await loadIdentities();
		} catch (e) {
			toast.error(`Failed to delete identity: ${e}`);
		}
	}

	async function addContactList() {
		if (!newContactListName.trim()) return;
		addingContactList = true;
		try {
			await sesv2().send(
				new CreateContactListCommand({
					ContactListName: newContactListName.trim(),
					Description: newContactListDesc.trim() || undefined
				})
			);
			toast.success(`Contact list "${newContactListName}" created`);
			showAddContactListModal = false;
			newContactListName = '';
			newContactListDesc = '';
			await loadContactLists();
		} catch (e) {
			toast.error(`Failed to create contact list: ${e}`);
		} finally {
			addingContactList = false;
		}
	}

	async function deleteContactList(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Contact List',
				message: `Delete contact list "${name}" and all its contacts?`
			}))
		)
			return;
		try {
			await sesv2().send(new DeleteContactListCommand({ ContactListName: name }));
			toast.success(`Deleted "${name}"`);
			if (selectedContactList === name) selectedContactList = null;
			await loadContactLists();
		} catch (e) {
			toast.error(`Failed to delete contact list: ${e}`);
		}
	}

	// Contact-list member management
	let selectedContactList = $state<string | null>(null);
	let contacts = $state<Contact[]>([]);
	let loadingContacts = $state(false);
	let showAddContactModal = $state(false);
	let contactEmail = $state('');
	let contactUnsubscribed = $state(false);
	let savingContact = $state(false);
	let editingContactEmail = $state<string | null>(null);

	async function openContactList(name: string) {
		selectedContactList = name;
		await loadContacts();
	}

	async function loadContacts() {
		if (!selectedContactList) return;
		loadingContacts = true;
		try {
			const res = await sesv2().send(
				new ListContactsCommand({ ContactListName: selectedContactList })
			);
			contacts = res.Contacts ?? [];
		} catch (e) {
			toast.error(`Failed to load contacts: ${e}`);
		} finally {
			loadingContacts = false;
		}
	}

	function openAddContact() {
		editingContactEmail = null;
		contactEmail = '';
		contactUnsubscribed = false;
		showAddContactModal = true;
	}

	function openEditContact(c: Contact) {
		editingContactEmail = c.EmailAddress ?? null;
		contactEmail = c.EmailAddress ?? '';
		contactUnsubscribed = !!c.UnsubscribeAll;
		showAddContactModal = true;
	}

	async function saveContact() {
		if (!selectedContactList || !contactEmail.trim()) return;
		savingContact = true;
		try {
			if (editingContactEmail) {
				await sesv2().send(
					new UpdateContactCommand({
						ContactListName: selectedContactList,
						EmailAddress: editingContactEmail,
						UnsubscribeAll: contactUnsubscribed
					})
				);
				toast.success(`Contact "${editingContactEmail}" updated`);
			} else {
				await sesv2().send(
					new CreateContactCommand({
						ContactListName: selectedContactList,
						EmailAddress: contactEmail.trim(),
						UnsubscribeAll: contactUnsubscribed
					})
				);
				toast.success(`Contact "${contactEmail}" added`);
			}
			showAddContactModal = false;
			await loadContacts();
		} catch (e) {
			toast.error(`Failed to save contact: ${e}`);
		} finally {
			savingContact = false;
		}
	}

	async function deleteContact(email: string) {
		if (!selectedContactList) return;
		if (!(await confirmDestructive({ title: 'Remove Contact', message: `Remove contact "${email}" from the list?` }))) return;
		try {
			await sesv2().send(
				new DeleteContactCommand({ ContactListName: selectedContactList, EmailAddress: email })
			);
			toast.success(`Removed "${email}"`);
			await loadContacts();
		} catch (e) {
			toast.error(`Failed to remove contact: ${e}`);
		}
	}

	// CSV export of the current contact list's members.
	function exportContactsCsv() {
		if (contacts.length === 0) return;
		const header = 'EmailAddress,UnsubscribeAll,TopicPreferences';
		const rows = contacts.map((c) => {
			const prefs = (c.TopicPreferences ?? [])
				.map((p) => `${p.TopicName}=${p.SubscriptionStatus}`)
				.join('|');
			return `${c.EmailAddress ?? ''},${c.UnsubscribeAll ? 'true' : 'false'},"${prefs}"`;
		});
		const blob = new Blob([[header, ...rows].join('\n')], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${selectedContactList}-contacts.csv`;
		a.click();
		URL.revokeObjectURL(url);
	}

	async function addSuppressedAddress() {
		if (!suppressEmail.trim()) return;
		addingSuppressionEntry = true;
		try {
			await sesv2().send(
				new PutSuppressedDestinationCommand({
					EmailAddress: suppressEmail.trim(),
					Reason: suppressReason
				})
			);
			toast.success(`"${suppressEmail}" added to suppression list`);
			showAddSuppressionModal = false;
			suppressEmail = '';
			suppressReason = 'BOUNCE';
			await loadSuppressedAddresses();
		} catch (e) {
			toast.error(`Failed to add suppression: ${e}`);
		} finally {
			addingSuppressionEntry = false;
		}
	}

	async function removeSuppressedAddress(email: string) {
		if (
			!(await confirmDestructive({
				title: 'Remove from Suppression List',
				message: `Remove "${email}" from the suppression list? This address will receive emails again.`
			}))
		)
			return;
		try {
			await sesv2().send(new DeleteSuppressedDestinationCommand({ EmailAddress: email }));
			toast.success(`Removed "${email}" from suppression list`);
			await loadSuppressedAddresses();
		} catch (e) {
			toast.error(`Failed to remove suppression: ${e}`);
		}
	}

	async function createTemplate() {
		if (!newTemplateName.trim() || !newTemplateSubject.trim()) return;
		creatingTemplate = true;
		try {
			await sesv2().send(
				new CreateEmailTemplateCommand({
					TemplateName: newTemplateName.trim(),
					TemplateContent: {
						Subject: newTemplateSubject.trim(),
						Html: newTemplateHtml || undefined,
						Text: newTemplateText || undefined
					}
				})
			);
			toast.success(`Template "${newTemplateName}" created`);
			showCreateTemplateModal = false;
			newTemplateName = '';
			newTemplateSubject = '';
			newTemplateHtml = '';
			newTemplateText = '';
			await loadTemplates();
		} catch (e) {
			toast.error(`Failed to create template: ${e}`);
		} finally {
			creatingTemplate = false;
		}
	}

	async function deleteTemplate(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Template',
				message: `Delete email template "${name}"?`
			}))
		)
			return;
		try {
			await sesv2().send(new DeleteEmailTemplateCommand({ TemplateName: name }));
			toast.success(`Template "${name}" deleted`);
			await loadTemplates();
		} catch (e) {
			toast.error(`Failed to delete template: ${e}`);
		}
	}

	async function sendEmail() {
		if (!sendFrom.trim() || !sendTo.trim() || !sendSubject.trim()) {
			toast.error('From, To, and Subject are required');
			return;
		}
		sending = true;
		try {
			await sesv2().send(
				new SendEmailCommand({
					FromEmailAddress: sendFrom.trim(),
					Destination: { ToAddresses: sendTo.split(',').map((s) => s.trim()) },
					Content: {
						Simple: {
							Subject: { Data: sendSubject.trim() },
							Body: {
								Text: { Data: sendBody },
								...(sendHtml ? { Html: { Data: sendHtml } } : {})
							}
						}
					}
				})
			);
			toast.success('Email sent successfully');
		} catch (e) {
			toast.error(`Failed to send email: ${e}`);
		} finally {
			sending = false;
		}
	}

	async function onTabChange(tab: Tab) {
		activeTab = tab;
		searchQuery = '';
		switch (tab) {
			case 'identities':
				await loadIdentities();
				break;
			case 'contact-lists':
				await loadContactLists();
				break;
			case 'suppression':
				await loadSuppressedAddresses();
				break;
			case 'templates':
				await loadTemplates();
				break;
			case 'account':
				await loadAccount();
				break;
		}
	}

	function refreshCurrentTab() {
		void onTabChange(activeTab);
	}

	// Every tab's data (identities, contact lists + their contacts,
	// suppression list, templates, account info) is region-scoped, so a
	// region switch must clear it (not just reload) and then reload whichever
	// tab is currently active. `activeTab` is read via `untrack` because
	// `onTabChange` also writes it — without that, this effect would depend
	// on `activeTab` too and re-clear/re-fetch everything on every tab click,
	// not just on a region change.
	onRegionChange(() => {
		identities = [];
		contactLists = [];
		suppressedAddresses = [];
		templates = [];
		accountInfo = null;
		selectedContactList = null;
		contacts = [];
		showAddIdentityModal = false;
		showAddContactListModal = false;
		showAddSuppressionModal = false;
		showCreateTemplateModal = false;
		showAddContactModal = false;
		void onTabChange(untrack(() => activeTab));
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Mail class="h-8 w-8 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold">Simple Email Service v2</h1>
				<p class="text-sm text-muted-foreground">
					Manage identities, contacts, suppression, and email delivery
				</p>
			</div>
		</div>
		<button
			onclick={refreshCurrentTab}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex flex-wrap border-b">
		{#each [
			{ id: 'identities' as Tab, label: 'Identities', icon: CheckCircle },
			{ id: 'contact-lists' as Tab, label: 'Contact Lists', icon: Users },
			{ id: 'suppression' as Tab, label: 'Suppression', icon: Shield },
			{ id: 'templates' as Tab, label: 'Templates', icon: FileText },
			{ id: 'send' as Tab, label: 'Send Email', icon: Send },
			{ id: 'account' as Tab, label: 'Account', icon: BarChart2 }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Search bar (for tabs with lists) -->
	{#if activeTab !== 'send' && activeTab !== 'account'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			{#if activeTab === 'identities'}
				<button
					onclick={() => (showAddIdentityModal = true)}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>
					<Plus class="h-4 w-4" />
					Add Identity
				</button>
			{:else if activeTab === 'contact-lists'}
				<button
					onclick={() => (showAddContactListModal = true)}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>
					<Plus class="h-4 w-4" />
					New List
				</button>
			{:else if activeTab === 'suppression'}
				<button
					onclick={() => (showAddSuppressionModal = true)}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>
					<Plus class="h-4 w-4" />
					Add Address
				</button>
			{:else if activeTab === 'templates'}
				<button
					onclick={() => (showCreateTemplateModal = true)}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>
					<Plus class="h-4 w-4" />
					Create Template
				</button>
			{/if}
		</div>
	{/if}

	<!-- Loading -->
	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{/if}

	<!-- Identities Tab -->
	{#if activeTab === 'identities' && !loading}
		{#if filteredIdentities.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Mail class="h-12 w-12 mb-3 opacity-30" />
				<p>No identities found</p>
				<p class="text-sm">Add an email address or domain to get started</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Identity</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Sending</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredIdentities as identity}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{identity.IdentityName}</td>
								<td class="px-4 py-3 text-muted-foreground">{identity.IdentityType ?? '—'}</td>
								<td class="px-4 py-3">
									{#if identity.SendingEnabled}
										<span class="flex items-center gap-1 text-green-600 dark:text-green-400">
											<CheckCircle class="h-4 w-4" />
											Enabled
										</span>
									{:else}
										<span class="flex items-center gap-1 text-red-500">
											<XCircle class="h-4 w-4" />
											Disabled
										</span>
									{/if}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteIdentity(identity.IdentityName ?? '')}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete identity"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Contact Lists Tab -->
	{#if activeTab === 'contact-lists' && !loading}
		{#if filteredContactLists.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Users class="h-12 w-12 mb-3 opacity-30" />
				<p>No contact lists found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">List Name</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredContactLists as list}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{list.ContactListName}</td>
								<td class="px-4 py-3 text-muted-foreground">{list.Description ?? '—'}</td>
								<td class="px-4 py-3 text-right whitespace-nowrap">
									<button
										onclick={() => openContactList(list.ContactListName ?? '')}
										class="inline-flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent"
										title="Manage members"
									>
										<Users class="h-3.5 w-3.5" /> Members
									</button>
									<button
										onclick={() => deleteContactList(list.ContactListName ?? '')}
										class="ml-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete contact list"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Contact-list members -->
		{#if selectedContactList}
			<div class="mt-4 rounded-lg border p-4 space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold flex items-center gap-2">
						<ChevronRight class="h-4 w-4 text-muted-foreground" />
						Members of {selectedContactList}
					</h3>
					<div class="flex items-center gap-2">
						<button onclick={openAddContact} class="inline-flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90">
							<Plus class="h-3.5 w-3.5" /> Add Contact
						</button>
						<button onclick={exportContactsCsv} disabled={contacts.length === 0} class="rounded-md border px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50">
							Export CSV
						</button>
						<button onclick={() => (selectedContactList = null)} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
					</div>
				</div>
				{#if loadingContacts}
					<div class="flex justify-center py-6"><RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" /></div>
				{:else if contacts.length === 0}
					<p class="text-sm text-muted-foreground">No contacts in this list.</p>
				{:else}
					<div class="rounded border overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-muted/50">
								<tr>
									<th class="px-4 py-2 text-left font-medium">Email</th>
									<th class="px-4 py-2 text-left font-medium">Unsubscribed (All)</th>
									<th class="px-4 py-2 text-right font-medium">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y">
								{#each contacts as c}
									<tr class="hover:bg-muted/30">
										<td class="px-4 py-2">{c.EmailAddress}</td>
										<td class="px-4 py-2">
											{#if c.UnsubscribeAll}
												<span class="rounded-full bg-red-100 px-2 py-0.5 text-xs text-red-700 dark:bg-red-900 dark:text-red-300">Yes</span>
											{:else}
												<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900 dark:text-green-300">No</span>
											{/if}
										</td>
										<td class="px-4 py-2 text-right whitespace-nowrap">
											<button onclick={() => openEditContact(c)} class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950" title="Edit contact">
												<Pencil class="h-4 w-4" />
											</button>
											<button onclick={() => deleteContact(c.EmailAddress ?? '')} class="ml-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Remove contact">
												<Trash2 class="h-4 w-4" />
											</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- Suppression Tab -->
	{#if activeTab === 'suppression' && !loading}
		{#if filteredSuppressed.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No suppressed addresses</p>
				<p class="text-sm">Suppressed addresses will not receive emails</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Email Address</th>
							<th class="px-4 py-3 text-left font-medium">Reason</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredSuppressed as s}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{s.EmailAddress}</td>
								<td class="px-4 py-3">
									<span
										class="rounded-full px-2 py-0.5 text-xs font-medium {s.Reason === 'BOUNCE' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300' : 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'}"
									>
										{s.Reason}
									</span>
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => removeSuppressedAddress(s.EmailAddress ?? '')}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Remove from suppression list"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Templates Tab -->
	{#if activeTab === 'templates' && !loading}
		{#if filteredTemplates.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<FileText class="h-12 w-12 mb-3 opacity-30" />
				<p>No email templates found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Template Name</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTemplates as tmpl}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{tmpl.TemplateName}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteTemplate(tmpl.TemplateName ?? '')}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete template"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Send Email Tab -->
	{#if activeTab === 'send'}
		<div class="max-w-2xl rounded-lg border p-6 space-y-4">
			<h2 class="text-lg font-semibold flex items-center gap-2">
				<Send class="h-5 w-5 text-orange-500" />
				Send Email
			</h2>
			<div class="space-y-3">
				<div>
					<label for="send-from" class="block text-sm font-medium mb-1"
						>From (verified identity)</label
					>
					<input
						id="send-from"
						type="email"
						bind:value={sendFrom}
						placeholder="noreply@example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="send-to" class="block text-sm font-medium mb-1"
						>To (comma-separated)</label
					>
					<input
						id="send-to"
						type="text"
						bind:value={sendTo}
						placeholder="user@example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="send-subject" class="block text-sm font-medium mb-1">Subject</label>
					<input
						id="send-subject"
						type="text"
						bind:value={sendSubject}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="send-body" class="block text-sm font-medium mb-1">Text Body</label>
					<textarea
						id="send-body"
						bind:value={sendBody}
						rows={4}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary resize-none"
					></textarea>
				</div>
				<div>
					<label for="send-html" class="block text-sm font-medium mb-1"
						>HTML Body (optional)</label
					>
					<textarea
						id="send-html"
						bind:value={sendHtml}
						rows={3}
						placeholder="<h1>Hello</h1>"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary resize-none font-mono"
					></textarea>
				</div>
				<button
					onclick={sendEmail}
					disabled={sending}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					<Send class="h-4 w-4" />
					{sending ? 'Sending...' : 'Send Email'}
				</button>
			</div>
		</div>
	{/if}

	<!-- Account Tab -->
	{#if activeTab === 'account' && !loading}
		{#if accountInfo}
			<div class="max-w-2xl rounded-lg border p-6 space-y-4">
				<h2 class="text-lg font-semibold flex items-center gap-2">
					<BarChart2 class="h-5 w-5 text-orange-500" />
					Account Details
				</h2>
				<div class="grid grid-cols-2 gap-4 text-sm">
					<div>
						<p class="font-medium text-muted-foreground">Sending Enabled</p>
						<p>
							{#if accountInfo.SendingEnabled}
								<span class="text-green-600 dark:text-green-400">Yes</span>
							{:else}
								<span class="text-red-500">No</span>
							{/if}
						</p>
					</div>
					<div>
						<p class="font-medium text-muted-foreground">Max 24hr Send</p>
						<p>{accountInfo.SendQuota?.Max24HourSend ?? '—'}</p>
					</div>
					<div>
						<p class="font-medium text-muted-foreground">Max Send Rate</p>
						<p>{accountInfo.SendQuota?.MaxSendRate ?? '—'} msg/sec</p>
					</div>
					<div>
						<p class="font-medium text-muted-foreground">Sent Last 24hrs</p>
						<p>{accountInfo.SendQuota?.SentLast24Hours ?? '0'}</p>
					</div>
					<div>
						<p class="font-medium text-muted-foreground">Enforcement Status</p>
						<p>{accountInfo.EnforcementStatus ?? '—'}</p>
					</div>
					<div>
						<p class="font-medium text-muted-foreground">Production Access</p>
						<p>{accountInfo.ProductionAccessEnabled ? 'Enabled' : 'Sandbox'}</p>
					</div>
				</div>
			</div>
		{:else}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<BarChart2 class="h-12 w-12 mb-3 opacity-30" />
				<p>No account info loaded</p>
				<button
					onclick={loadAccount}
					class="mt-3 text-sm text-primary hover:underline"
				>
					Load Account Info
				</button>
			</div>
		{/if}
	{/if}
</div>

<!-- Add Identity Modal -->
{#if showAddIdentityModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Email Identity</h2>
			<p class="text-sm text-muted-foreground mb-4">
				Enter an email address or domain to create a new identity.
			</p>
			<div>
				<label for="new-identity" class="block text-sm font-medium mb-1">Email or Domain</label>
				<input
					id="new-identity"
					type="text"
					bind:value={newIdentity}
					placeholder="user@example.com or example.com"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAddIdentityModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={addIdentity}
					disabled={addingIdentity || !newIdentity.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{addingIdentity ? 'Creating...' : 'Create Identity'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Add Contact List Modal -->
{#if showAddContactListModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Contact List</h2>
			<div class="space-y-3">
				<div>
					<label for="cl-name" class="block text-sm font-medium mb-1">List Name *</label>
					<input
						id="cl-name"
						type="text"
						bind:value={newContactListName}
						placeholder="my-newsletter"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="cl-desc" class="block text-sm font-medium mb-1">Description</label>
					<input
						id="cl-desc"
						type="text"
						bind:value={newContactListDesc}
						placeholder="Weekly newsletter subscribers"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAddContactListModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={addContactList}
					disabled={addingContactList || !newContactListName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{addingContactList ? 'Creating...' : 'Create List'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Add Suppression Modal -->
{#if showAddSuppressionModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add to Suppression List</h2>
			<div class="space-y-3">
				<div>
					<label for="suppress-email" class="block text-sm font-medium mb-1"
						>Email Address *</label
					>
					<input
						id="suppress-email"
						type="email"
						bind:value={suppressEmail}
						placeholder="user@example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="suppress-reason" class="block text-sm font-medium mb-1">Reason</label>
					<select
						id="suppress-reason"
						bind:value={suppressReason}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="BOUNCE">Bounce</option>
						<option value="COMPLAINT">Complaint</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAddSuppressionModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={addSuppressedAddress}
					disabled={addingSuppressionEntry || !suppressEmail.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{addingSuppressionEntry ? 'Adding...' : 'Add to Suppression List'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Template Modal -->
{#if showCreateTemplateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Email Template</h2>
			<div class="space-y-3">
				<div>
					<label for="tmpl-name" class="block text-sm font-medium mb-1">Template Name *</label>
					<input
						id="tmpl-name"
						type="text"
						bind:value={newTemplateName}
						placeholder="welcome-email"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="tmpl-subject" class="block text-sm font-medium mb-1">Subject *</label>
					<input
						id="tmpl-subject"
						type="text"
						bind:value={newTemplateSubject}
						placeholder="Welcome to our service!"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="tmpl-html" class="block text-sm font-medium mb-1">HTML Body</label>
					<textarea
						id="tmpl-html"
						bind:value={newTemplateHtml}
						rows={3}
						placeholder="<h1>Welcome!</h1>"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary resize-none font-mono"
					></textarea>
				</div>
				<div>
					<label for="tmpl-text" class="block text-sm font-medium mb-1">Text Body</label>
					<textarea
						id="tmpl-text"
						bind:value={newTemplateText}
						rows={3}
						placeholder="Welcome!"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary resize-none"
					></textarea>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateTemplateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createTemplate}
					disabled={creatingTemplate || !newTemplateName.trim() || !newTemplateSubject.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingTemplate ? 'Creating...' : 'Create Template'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Add / Edit Contact Modal -->
{#if showAddContactModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">{editingContactEmail ? 'Edit Contact' : 'Add Contact'}</h2>
			<div class="space-y-3">
				<div>
					<label for="contact-email" class="block text-sm font-medium mb-1">Email Address *</label>
					<input
						id="contact-email"
						type="email"
						bind:value={contactEmail}
						disabled={!!editingContactEmail}
						placeholder="subscriber@example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary disabled:opacity-60"
					/>
				</div>
				<label class="flex items-center gap-2 text-sm">
					<input type="checkbox" bind:checked={contactUnsubscribed} />
					Unsubscribe from all topics
				</label>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showAddContactModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={saveContact}
					disabled={savingContact || !contactEmail.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{savingContact ? 'Saving…' : editingContactEmail ? 'Update' : 'Add'}
				</button>
			</div>
		</div>
	</div>
{/if}
