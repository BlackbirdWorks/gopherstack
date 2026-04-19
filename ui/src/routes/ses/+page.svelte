<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getSESClient } from '$lib/aws-client';
	import {
		ListIdentitiesCommand,
		GetIdentityVerificationAttributesCommand,
		VerifyEmailIdentityCommand,
		DeleteIdentityCommand,
		SendEmailCommand,
		ListTemplatesCommand,
		CreateTemplateCommand,
		DeleteTemplateCommand,
		GetTemplateCommand,
		type IdentityVerificationAttributes
	} from '@aws-sdk/client-ses';
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
		Clock,
		Eye
	} from 'lucide-svelte';

	const ses = getSESClient();

	let loading = $state(false);
	let activeTab = $state<'identities' | 'templates' | 'send'>('identities');
	let searchQuery = $state('');

	// Identities
	let identities = $state<string[]>([]);
	let verificationAttrs = $state<Record<string, IdentityVerificationAttributes>>({});
	let showVerifyModal = $state(false);
	let verifying = $state(false);
	let newIdentity = $state('');

	// Templates
	let templates = $state<Array<{ Name?: string; CreatedTimestamp?: Date }>>([]);
	let selectedTemplate = $state<{
		TemplateName?: string;
		SubjectPart?: string;
		HtmlPart?: string;
		TextPart?: string;
	} | null>(null);
	let showCreateTemplateModal = $state(false);
	let creatingTemplate = $state(false);
	let newTemplateName = $state('');
	let newTemplateSubject = $state('');
	let newTemplateHtml = $state('');
	let newTemplateText = $state('');

	// Send Email
	let sendFrom = $state('');
	let sendTo = $state('');
	let sendSubject = $state('Hello from GopherStack');
	let sendBody = $state('This is a test email sent via Amazon SES.');
	let sendHtml = $state('');
	let sending = $state(false);

	const filteredIdentities = $derived(
		identities.filter((id) => id.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredTemplates = $derived(
		templates.filter((t) => (t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function getVerificationStatus(identity: string): string {
		return verificationAttrs[identity]?.VerificationStatus ?? 'Pending';
	}

	function statusIcon(status: string) {
		if (status === 'Success') return 'success';
		if (status === 'Failed') return 'failed';
		return 'pending';
	}

	async function loadIdentities() {
		loading = true;
		try {
			const res = await ses.send(new ListIdentitiesCommand({ IdentityType: 'EmailAddress' }));
			const ids = res.Identities ?? [];
			identities = ids;
			if (ids.length > 0) {
				const attrs = await ses.send(
					new GetIdentityVerificationAttributesCommand({ Identities: ids })
				);
				verificationAttrs = attrs.VerificationAttributes ?? {};
			}
		} catch (e) {
			toast.error(`Failed to load identities: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadTemplates() {
		loading = true;
		try {
			const res = await ses.send(new ListTemplatesCommand({ MaxItems: 100 }));
			templates = (res.TemplatesMetadata ?? []).map((t) => ({
				Name: t.Name,
				CreatedTimestamp: t.CreatedTimestamp
			}));
		} catch (e) {
			toast.error(`Failed to load templates: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function verifyIdentity() {
		if (!newIdentity.trim()) return;
		verifying = true;
		try {
			await ses.send(new VerifyEmailIdentityCommand({ EmailAddress: newIdentity.trim() }));
			toast.success(`Verification email sent to ${newIdentity}`);
			showVerifyModal = false;
			newIdentity = '';
			await loadIdentities();
		} catch (e) {
			toast.error(`Failed to verify: ${e}`);
		} finally {
			verifying = false;
		}
	}

	async function deleteIdentity(identity: string) {
		if (!await confirmDestructive(`Delete identity "${identity}"?`)) return;
		try {
			await ses.send(new DeleteIdentityCommand({ Identity: identity }));
			toast.success(`Deleted ${identity}`);
			await loadIdentities();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function createTemplate() {
		if (!newTemplateName.trim() || !newTemplateSubject.trim()) return;
		creatingTemplate = true;
		try {
			await ses.send(
				new CreateTemplateCommand({
					Template: {
						TemplateName: newTemplateName.trim(),
						SubjectPart: newTemplateSubject.trim(),
						HtmlPart: newTemplateHtml || undefined,
						TextPart: newTemplateText || undefined
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
		if (!await confirmDestructive(`Delete template "${name}"?`)) return;
		try {
			await ses.send(new DeleteTemplateCommand({ TemplateName: name }));
			toast.success(`Template "${name}" deleted`);
			await loadTemplates();
		} catch (e) {
			toast.error(`Failed to delete template: ${e}`);
		}
	}

	async function viewTemplate(name: string) {
		try {
			const res = await ses.send(new GetTemplateCommand({ TemplateName: name }));
			selectedTemplate = res.Template
				? {
						TemplateName: res.Template.TemplateName,
						SubjectPart: res.Template.SubjectPart,
						HtmlPart: res.Template.HtmlPart,
						TextPart: res.Template.TextPart
					}
				: null;
		} catch (e) {
			toast.error(`Failed to load template: ${e}`);
		}
	}

	async function sendEmail() {
		if (!sendFrom.trim() || !sendTo.trim() || !sendSubject.trim()) {
			toast.error('From, To, and Subject are required');
			return;
		}
		sending = true;
		try {
			await ses.send(
				new SendEmailCommand({
					Source: sendFrom.trim(),
					Destination: { ToAddresses: sendTo.split(',').map((s) => s.trim()) },
					Message: {
						Subject: { Data: sendSubject.trim() },
						Body: {
							Text: { Data: sendBody },
							...(sendHtml ? { Html: { Data: sendHtml } } : {})
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

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'identities') await loadIdentities();
		else if (tab === 'templates') await loadTemplates();
	}

	onMount(() => loadIdentities());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Mail class="h-8 w-8 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold">Simple Email Service</h1>
				<p class="text-sm text-muted-foreground">Manage email identities, templates, and sending</p>
			</div>
		</div>
		<button
			onclick={() => (activeTab === 'identities' ? loadIdentities() : loadTemplates())}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'identities', label: 'Identities', icon: CheckCircle }, { id: 'templates', label: 'Templates', icon: FileText }, { id: 'send', label: 'Send Email', icon: Send }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Identities Tab -->
	{#if activeTab === 'identities'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search identities..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showVerifyModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Verify Identity
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredIdentities.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Mail class="h-12 w-12 mb-3 opacity-30" />
				<p>No verified identities found</p>
				<p class="text-sm">Add an email address to get started</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Identity</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Token</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredIdentities as identity}
							{@const status = getVerificationStatus(identity)}
							{@const icon = statusIcon(status)}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{identity}</td>
								<td class="px-4 py-3">
									<span class="flex items-center gap-1.5">
										{#if icon === 'success'}
											<CheckCircle class="h-4 w-4 text-green-500" />
											<span class="text-green-700 dark:text-green-400">{status}</span>
										{:else if icon === 'failed'}
											<XCircle class="h-4 w-4 text-red-500" />
											<span class="text-red-700 dark:text-red-400">{status}</span>
										{:else}
											<Clock class="h-4 w-4 text-yellow-500" />
											<span class="text-yellow-700 dark:text-yellow-400">{status}</span>
										{/if}
									</span>
								</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground truncate max-w-[200px]">
									{verificationAttrs[identity]?.VerificationToken ?? '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteIdentity(identity)}
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

	<!-- Templates Tab -->
	{#if activeTab === 'templates'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search templates..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateTemplateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Template
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredTemplates.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<FileText class="h-12 w-12 mb-3 opacity-30" />
				<p>No email templates found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTemplates as tmpl}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{tmpl.Name}</td>
								<td class="px-4 py-3 text-muted-foreground">
									{tmpl.CreatedTimestamp ? new Date(tmpl.CreatedTimestamp).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={() => viewTemplate(tmpl.Name ?? '')}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View template"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={() => deleteTemplate(tmpl.Name ?? '')}
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

		{#if selectedTemplate}
			<div
				class="fixed inset-0 z-50 bg-black/50 flex justify-end"
				role="button"
				tabindex="0"
				onclick={() => (selectedTemplate = null)}
				onkeydown={(e) => e.key === 'Escape' && (selectedTemplate = null)}
			>
				<div
					class="w-full max-w-xl bg-background h-full overflow-y-auto shadow-xl"
					role="presentation"
					onclick={(e) => e.stopPropagation()}
				>
					<div class="flex items-center justify-between border-b px-6 py-4">
						<h2 class="text-lg font-semibold">{selectedTemplate.TemplateName}</h2>
						<button onclick={() => (selectedTemplate = null)} class="rounded p-1 hover:bg-accent">
							<XCircle class="h-5 w-5" />
						</button>
					</div>
					<div class="p-6 space-y-4">
						<div>
							<p class="text-sm font-medium text-muted-foreground">Subject</p>
							<p>{selectedTemplate.SubjectPart ?? '—'}</p>
						</div>
						{#if selectedTemplate.HtmlPart}
							<div>
								<p class="text-sm font-medium text-muted-foreground">HTML Body</p>
								<pre class="mt-1 rounded bg-muted p-3 text-xs overflow-auto max-h-48">{selectedTemplate.HtmlPart}</pre>
							</div>
						{/if}
						{#if selectedTemplate.TextPart}
							<div>
								<p class="text-sm font-medium text-muted-foreground">Text Body</p>
								<pre class="mt-1 rounded bg-muted p-3 text-xs overflow-auto max-h-48">{selectedTemplate.TextPart}</pre>
							</div>
						{/if}
					</div>
				</div>
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
</div>

<!-- Verify Identity Modal -->
{#if showVerifyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Verify Email Identity</h2>
			<p class="text-sm text-muted-foreground mb-4">
				A verification email will be sent to the address. Click the link to verify.
			</p>
			<div>
				<label for="new-identity" class="block text-sm font-medium mb-1">Email Address</label>
				<input
					id="new-identity"
					type="email"
					bind:value={newIdentity}
					placeholder="user@example.com"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showVerifyModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={verifyIdentity}
					disabled={verifying || !newIdentity.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{verifying ? 'Sending...' : 'Send Verification'}
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
						placeholder="Welcome, {{name}}!"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="tmpl-html" class="block text-sm font-medium mb-1">HTML Body</label>
					<textarea
						id="tmpl-html"
						bind:value={newTemplateHtml}
						rows={3}
						placeholder="<h1>Hello {{name}}</h1>"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary resize-none font-mono"
					></textarea>
				</div>
				<div>
					<label for="tmpl-text" class="block text-sm font-medium mb-1">Text Body</label>
					<textarea
						id="tmpl-text"
						bind:value={newTemplateText}
						rows={3}
						placeholder="Hello {{name}}"
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
