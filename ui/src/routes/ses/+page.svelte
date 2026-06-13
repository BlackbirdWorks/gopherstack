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
		ListConfigurationSetsCommand,
		CreateConfigurationSetCommand,
		DeleteConfigurationSetCommand,
		ListReceiptRuleSetsCommand,
		CreateReceiptRuleSetCommand,
		DeleteReceiptRuleSetCommand,
		DescribeReceiptRuleSetCommand,
		TestRenderTemplateCommand,
		type SESClient,
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
		Eye,
		Settings,
		Filter
	} from 'lucide-svelte';

	let sesClient: SESClient | undefined;
	function ses(): SESClient {
		return (sesClient ??= getSESClient());
	}

	let loading = $state(false);
	let activeTab = $state<'identities' | 'templates' | 'send' | 'configsets' | 'receiptrules' | 'emails'>('identities');
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

	// Template test-render (send-test) state
	let testRenderData = $state('{\n  "name": "World"\n}');
	let testRenderResult = $state('');
	let testingRender = $state(false);

	// Send Email
	let sendFrom = $state('');
	let sendTo = $state('');
	let sendSubject = $state('Hello from GopherStack');
	let sendBody = $state('This is a test email sent via Amazon SES.');
	let sendHtml = $state('');
	let sending = $state(false);

	// Configuration Sets
	let configSets = $state<string[]>([]);
	let showCreateConfigSetModal = $state(false);
	let creatingConfigSet = $state(false);
	let newConfigSetName = $state('');

	// Receipt Rule Sets
	type ReceiptRuleSetMeta = { RuleSetName?: string; CreatedTimestamp?: Date };
	type ReceiptRule = {
		Name?: string;
		Enabled?: boolean;
		TlsPolicy?: string;
		ScanEnabled?: boolean;
		Recipients?: string[];
	};
	let receiptRuleSets = $state<ReceiptRuleSetMeta[]>([]);
	let selectedRuleSet = $state<{ name: string; rules: ReceiptRule[] } | null>(null);
	let showCreateRuleSetModal = $state(false);
	let creatingRuleSet = $state(false);
	let newRuleSetName = $state('');

	// Emails search index
	type SentEmail = {
		messageID: string;
		from: string;
		to: string[];
		subject: string;
		timestamp: string;
	};
	let emails = $state<SentEmail[]>([]);
	let emailSearchQuery = $state('');

	const filteredIdentities = $derived(
		identities.filter((id) => id.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredTemplates = $derived(
		templates.filter((t) => (t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredConfigSets = $derived(
		configSets.filter((cs) => cs.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredRuleSets = $derived(
		receiptRuleSets.filter((rs) =>
			(rs.RuleSetName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredEmails = $derived(
		emails.filter((e) => {
			const q = emailSearchQuery.toLowerCase();
			if (!q) return true;
			return (
				e.from.toLowerCase().includes(q) ||
				e.subject.toLowerCase().includes(q) ||
				e.to.some((t) => t.toLowerCase().includes(q)) ||
				e.messageID.toLowerCase().includes(q)
			);
		})
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
			const res = await ses().send(new ListIdentitiesCommand({ IdentityType: 'EmailAddress' }));
			const ids = res.Identities ?? [];
			identities = ids;
			if (ids.length > 0) {
				const attrs = await ses().send(
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
			const res = await ses().send(new ListTemplatesCommand({ MaxItems: 100 }));
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

	async function loadConfigSets() {
		loading = true;
		try {
			const res = await ses().send(new ListConfigurationSetsCommand({ MaxItems: 100 }));
			configSets = (res.ConfigurationSets ?? []).map((cs) => cs.Name ?? '');
		} catch (e) {
			toast.error(`Failed to load configuration sets: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadReceiptRuleSets() {
		loading = true;
		try {
			const res = await ses().send(new ListReceiptRuleSetsCommand({}));
			receiptRuleSets = (res.RuleSets ?? []).map((rs) => ({
				RuleSetName: rs.Name,
				CreatedTimestamp: rs.CreatedTimestamp
			}));
		} catch (e) {
			toast.error(`Failed to load receipt rule sets: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadEmails() {
		loading = true;
		try {
			const resp = await fetch('/dashboard/ses/emails');
			if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const data: any[] = await resp.json();
			emails = data.map((e) => ({
				messageID: e.messageID ?? e.MessageID ?? '',
				from: e.from ?? e.From ?? '',
				to: e.to ?? e.To ?? [],
				subject: e.subject ?? e.Subject ?? '',
				timestamp: e.timestamp ?? e.Timestamp ?? ''
			}));
		} catch (e) {
			toast.error(`Failed to load emails: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function verifyIdentity() {
		if (!newIdentity.trim()) return;
		verifying = true;
		try {
			await ses().send(new VerifyEmailIdentityCommand({ EmailAddress: newIdentity.trim() }));
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
		if (!await confirmDestructive({ title: 'Delete Identity', message: `Delete SES identity "${identity}"? Email sending from this identity will be disabled.` })) return;
		try {
			await ses().send(new DeleteIdentityCommand({ Identity: identity }));
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
			await ses().send(
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
		if (!await confirmDestructive({ title: 'Delete Email Template', message: `Delete template "${name}"? Any integrations referencing this template will stop working.` })) return;
		try {
			await ses().send(new DeleteTemplateCommand({ TemplateName: name }));
			toast.success(`Template "${name}" deleted`);
			await loadTemplates();
		} catch (e) {
			toast.error(`Failed to delete template: ${e}`);
		}
	}

	async function viewTemplate(name: string) {
		testRenderResult = '';
		try {
			const res = await ses().send(new GetTemplateCommand({ TemplateName: name }));
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

	// Render the selected template against sample data (TestRenderTemplate) so the
	// operator can preview substitution output before sending.
	async function testRenderTemplate() {
		if (!selectedTemplate?.TemplateName) return;
		let parsed: Record<string, unknown>;
		try {
			parsed = JSON.parse(testRenderData || '{}');
		} catch {
			toast.error('Template data must be valid JSON');
			return;
		}
		testingRender = true;
		testRenderResult = '';
		try {
			const res = await ses().send(
				new TestRenderTemplateCommand({
					TemplateName: selectedTemplate.TemplateName,
					TemplateData: JSON.stringify(parsed)
				})
			);
			testRenderResult = res.RenderedTemplate ?? '(empty render result)';
			toast.success('Template rendered');
		} catch (e) {
			toast.error(`Failed to render template: ${e}`);
		} finally {
			testingRender = false;
		}
	}

	async function sendEmail() {
		if (!sendFrom.trim() || !sendTo.trim() || !sendSubject.trim()) {
			toast.error('From, To, and Subject are required');
			return;
		}
		sending = true;
		try {
			await ses().send(
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

	async function createConfigSet() {
		if (!newConfigSetName.trim()) return;
		creatingConfigSet = true;
		try {
			await ses().send(
				new CreateConfigurationSetCommand({
					ConfigurationSet: { Name: newConfigSetName.trim() }
				})
			);
			toast.success(`Configuration set "${newConfigSetName}" created`);
			showCreateConfigSetModal = false;
			newConfigSetName = '';
			await loadConfigSets();
		} catch (e) {
			toast.error(`Failed to create configuration set: ${e}`);
		} finally {
			creatingConfigSet = false;
		}
	}

	async function deleteConfigSet(name: string) {
		if (!await confirmDestructive({ title: 'Delete Configuration Set', message: `Delete configuration set "${name}"?` })) return;
		try {
			await ses().send(new DeleteConfigurationSetCommand({ ConfigurationSetName: name }));
			toast.success(`Configuration set "${name}" deleted`);
			await loadConfigSets();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function createReceiptRuleSet() {
		if (!newRuleSetName.trim()) return;
		creatingRuleSet = true;
		try {
			await ses().send(new CreateReceiptRuleSetCommand({ RuleSetName: newRuleSetName.trim() }));
			toast.success(`Receipt rule set "${newRuleSetName}" created`);
			showCreateRuleSetModal = false;
			newRuleSetName = '';
			await loadReceiptRuleSets();
		} catch (e) {
			toast.error(`Failed to create receipt rule set: ${e}`);
		} finally {
			creatingRuleSet = false;
		}
	}

	async function deleteReceiptRuleSet(name: string) {
		if (!await confirmDestructive({ title: 'Delete Receipt Rule Set', message: `Delete receipt rule set "${name}" and all its rules?` })) return;
		try {
			await ses().send(new DeleteReceiptRuleSetCommand({ RuleSetName: name }));
			toast.success(`Receipt rule set "${name}" deleted`);
			if (selectedRuleSet?.name === name) selectedRuleSet = null;
			await loadReceiptRuleSets();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function viewRuleSet(name: string) {
		try {
			const res = await ses().send(new DescribeReceiptRuleSetCommand({ RuleSetName: name }));
			selectedRuleSet = {
				name,
				rules: (res.Rules ?? []).map((r) => ({
					Name: r.Name,
					Enabled: r.Enabled,
					TlsPolicy: r.TlsPolicy,
					ScanEnabled: r.ScanEnabled,
					Recipients: r.Recipients
				}))
			};
		} catch (e) {
			toast.error(`Failed to load rule set: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		emailSearchQuery = '';
		if (tab === 'identities') await loadIdentities();
		else if (tab === 'templates') await loadTemplates();
		else if (tab === 'configsets') await loadConfigSets();
		else if (tab === 'receiptrules') await loadReceiptRuleSets();
		else if (tab === 'emails') await loadEmails();
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
			onclick={() => {
				if (activeTab === 'identities') loadIdentities();
				else if (activeTab === 'templates') loadTemplates();
				else if (activeTab === 'configsets') loadConfigSets();
				else if (activeTab === 'receiptrules') loadReceiptRuleSets();
				else if (activeTab === 'emails') loadEmails();
			}}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b overflow-x-auto">
		{#each [
			{ id: 'identities', label: 'Identities', icon: CheckCircle },
			{ id: 'templates', label: 'Templates', icon: FileText },
			{ id: 'send', label: 'Send Email', icon: Send },
			{ id: 'configsets', label: 'Config Sets', icon: Settings },
			{ id: 'receiptrules', label: 'Receipt Rules', icon: Filter },
			{ id: 'emails', label: 'Sent Emails', icon: Mail }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
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

						<!-- Test Render / Send-Test -->
						<div class="border-t pt-4">
							<p class="text-sm font-medium flex items-center gap-2">
								<Send class="h-4 w-4 text-orange-500" /> Test Render
							</p>
							<p class="text-xs text-muted-foreground mb-2">
								Preview the rendered template with sample template data (JSON).
							</p>
							<textarea
								bind:value={testRenderData}
								rows={5}
								class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
							></textarea>
							<button
								onclick={testRenderTemplate}
								disabled={testingRender}
								class="mt-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							>
								{testingRender ? 'Rendering…' : 'Render Template'}
							</button>
							{#if testRenderResult}
								<div class="mt-3">
									<p class="text-sm font-medium text-muted-foreground">Rendered Output</p>
									<pre class="mt-1 rounded bg-muted p-3 text-xs overflow-auto max-h-64">{testRenderResult}</pre>
								</div>
							{/if}
						</div>
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

	<!-- Configuration Sets Tab -->
	{#if activeTab === 'configsets'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search configuration sets..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateConfigSetModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Config Set
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredConfigSets.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Settings class="h-12 w-12 mb-3 opacity-30" />
				<p>No configuration sets found</p>
				<p class="text-sm">Create a configuration set to track sending events</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredConfigSets as cs}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium font-mono">{cs}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteConfigSet(cs)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete configuration set"
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

	<!-- Receipt Rules Tab -->
	{#if activeTab === 'receiptrules'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search receipt rule sets..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateRuleSetModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Rule Set
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredRuleSets.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Filter class="h-12 w-12 mb-3 opacity-30" />
				<p>No receipt rule sets found</p>
				<p class="text-sm">Create a rule set to route inbound email</p>
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
						{#each filteredRuleSets as rs}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium font-mono">{rs.RuleSetName}</td>
								<td class="px-4 py-3 text-muted-foreground">
									{rs.CreatedTimestamp ? new Date(rs.CreatedTimestamp).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={() => viewRuleSet(rs.RuleSetName ?? '')}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View rules"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={() => deleteReceiptRuleSet(rs.RuleSetName ?? '')}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete rule set"
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

		{#if selectedRuleSet}
			<div
				class="fixed inset-0 z-50 bg-black/50 flex justify-end"
				role="button"
				tabindex="0"
				onclick={() => (selectedRuleSet = null)}
				onkeydown={(e) => e.key === 'Escape' && (selectedRuleSet = null)}
			>
				<div
					class="w-full max-w-xl bg-background h-full overflow-y-auto shadow-xl"
					role="presentation"
					onclick={(e) => e.stopPropagation()}
				>
					<div class="flex items-center justify-between border-b px-6 py-4">
						<h2 class="text-lg font-semibold">{selectedRuleSet.name}</h2>
						<button onclick={() => (selectedRuleSet = null)} class="rounded p-1 hover:bg-accent">
							<XCircle class="h-5 w-5" />
						</button>
					</div>
					<div class="p-6 space-y-4">
						{#if selectedRuleSet.rules.length === 0}
							<p class="text-sm text-muted-foreground">No rules in this rule set.</p>
						{:else}
							{#each selectedRuleSet.rules as rule}
								<div class="rounded-md border p-4 space-y-2">
									<div class="flex items-center justify-between">
										<span class="font-medium font-mono text-sm">{rule.Name}</span>
										<span class="text-xs px-2 py-0.5 rounded-full {rule.Enabled ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' : 'bg-muted text-muted-foreground'}">
											{rule.Enabled ? 'Enabled' : 'Disabled'}
										</span>
									</div>
									{#if rule.Recipients && rule.Recipients.length > 0}
										<div>
											<span class="text-xs text-muted-foreground">Recipients: </span>
											<span class="text-xs">{rule.Recipients.join(', ')}</span>
										</div>
									{/if}
									{#if rule.TlsPolicy}
										<div>
											<span class="text-xs text-muted-foreground">TLS: </span>
											<span class="text-xs font-mono">{rule.TlsPolicy}</span>
										</div>
									{/if}
								</div>
							{/each}
						{/if}
					</div>
				</div>
			</div>
		{/if}
	{/if}

	<!-- Sent Emails Tab -->
	{#if activeTab === 'emails'}
		<div class="flex items-center gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search by from, to, subject, or message ID..."
					bind:value={emailSearchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredEmails.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Mail class="h-12 w-12 mb-3 opacity-30" />
				<p>No sent emails found</p>
				<p class="text-sm">Emails appear here after sending via the Send Email tab</p>
			</div>
		{:else}
			<p class="text-sm text-muted-foreground">{filteredEmails.length} email{filteredEmails.length !== 1 ? 's' : ''}</p>
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">From</th>
							<th class="px-4 py-3 text-left font-medium">To</th>
							<th class="px-4 py-3 text-left font-medium">Subject</th>
							<th class="px-4 py-3 text-left font-medium">Sent</th>
							<th class="px-4 py-3 text-left font-medium">Message ID</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredEmails as email}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-mono text-xs">{email.from}</td>
								<td class="px-4 py-3 text-xs truncate max-w-[160px]">{email.to.join(', ')}</td>
								<td class="px-4 py-3 truncate max-w-[200px]">{email.subject || '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs whitespace-nowrap">
									{email.timestamp ? new Date(email.timestamp).toLocaleString() : '—'}
								</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground truncate max-w-[120px]">{email.messageID}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
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

<!-- Create Configuration Set Modal -->
{#if showCreateConfigSetModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Configuration Set</h2>
			<div>
				<label for="cs-name" class="block text-sm font-medium mb-1">Name *</label>
				<input
					id="cs-name"
					type="text"
					bind:value={newConfigSetName}
					placeholder="my-config-set"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateConfigSetModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createConfigSet}
					disabled={creatingConfigSet || !newConfigSetName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingConfigSet ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Receipt Rule Set Modal -->
{#if showCreateRuleSetModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Receipt Rule Set</h2>
			<div>
				<label for="rs-name" class="block text-sm font-medium mb-1">Rule Set Name *</label>
				<input
					id="rs-name"
					type="text"
					bind:value={newRuleSetName}
					placeholder="my-rule-set"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateRuleSetModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createReceiptRuleSet}
					disabled={creatingRuleSet || !newRuleSetName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingRuleSet ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
