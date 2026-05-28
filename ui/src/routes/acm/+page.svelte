<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getACMClient } from '$lib/aws-client';
	import {
		ListCertificatesCommand,
		DescribeCertificateCommand,
		RequestCertificateCommand,
		DeleteCertificateCommand,
		RenewCertificateCommand,
		ImportCertificateCommand,
		ExportCertificateCommand,
		ResendValidationEmailCommand,
		AddTagsToCertificateCommand,
		RemoveTagsFromCertificateCommand,
		ListTagsForCertificateCommand,
		GetAccountConfigurationCommand,
		PutAccountConfigurationCommand,
		type CertificateSummary,
		type CertificateDetail,
		type Tag
	} from '@aws-sdk/client-acm';
	import { toast } from 'svelte-sonner';
	import {
		Lock, Search, RefreshCw, Plus, Trash2, CheckCircle, XCircle, Clock,
		AlertTriangle, Eye, Download, Upload, Tag as TagIcon, Mail, Settings, Copy
	} from 'lucide-svelte';

	const acm = getACMClient();

	let loading = $state(false);
	let certificates = $state<CertificateSummary[]>([]);
	let selectedCert = $state<CertificateDetail | null>(null);
	let loadingDetail = $state(false);
	
	// Filtering
	let searchQuery = $state('');
	let statusFilter = $state('');
	let typeFilter = $state('');
	let keyAlgoFilter = $state('');

	// Selections for bulk ops
	let selectedArns = $state<Set<string>>(new Set());

	// Tabs
	let detailTab = $state<'overview' | 'sans' | 'validation' | 'tags' | 'renewal'>('overview');

	// Modals & Forms
	let showRequestModal = $state(false);
	let requesting = $state(false);
	let newDomain = $state('');
	let newSANs = $state('');
	let newValidationMethod = $state<'DNS' | 'EMAIL'>('DNS');

	let showImportModal = $state(false);
	let importing = $state(false);
	let importCert = $state('');
	let importKey = $state('');
	let importChain = $state('');

	let showExportModal = $state(false);
	let exporting = $state(false);
	let exportPassphrase = $state('');
	let exportResult = $state<{Certificate?: string, CertificateChain?: string, PrivateKey?: string} | null>(null);

	let showRevokeModal = $state(false);
	let revoking = $state(false);
	let revokeReason = $state('UNSPECIFIED');

	let showConfigModal = $state(false);
	let configDays = $state(395);
	let savingConfig = $state(false);

	// Tags
	let tags = $state<Tag[]>([]);
	let loadingTags = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');

	const filteredCerts = $derived(
		certificates.filter((c) => {
			const domainMatch = (c.DomainName ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const statusMatch = !statusFilter || c.Status === statusFilter;
			const typeMatch = !typeFilter || c.Type === typeFilter;
			const keyAlgoMatch = !keyAlgoFilter || c.KeyAlgorithm === keyAlgoFilter;
			return domainMatch && statusMatch && typeMatch && keyAlgoMatch;
		})
	);

	function toggleAll(checked: boolean) {
		if (checked) {
			selectedArns = new Set(filteredCerts.map(c => c.CertificateArn!));
		} else {
			selectedArns = new Set();
		}
	}

	function toggleOne(arn: string, checked: boolean) {
		const newSet = new Set(selectedArns);
		if (checked) newSet.add(arn);
		else newSet.delete(arn);
		selectedArns = newSet;
	}

	function expiryAlert(notAfter?: Date): 'critical' | 'warning' | null {
		if (!notAfter) return null;
		const days = (notAfter.getTime() - Date.now()) / 86400000;
		if (days <= 30) return 'critical';
		if (days <= 60) return 'warning';
		return null;
	}

	function statusBadge(status?: string) {
		if (status === 'ISSUED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'PENDING_VALIDATION') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (status === 'FAILED' || status === 'EXPIRED' || status === 'REVOKED') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	function certTypeIcon(status?: string) {
		if (status === 'ISSUED') return 'success';
		if (status === 'PENDING_VALIDATION') return 'pending';
		if (status === 'FAILED' || status === 'EXPIRED') return 'error';
		return 'unknown';
	}

	async function loadCertificates() {
		loading = true;
		try {
			const res = await acm.send(new ListCertificatesCommand({ MaxItems: 100 }));
			certificates = res.CertificateSummaryList ?? [];
			// Cleanup selectedArns
			const currentArns = new Set(certificates.map(c => c.CertificateArn));
			selectedArns = new Set([...selectedArns].filter(a => currentArns.has(a)));
		} catch (e) {
			toast.error(`Failed to load certificates: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewCertificate(arn: string) {
		loadingDetail = true;
		selectedCert = null;
		detailTab = 'overview';
		try {
			const res = await acm.send(new DescribeCertificateCommand({ CertificateArn: arn }));
			selectedCert = res.Certificate ?? null;
		} catch (e) {
			toast.error(`Failed to load certificate details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function requestCertificate() {
		if (!newDomain.trim()) return;
		requesting = true;
		try {
			const sans = newSANs.split(',').map((s) => s.trim()).filter(Boolean);
			await acm.send(
				new RequestCertificateCommand({
					DomainName: newDomain.trim(),
					SubjectAlternativeNames: sans.length > 0 ? sans : undefined,
					ValidationMethod: newValidationMethod
				})
			);
			toast.success(`Certificate requested for ${newDomain}`);
			showRequestModal = false;
			newDomain = '';
			newSANs = '';
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to request certificate: ${e}`);
		} finally {
			requesting = false;
		}
	}

	async function importCertificateCmd() {
		if (!importCert.trim() || !importKey.trim()) return;
		importing = true;
		try {
			await acm.send(new ImportCertificateCommand({
				Certificate: new TextEncoder().encode(importCert.trim()),
				PrivateKey: new TextEncoder().encode(importKey.trim()),
				CertificateChain: importChain.trim() ? new TextEncoder().encode(importChain.trim()) : undefined,
			}));
			toast.success('Certificate imported successfully');
			showImportModal = false;
			importCert = '';
			importKey = '';
			importChain = '';
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to import certificate: ${e}`);
		} finally {
			importing = false;
		}
	}

	async function deleteCertificate(arn: string, domain?: string) {
		if (!await confirmDestructive({ title: 'Delete Certificate', message: `Delete certificate for "${domain ?? arn}"? This cannot be undone.` })) return;
		try {
			await acm.send(new DeleteCertificateCommand({ CertificateArn: arn }));
			toast.success('Certificate deleted');
			if (selectedCert?.CertificateArn === arn) selectedCert = null;
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to delete certificate: ${e}`);
		}
	}

	async function bulkDelete() {
		if (selectedArns.size === 0) return;
		if (!await confirmDestructive({ title: 'Delete Certificates', message: `Delete ${selectedArns.size} certificates? This cannot be undone.` })) return;
		let successCount = 0;
		for (const arn of selectedArns) {
			try {
				await acm.send(new DeleteCertificateCommand({ CertificateArn: arn }));
				successCount++;
				if (selectedCert?.CertificateArn === arn) selectedCert = null;
			} catch (e) {
				toast.error(`Failed to delete ${arn}: ${e}`);
			}
		}
		if (successCount > 0) toast.success(`Deleted ${successCount} certificates`);
		await loadCertificates();
	}

	async function renewCertificate(arn: string) {
		try {
			await acm.send(new RenewCertificateCommand({ CertificateArn: arn }));
			toast.success('Certificate renewal initiated');
			if (selectedCert?.CertificateArn === arn) await viewCertificate(arn);
		} catch (e) {
			toast.error(`Failed to renew certificate: ${e}`);
		}
	}

	async function resendValidationEmail(arn: string, domain: string, validationDomain: string) {
		try {
			await acm.send(new ResendValidationEmailCommand({
				CertificateArn: arn,
				Domain: domain,
				ValidationDomain: validationDomain
			}));
			toast.success(`Validation email resent for ${domain}`);
		} catch (e) {
			toast.error(`Failed to resend email: ${e}`);
		}
	}

	async function doExportCertificate() {
		if (!selectedCert?.CertificateArn || !exportPassphrase) return;
		exporting = true;
		try {
			const res = await acm.send(new ExportCertificateCommand({
				CertificateArn: selectedCert.CertificateArn,
				Passphrase: new TextEncoder().encode(exportPassphrase)
			}));
			exportResult = {
				Certificate: res.Certificate,
				CertificateChain: res.CertificateChain,
				PrivateKey: res.PrivateKey
			};
			toast.success('Export successful');
		} catch (e) {
			toast.error(`Export failed: ${e}`);
		} finally {
			exporting = false;
		}
	}

	async function doRevokeCertificate() {
		if (!selectedCert?.CertificateArn) return;
		revoking = true;
		try {
			const res = await fetch(window.location.origin + '/', {
				method: 'POST',
				headers: {
					'X-Amz-Target': 'CertificateManager.RevokeCertificate',
					'Content-Type': 'application/x-amz-json-1.1'
				},
				body: JSON.stringify({ CertificateArn: selectedCert.CertificateArn, RevocationReason: revokeReason })
			});
			if (!res.ok) throw new Error(await res.text());
			toast.success('Certificate revoked');
			showRevokeModal = false;
			await viewCertificate(selectedCert.CertificateArn);
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to revoke certificate: ${e}`);
		} finally {
			revoking = false;
		}
	}

	async function loadConfig() {
		try {
			const res = await acm.send(new GetAccountConfigurationCommand({}));
			configDays = res.ExpiryEvents?.DaysBeforeExpiry ?? 45;
			showConfigModal = true;
		} catch (e) {
			toast.error(`Failed to load config: ${e}`);
		}
	}

	async function saveConfig() {
		savingConfig = true;
		try {
			await acm.send(new PutAccountConfigurationCommand({
				ExpiryEvents: { DaysBeforeExpiry: configDays },
				IdempotencyToken: Date.now().toString()
			}));
			toast.success('Account configuration saved');
			showConfigModal = false;
		} catch (e) {
			toast.error(`Failed to save config: ${e}`);
		} finally {
			savingConfig = false;
		}
	}

	async function loadTags(arn: string) {
		loadingTags = true;
		try {
			const res = await acm.send(new ListTagsForCertificateCommand({ CertificateArn: arn }));
			tags = res.Tags ?? [];
		} catch (e) {
			toast.error(`Failed to load tags: ${e}`);
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!selectedCert?.CertificateArn || !newTagKey) return;
		try {
			await acm.send(new AddTagsToCertificateCommand({
				CertificateArn: selectedCert.CertificateArn,
				Tags: [{ Key: newTagKey, Value: newTagValue }]
			}));
			toast.success('Tag added');
			newTagKey = '';
			newTagValue = '';
			await loadTags(selectedCert.CertificateArn);
		} catch (e) {
			toast.error(`Failed to add tag: ${e}`);
		}
	}

	async function removeTag(key: string, value?: string) {
		if (!selectedCert?.CertificateArn) return;
		try {
			await acm.send(new RemoveTagsFromCertificateCommand({
				CertificateArn: selectedCert.CertificateArn,
				Tags: [{ Key: key, Value: value }]
			}));
			toast.success('Tag removed');
			await loadTags(selectedCert.CertificateArn);
		} catch (e) {
			toast.error(`Failed to remove tag: ${e}`);
		}
	}

	function copyToClipboard(text: string) {
		navigator.clipboard.writeText(text);
		toast.success('Copied to clipboard');
	}

	$effect(() => {
		if (selectedCert && detailTab === 'tags') {
			loadTags(selectedCert.CertificateArn!);
		}
	});

	onMount(() => loadCertificates());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Lock class="h-8 w-8 text-green-600" />
			<div>
				<h1 class="text-2xl font-bold">Certificate Manager</h1>
				<p class="text-sm text-muted-foreground">Provision and manage SSL/TLS certificates</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={loadConfig} class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent" title="Account Configuration">
				<Settings class="h-4 w-4" />
			</button>
			<button onclick={loadCertificates} class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent">
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Filter bar -->
	<div class="flex items-center gap-3 flex-wrap">
		<div class="relative flex-1 min-w-[200px]">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search by domain..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<select bind:value={statusFilter} class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
			<option value="">All Statuses</option>
			<option value="ISSUED">Issued</option>
			<option value="PENDING_VALIDATION">Pending Validation</option>
			<option value="EXPIRED">Expired</option>
			<option value="FAILED">Failed</option>
			<option value="REVOKED">Revoked</option>
		</select>
		<select bind:value={typeFilter} class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
			<option value="">All Types</option>
			<option value="AMAZON_ISSUED">Amazon Issued</option>
			<option value="IMPORTED">Imported</option>
			<option value="PRIVATE">Private</option>
		</select>
		<select bind:value={keyAlgoFilter} class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
			<option value="">All Algorithms</option>
			<option value="RSA_2048">RSA 2048</option>
			<option value="EC_prime256v1">EC prime256v1</option>
			<option value="EC_secp384r1">EC secp384r1</option>
		</select>
		<button onclick={() => (showImportModal = true)} class="flex items-center gap-2 rounded-md border px-4 py-2 text-sm hover:bg-accent">
			<Upload class="h-4 w-4" />
			Import
		</button>
		<button onclick={() => (showRequestModal = true)} class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90">
			<Plus class="h-4 w-4" />
			Request Certificate
		</button>
	</div>

	<!-- Bulk Operations -->
	{#if selectedArns.size > 0}
		<div class="flex items-center gap-3 bg-muted/50 p-3 rounded-lg border">
			<span class="text-sm font-medium">{selectedArns.size} selected</span>
			<button onclick={bulkDelete} class="flex items-center gap-1.5 rounded-md bg-red-100 text-red-700 px-3 py-1.5 text-xs font-medium hover:bg-red-200 dark:bg-red-900 dark:text-red-300 dark:hover:bg-red-800">
				<Trash2 class="h-3.5 w-3.5" />
				Delete Selected
			</button>
		</div>
	{/if}

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredCerts.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Lock class="h-12 w-12 mb-3 opacity-30" />
			<p>No certificates found</p>
			<p class="text-sm">Request or import a certificate to get started</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="w-10 px-4 py-3">
							<input type="checkbox" onchange={(e) => toggleAll(e.currentTarget.checked)} class="rounded border-gray-300" checked={selectedArns.size === filteredCerts.length && filteredCerts.length > 0} />
						</th>
						<th class="px-4 py-3 text-left font-medium">Domain Name</th>
						<th class="px-4 py-3 text-left font-medium">Status</th>
						<th class="px-4 py-3 text-left font-medium">Type</th>
						<th class="px-4 py-3 text-left font-medium">Expires</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each filteredCerts as cert}
						{@const icon = certTypeIcon(cert.Status)}
						<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewCertificate(cert.CertificateArn ?? '')}>
							<td class="px-4 py-3" onclick={(e) => e.stopPropagation()}>
								<input type="checkbox" class="rounded border-gray-300" checked={selectedArns.has(cert.CertificateArn ?? '')} onchange={(e) => toggleOne(cert.CertificateArn ?? '', e.currentTarget.checked)} />
							</td>
							<td class="px-4 py-3">
								<div class="flex items-center gap-2">
									{#if icon === 'success'}
										<CheckCircle class="h-4 w-4 text-green-500 shrink-0" />
									{:else if icon === 'pending'}
										<Clock class="h-4 w-4 text-yellow-500 shrink-0" />
									{:else if icon === 'error'}
										<XCircle class="h-4 w-4 text-red-500 shrink-0" />
									{:else}
										<AlertTriangle class="h-4 w-4 text-muted-foreground shrink-0" />
									{/if}
									<span class="font-medium">{cert.DomainName}</span>
								</div>
							</td>
							<td class="px-4 py-3">
								<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(cert.Status)}">
									{cert.Status ?? '—'}
								</span>
							</td>
							<td class="px-4 py-3 text-muted-foreground capitalize">
								{(cert.Type ?? 'AMAZON_ISSUED').replace(/_/g, ' ').toLowerCase()}
							</td>
							<td class="px-4 py-3 text-xs">
								{#if cert.NotAfter}
									{@const alert = expiryAlert(new Date(cert.NotAfter))}
									<span class="flex items-center gap-1 {alert === 'critical' ? 'text-red-600 font-semibold' : alert === 'warning' ? 'text-yellow-600 font-medium' : 'text-muted-foreground'}">
										{#if alert === 'critical'}
											<AlertTriangle class="h-3 w-3 shrink-0" />
										{:else if alert === 'warning'}
											<Clock class="h-3 w-3 shrink-0" />
										{/if}
										{new Date(cert.NotAfter).toLocaleDateString()}
									</span>
								{:else}
									<span class="text-muted-foreground">—</span>
								{/if}
							</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button onclick={(e) => { e.stopPropagation(); viewCertificate(cert.CertificateArn ?? ''); }} class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950" title="View details">
									<Eye class="h-4 w-4" />
								</button>
								<button onclick={(e) => { e.stopPropagation(); deleteCertificate(cert.CertificateArn ?? '', cert.DomainName); }} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete certificate">
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	<!-- Certificate Detail Panel -->
	{#if loadingDetail}
		<div class="flex justify-center py-4">
			<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
		</div>
	{:else if selectedCert}
		{@const certExpiry = selectedCert.NotAfter ? new Date(selectedCert.NotAfter) : null}
		{@const expAlert = expiryAlert(certExpiry ?? undefined)}
		<div class="rounded-lg border overflow-hidden bg-background">
			<div class="flex items-center justify-between px-5 py-4 border-b bg-muted/30">
				<div class="flex items-center gap-3">
					{#if selectedCert.Status === 'ISSUED'}
						<CheckCircle class="h-5 w-5 text-green-500 shrink-0" />
					{:else if selectedCert.Status === 'PENDING_VALIDATION'}
						<Clock class="h-5 w-5 text-yellow-500 shrink-0" />
					{:else if selectedCert.Status === 'FAILED' || selectedCert.Status === 'EXPIRED'}
						<XCircle class="h-5 w-5 text-red-500 shrink-0" />
					{:else}
						<AlertTriangle class="h-5 w-5 text-muted-foreground shrink-0" />
					{/if}
					<div>
						<h3 class="font-semibold">{selectedCert.DomainName}</h3>
						<div class="flex items-center gap-2 mt-0.5">
							<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(selectedCert.Status)}">
								{selectedCert.Status}
							</span>
							{#if expAlert === 'critical'}
								<span class="flex items-center gap-1 rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700 dark:bg-red-900 dark:text-red-300">
									<AlertTriangle class="h-3 w-3" />
									Expires soon
								</span>
							{:else if expAlert === 'warning'}
								<span class="flex items-center gap-1 rounded-full bg-yellow-100 px-2 py-0.5 text-xs font-medium text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300">
									<Clock class="h-3 w-3" />
									Expiring soon
								</span>
							{/if}
						</div>
					</div>
				</div>
				<div class="flex gap-2">
					<button onclick={() => { exportResult = null; showExportModal = true; }} class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent">
						<Download class="h-3.5 w-3.5" />
						Export
					</button>
					{#if selectedCert.Status === 'ISSUED'}
						<button onclick={() => renewCertificate(selectedCert?.CertificateArn ?? '')} class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent">
							<RefreshCw class="h-3.5 w-3.5" />
							Renew
						</button>
						<button onclick={() => showRevokeModal = true} class="flex items-center gap-1.5 rounded-md border border-orange-200 px-3 py-1.5 text-xs text-orange-600 hover:bg-orange-50 dark:border-orange-800 dark:hover:bg-orange-950">
							<AlertTriangle class="h-3.5 w-3.5" />
							Revoke
						</button>
					{/if}
					<button onclick={() => deleteCertificate(selectedCert?.CertificateArn ?? '', selectedCert?.DomainName)} class="flex items-center gap-1.5 rounded-md border border-red-200 px-3 py-1.5 text-xs text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-950">
						<Trash2 class="h-3.5 w-3.5" />
						Delete
					</button>
					<button onclick={() => (selectedCert = null)} class="text-xs text-muted-foreground hover:text-foreground px-2">
						Close
					</button>
				</div>
			</div>

			<div class="flex border-b overflow-x-auto">
				{#each [['overview', 'Overview'], ['sans', 'SANs'], ['validation', 'Validation'], ['tags', 'Tags'], ['renewal', 'Renewal']] as [tab, label]}
					<button onclick={() => (detailTab = tab as typeof detailTab)} class="px-4 py-2.5 text-sm font-medium border-b-2 -mb-px transition-colors whitespace-nowrap {detailTab === tab ? 'border-primary text-foreground' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						{label}
						{#if tab === 'sans'}
							<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{(selectedCert.SubjectAlternativeNames ?? []).length}</span>
						{:else if tab === 'validation'}
							<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{(selectedCert.DomainValidationOptions ?? []).length}</span>
						{:else if tab === 'tags'}
							<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{tags.length}</span>
						{/if}
					</button>
				{/each}
			</div>

			<div class="p-5">
				{#if detailTab === 'overview'}
					<div class="grid grid-cols-2 gap-x-8 gap-y-4 text-sm">
						<div class="col-span-2">
							<p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">ARN</p>
							<div class="flex items-center gap-2">
								<p class="font-mono text-xs break-all">{selectedCert.CertificateArn ?? '—'}</p>
								<button onclick={() => copyToClipboard(selectedCert?.CertificateArn ?? '')} class="text-muted-foreground hover:text-foreground"><Copy class="h-3.5 w-3.5" /></button>
							</div>
						</div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Type</p><p>{selectedCert.Type ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Status</p><p>{selectedCert.Status ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Issued At</p><p>{selectedCert.IssuedAt ? new Date(selectedCert.IssuedAt).toLocaleString() : '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Imported At</p><p>{selectedCert.ImportedAt ? new Date(selectedCert.ImportedAt).toLocaleString() : '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Expires At</p><p class="{expAlert === 'critical' ? 'text-red-600 font-semibold' : expAlert === 'warning' ? 'text-yellow-600 font-medium' : ''}">{certExpiry ? certExpiry.toLocaleString() : '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Revoked At</p><p>{selectedCert.RevokedAt ? new Date(selectedCert.RevokedAt).toLocaleString() : '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Revocation Reason</p><p>{selectedCert.RevocationReason ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Issuer</p><p>{selectedCert.Issuer ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Key Algorithm</p><p>{selectedCert.KeyAlgorithm ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Signature Algorithm</p><p>{selectedCert.SignatureAlgorithm ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">In Use By</p><p>{(selectedCert.InUseBy ?? []).length > 0 ? `${(selectedCert.InUseBy ?? []).length} resource(s)` : 'Not in use'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Failure Reason</p><p>{selectedCert.FailureReason ?? '—'}</p></div>
						<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Certificate Authority ARN</p><p class="font-mono text-xs">{selectedCert.CertificateAuthorityArn ?? '—'}</p></div>
					</div>
				{:else if detailTab === 'sans'}
					{@const sans = selectedCert.SubjectAlternativeNames ?? []}
					{#if sans.length === 0}
						<p class="text-sm text-muted-foreground py-4 text-center">No Subject Alternative Names</p>
					{:else}
						<div class="flex flex-wrap gap-1.5">
							{#each sans as san}
								<span class="rounded bg-muted px-2.5 py-1 text-xs font-mono">{san}</span>
							{/each}
						</div>
					{/if}
				{:else if detailTab === 'validation'}
					{@const opts = selectedCert.DomainValidationOptions ?? []}
					{#if opts.length === 0}
						<p class="text-sm text-muted-foreground py-4 text-center">No validation records</p>
					{:else}
						<div class="rounded border overflow-hidden">
							<table class="w-full text-xs">
								<thead class="bg-muted/50">
									<tr>
										<th class="px-3 py-2 text-left font-medium">Domain</th>
										<th class="px-3 py-2 text-left font-medium">Status</th>
										<th class="px-3 py-2 text-left font-medium">Method</th>
										<th class="px-3 py-2 text-left font-medium">Record Name</th>
										<th class="px-3 py-2 text-left font-medium">Record Value</th>
										<th class="px-3 py-2 text-right font-medium">Actions</th>
									</tr>
								</thead>
								<tbody class="divide-y">
									{#each opts as opt}
										<tr class="hover:bg-muted/30">
											<td class="px-3 py-2 font-medium">{opt.DomainName ?? '—'}</td>
											<td class="px-3 py-2">
												{#if opt.ValidationStatus === 'SUCCESS'}
													<span class="flex items-center gap-1 text-green-600"><CheckCircle class="h-3 w-3" /> Validated</span>
												{:else if opt.ValidationStatus === 'PENDING_VALIDATION'}
													<span class="flex items-center gap-1 text-yellow-600"><Clock class="h-3 w-3" /> Pending</span>
												{:else if opt.ValidationStatus === 'FAILED'}
													<span class="flex items-center gap-1 text-red-600"><XCircle class="h-3 w-3" /> Failed</span>
												{:else}
													<span class="text-muted-foreground">{opt.ValidationStatus ?? '—'}</span>
												{/if}
											</td>
											<td class="px-3 py-2 text-muted-foreground">{opt.ValidationMethod ?? '—'}</td>
											<td class="px-3 py-2 font-mono">
												{#if opt.ResourceRecord?.Name}
													<div class="flex items-center justify-between gap-2">
														<span class="block truncate max-w-[150px]" title={opt.ResourceRecord.Name}>{opt.ResourceRecord.Name}</span>
														<button onclick={() => copyToClipboard(opt.ResourceRecord!.Name!)} class="text-muted-foreground hover:text-foreground"><Copy class="h-3 w-3" /></button>
													</div>
												{:else}
													<span class="text-muted-foreground">—</span>
												{/if}
											</td>
											<td class="px-3 py-2 font-mono">
												{#if opt.ResourceRecord?.Value}
													<div class="flex items-center justify-between gap-2">
														<span class="block truncate max-w-[150px]" title={opt.ResourceRecord.Value}>{opt.ResourceRecord.Value}</span>
														<button onclick={() => copyToClipboard(opt.ResourceRecord!.Value!)} class="text-muted-foreground hover:text-foreground"><Copy class="h-3 w-3" /></button>
													</div>
												{:else}
													<span class="text-muted-foreground">—</span>
												{/if}
											</td>
											<td class="px-3 py-2 text-right">
												{#if opt.ValidationMethod === 'EMAIL' && opt.ValidationStatus === 'PENDING_VALIDATION'}
													<button onclick={() => resendValidationEmail(selectedCert?.CertificateArn ?? '', opt.DomainName ?? '', opt.ValidationDomain ?? opt.DomainName ?? '')} class="text-blue-500 hover:text-blue-700" title="Resend Email">
														<Mail class="h-4 w-4" />
													</button>
												{/if}
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				{:else if detailTab === 'tags'}
					<div class="space-y-4">
						<div class="flex items-center gap-2">
							<input type="text" placeholder="Key" bind:value={newTagKey} class="rounded border bg-background px-2 py-1 text-sm flex-1" />
							<input type="text" placeholder="Value" bind:value={newTagValue} class="rounded border bg-background px-2 py-1 text-sm flex-1" />
							<button onclick={addTag} disabled={!newTagKey} class="rounded bg-primary px-3 py-1 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">Add Tag</button>
						</div>
						{#if loadingTags}
							<div class="flex justify-center py-4"><RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" /></div>
						{:else if tags.length === 0}
							<p class="text-sm text-muted-foreground text-center py-4">No tags present.</p>
						{:else}
							<div class="rounded border">
								<table class="w-full text-sm">
									<thead class="bg-muted/50"><tr><th class="px-3 py-2 text-left font-medium">Key</th><th class="px-3 py-2 text-left font-medium">Value</th><th class="w-10"></th></tr></thead>
									<tbody class="divide-y">
										{#each tags as tag}
											<tr>
												<td class="px-3 py-2 font-medium">{tag.Key}</td>
												<td class="px-3 py-2 text-muted-foreground">{tag.Value ?? ''}</td>
												<td class="px-3 py-2 text-right">
													<button onclick={() => removeTag(tag.Key!, tag.Value)} class="text-red-500 hover:text-red-700"><Trash2 class="h-4 w-4" /></button>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				{:else if detailTab === 'renewal'}
					<div class="space-y-4 text-sm">
						<div class="grid grid-cols-2 gap-4">
							<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Renewal Eligibility</p><p>{selectedCert.RenewalEligibility ?? '—'}</p></div>
							<div><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Renewal Status</p><p>{selectedCert.RenewalSummary?.RenewalStatus ?? '—'}</p></div>
							<div class="col-span-2"><p class="text-xs text-muted-foreground uppercase tracking-wide mb-1">Renewal Status Reason</p><p>{selectedCert.RenewalSummary?.RenewalStatusReason ?? '—'}</p></div>
						</div>
						{#if selectedCert.RenewalSummary?.DomainValidationOptions && selectedCert.RenewalSummary.DomainValidationOptions.length > 0}
							<h4 class="font-medium mt-4 mb-2">Renewal Validation Options</h4>
							<div class="rounded border overflow-hidden">
								<table class="w-full text-xs">
									<thead class="bg-muted/50"><tr><th class="px-3 py-2 text-left">Domain</th><th class="px-3 py-2 text-left">Status</th><th class="px-3 py-2 text-left">Method</th></tr></thead>
									<tbody class="divide-y">
										{#each selectedCert.RenewalSummary.DomainValidationOptions as opt}
											<tr class="hover:bg-muted/30">
												<td class="px-3 py-2 font-medium">{opt.DomainName ?? '—'}</td>
												<td class="px-3 py-2">{opt.ValidationStatus ?? '—'}</td>
												<td class="px-3 py-2">{opt.ValidationMethod ?? '—'}</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>

{#if showRequestModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl border">
			<h2 class="text-lg font-semibold mb-4">Request Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="cert-domain" class="block text-sm font-medium mb-1">Domain Name *</label>
					<input id="cert-domain" type="text" bind:value={newDomain} placeholder="example.com" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="cert-sans" class="block text-sm font-medium mb-1">Subject Alternative Names (comma-separated)</label>
					<input id="cert-sans" type="text" bind:value={newSANs} placeholder="www.example.com, api.example.com" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="cert-validation" class="block text-sm font-medium mb-1">Validation Method</label>
					<select id="cert-validation" bind:value={newValidationMethod} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="DNS">DNS</option>
						<option value="EMAIL">Email</option>
					</select>
				</div>
			</div>
			<div class="mt-5 flex justify-end gap-2">
				<button onclick={() => (showRequestModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={requestCertificate} disabled={requesting || !newDomain.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{requesting ? 'Requesting...' : 'Request Certificate'}
				</button>
			</div>
		</div>
	</div>
{/if}

{#if showImportModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl border">
			<h2 class="text-lg font-semibold mb-4">Import Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="import-cert" class="block text-sm font-medium mb-1">Certificate body (PEM) *</label>
					<textarea id="import-cert" bind:value={importCert} rows={3} class="w-full font-mono text-xs rounded-md border bg-background px-3 py-2 focus:outline-none focus:ring-2 focus:ring-primary" placeholder="-----BEGIN CERTIFICATE-----"></textarea>
				</div>
				<div>
					<label for="import-key" class="block text-sm font-medium mb-1">Certificate private key (PEM) *</label>
					<textarea id="import-key" bind:value={importKey} rows={3} class="w-full font-mono text-xs rounded-md border bg-background px-3 py-2 focus:outline-none focus:ring-2 focus:ring-primary" placeholder="-----BEGIN PRIVATE KEY-----"></textarea>
				</div>
				<div>
					<label for="import-chain" class="block text-sm font-medium mb-1">Certificate chain (PEM)</label>
					<textarea id="import-chain" bind:value={importChain} rows={3} class="w-full font-mono text-xs rounded-md border bg-background px-3 py-2 focus:outline-none focus:ring-2 focus:ring-primary" placeholder="-----BEGIN CERTIFICATE-----"></textarea>
				</div>
			</div>
			<div class="mt-5 flex justify-end gap-2">
				<button onclick={() => (showImportModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={importCertificateCmd} disabled={importing || !importCert.trim() || !importKey.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{importing ? 'Importing...' : 'Import'}
				</button>
			</div>
		</div>
	</div>
{/if}

{#if showExportModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl border">
			<h2 class="text-lg font-semibold mb-4">Export Certificate</h2>
			{#if !exportResult}
				<div class="space-y-3">
					<p class="text-sm text-muted-foreground">Exporting a certificate requires encrypting the private key with a passphrase.</p>
					<div>
						<label for="export-pass" class="block text-sm font-medium mb-1">Passphrase *</label>
						<input id="export-pass" type="password" bind:value={exportPassphrase} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
				</div>
				<div class="mt-5 flex justify-end gap-2">
					<button onclick={() => (showExportModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
					<button onclick={doExportCertificate} disabled={exporting || !exportPassphrase} class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
						{exporting ? 'Exporting...' : 'Export'}
					</button>
				</div>
			{:else}
				<div class="space-y-3 max-h-[60vh] overflow-y-auto">
					<div>
						<label class="block text-sm font-medium mb-1">Certificate</label>
						<textarea readonly value={exportResult.Certificate} rows={4} class="w-full font-mono text-xs rounded border bg-muted px-3 py-2"></textarea>
					</div>
					<div>
						<label class="block text-sm font-medium mb-1">Private Key (Encrypted)</label>
						<textarea readonly value={exportResult.PrivateKey} rows={4} class="w-full font-mono text-xs rounded border bg-muted px-3 py-2"></textarea>
					</div>
					{#if exportResult.CertificateChain}
						<div>
							<label class="block text-sm font-medium mb-1">Certificate Chain</label>
							<textarea readonly value={exportResult.CertificateChain} rows={4} class="w-full font-mono text-xs rounded border bg-muted px-3 py-2"></textarea>
						</div>
					{/if}
				</div>
				<div class="mt-5 flex justify-end">
					<button onclick={() => (showExportModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Close</button>
				</div>
			{/if}
		</div>
	</div>
{/if}

{#if showRevokeModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl border">
			<h2 class="text-lg font-semibold mb-4 text-red-600 flex items-center gap-2">
				<AlertTriangle class="h-5 w-5" /> Revoke Certificate
			</h2>
			<p class="text-sm text-muted-foreground mb-4">Are you sure you want to revoke this certificate? This action cannot be undone.</p>
			<div class="space-y-3">
				<div>
					<label for="revoke-reason" class="block text-sm font-medium mb-1">Revocation Reason</label>
					<select id="revoke-reason" bind:value={revokeReason} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="UNSPECIFIED">Unspecified</option>
						<option value="KEY_COMPROMISE">Key Compromise</option>
						<option value="CA_COMPROMISE">CA Compromise</option>
						<option value="AFFILIATION_CHANGED">Affiliation Changed</option>
						<option value="SUPERSEDED">Superseded</option>
						<option value="CESSATION_OF_OPERATION">Cessation of Operation</option>
						<option value="CERTIFICATE_HOLD">Certificate Hold</option>
						<option value="REMOVE_FROM_CRL">Remove from CRL</option>
						<option value="PRIVILEGE_WITHDRAWN">Privilege Withdrawn</option>
						<option value="A_A_COMPROMISE">AA Compromise</option>
					</select>
				</div>
			</div>
			<div class="mt-5 flex justify-end gap-2">
				<button onclick={() => (showRevokeModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={doRevokeCertificate} disabled={revoking} class="rounded-md bg-red-600 px-4 py-2 text-sm text-white hover:bg-red-700 disabled:opacity-50">
					{revoking ? 'Revoking...' : 'Revoke Certificate'}
				</button>
			</div>
		</div>
	</div>
{/if}

{#if showConfigModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl border">
			<h2 class="text-lg font-semibold mb-4">Account Configuration</h2>
			<div class="space-y-3">
				<div>
					<label for="config-days" class="block text-sm font-medium mb-1">Days Before Expiry Event</label>
					<input id="config-days" type="number" min="1" max="365" bind:value={configDays} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					<p class="text-xs text-muted-foreground mt-1">Number of days before expiration to start generating CloudWatch events.</p>
				</div>
			</div>
			<div class="mt-5 flex justify-end gap-2">
				<button onclick={() => (showConfigModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={saveConfig} disabled={savingConfig} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{savingConfig ? 'Saving...' : 'Save Configuration'}
				</button>
			</div>
		</div>
	</div>
{/if}
