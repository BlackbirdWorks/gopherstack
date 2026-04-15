<script lang="ts">
	import { onMount } from 'svelte';
	import { getACMClient } from '$lib/aws-client';
	import {
		ListCertificatesCommand,
		DescribeCertificateCommand,
		RequestCertificateCommand,
		DeleteCertificateCommand,
		RenewCertificateCommand,
		type CertificateSummary,
		type CertificateDetail
	} from '@aws-sdk/client-acm';
	import { toast } from 'svelte-sonner';
	import {
		Lock,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		CheckCircle,
		XCircle,
		Clock,
		AlertTriangle,
		Eye
	} from 'lucide-svelte';

	const acm = getACMClient();

	let loading = $state(false);
	let certificates = $state<CertificateSummary[]>([]);
	let selectedCert = $state<CertificateDetail | null>(null);
	let loadingDetail = $state(false);
	let searchQuery = $state('');
	let statusFilter = $state('all');

	let showRequestModal = $state(false);
	let requesting = $state(false);
	let newDomain = $state('');
	let newSANs = $state('');
	let newValidationMethod = $state<'DNS' | 'EMAIL'>('DNS');

	const filteredCerts = $derived(
		certificates.filter((c) => {
			const domainMatch = (c.DomainName ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const statusMatch = statusFilter === 'all' || c.Status === statusFilter;
			return domainMatch && statusMatch;
		})
	);

	function statusBadge(status?: string) {
		if (status === 'ISSUED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'PENDING_VALIDATION')
			return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (status === 'FAILED' || status === 'EXPIRED' || status === 'REVOKED')
			return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
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
		} catch (e) {
			toast.error(`Failed to load certificates: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewCertificate(arn: string) {
		loadingDetail = true;
		selectedCert = null;
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
			const sans = newSANs
				.split(',')
				.map((s) => s.trim())
				.filter(Boolean);
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

	async function deleteCertificate(arn: string, domain?: string) {
		if (!confirm(`Delete certificate for "${domain ?? arn}"?`)) return;
		try {
			await acm.send(new DeleteCertificateCommand({ CertificateArn: arn }));
			toast.success('Certificate deleted');
			if (selectedCert?.CertificateArn === arn) selectedCert = null;
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to delete certificate: ${e}`);
		}
	}

	async function renewCertificate(arn: string) {
		try {
			await acm.send(new RenewCertificateCommand({ CertificateArn: arn }));
			toast.success('Certificate renewal initiated');
		} catch (e) {
			toast.error(`Failed to renew certificate: ${e}`);
		}
	}

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
		<button
			onclick={loadCertificates}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Filter bar -->
	<div class="flex items-center gap-3">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search by domain..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<select
			bind:value={statusFilter}
			class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
		>
			<option value="all">All Statuses</option>
			<option value="ISSUED">Issued</option>
			<option value="PENDING_VALIDATION">Pending Validation</option>
			<option value="EXPIRED">Expired</option>
			<option value="FAILED">Failed</option>
		</select>
		<button
			onclick={() => (showRequestModal = true)}
			class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
		>
			<Plus class="h-4 w-4" />
			Request Certificate
		</button>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredCerts.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Lock class="h-12 w-12 mb-3 opacity-30" />
			<p>No certificates found</p>
			<p class="text-sm">Request a certificate to get started</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
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
						<tr
							class="hover:bg-muted/30 cursor-pointer"
							onclick={() => viewCertificate(cert.CertificateArn ?? '')}
						>
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
							<td class="px-4 py-3 text-muted-foreground text-xs">
								{cert.NotAfter ? new Date(cert.NotAfter).toLocaleDateString() : '—'}
							</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button
									onclick={(e) => { e.stopPropagation(); viewCertificate(cert.CertificateArn ?? ''); }}
									class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
									title="View details"
								>
									<Eye class="h-4 w-4" />
								</button>
								<button
									onclick={(e) => { e.stopPropagation(); deleteCertificate(cert.CertificateArn ?? '', cert.DomainName); }}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete certificate"
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

	<!-- Certificate Detail Panel -->
	{#if loadingDetail}
		<div class="flex justify-center py-4">
			<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
		</div>
	{:else if selectedCert}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="font-semibold">{selectedCert.DomainName}</h3>
				<div class="flex gap-2">
					{#if selectedCert.Status === 'ISSUED'}
						<button
							onclick={() => renewCertificate(selectedCert?.CertificateArn ?? '')}
							class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent"
						>
							<RefreshCw class="h-3.5 w-3.5" />
							Renew
						</button>
					{/if}
					<button onclick={() => (selectedCert = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-4 text-sm">
				<div>
					<p class="text-muted-foreground">Status</p>
					<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(selectedCert.Status)}">
						{selectedCert.Status}
					</span>
				</div>
				<div>
					<p class="text-muted-foreground">Type</p>
					<p>{selectedCert.Type ?? '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Issued</p>
					<p>{selectedCert.IssuedAt ? new Date(selectedCert.IssuedAt).toLocaleDateString() : '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Expires</p>
					<p>{selectedCert.NotAfter ? new Date(selectedCert.NotAfter).toLocaleDateString() : '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Issuer</p>
					<p>{selectedCert.Issuer ?? 'Amazon'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Key Algorithm</p>
					<p>{selectedCert.KeyAlgorithm ?? '—'}</p>
				</div>
			</div>
			{#if (selectedCert.SubjectAlternativeNames ?? []).length > 0}
				<div>
					<p class="text-sm text-muted-foreground mb-1">Subject Alternative Names</p>
					<div class="flex flex-wrap gap-1">
						{#each selectedCert.SubjectAlternativeNames ?? [] as san}
							<span class="rounded bg-muted px-2 py-0.5 text-xs font-mono">{san}</span>
						{/each}
					</div>
				</div>
			{/if}
			{#if (selectedCert.DomainValidationOptions ?? []).length > 0}
				<div>
					<p class="text-sm font-medium mb-2">DNS Validation Records</p>
					<div class="rounded border overflow-hidden">
						<table class="w-full text-xs">
							<thead class="bg-muted/50">
								<tr>
									<th class="px-3 py-2 text-left">Domain</th>
									<th class="px-3 py-2 text-left">CNAME Name</th>
									<th class="px-3 py-2 text-left">CNAME Value</th>
								</tr>
							</thead>
							<tbody class="divide-y">
								{#each selectedCert.DomainValidationOptions ?? [] as opt}
									<tr>
										<td class="px-3 py-2">{opt.DomainName}</td>
										<td class="px-3 py-2 font-mono truncate max-w-[180px]">
											{opt.ResourceRecord?.Name ?? '—'}
										</td>
										<td class="px-3 py-2 font-mono truncate max-w-[180px]">
											{opt.ResourceRecord?.Value ?? '—'}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Request Certificate Modal -->
{#if showRequestModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Request Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="cert-domain" class="block text-sm font-medium mb-1">Domain Name *</label>
					<input
						id="cert-domain"
						type="text"
						bind:value={newDomain}
						placeholder="example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="cert-sans" class="block text-sm font-medium mb-1"
						>Subject Alternative Names (comma-separated)</label
					>
					<input
						id="cert-sans"
						type="text"
						bind:value={newSANs}
						placeholder="www.example.com, api.example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="cert-validation" class="block text-sm font-medium mb-1"
						>Validation Method</label
					>
					<select
						id="cert-validation"
						bind:value={newValidationMethod}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="DNS">DNS</option>
						<option value="EMAIL">Email</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showRequestModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={requestCertificate}
					disabled={requesting || !newDomain.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{requesting ? 'Requesting...' : 'Request Certificate'}
				</button>
			</div>
		</div>
	</div>
{/if}
