<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getACMPCAClient } from '$lib/aws-client';
	import {
		ListCertificateAuthoritiesCommand,
		DescribeCertificateAuthorityCommand,
		DeleteCertificateAuthorityCommand,
		CreateCertificateAuthorityCommand,
		CertificateAuthorityType,
		GetCertificateAuthorityCsrCommand,
		ListPermissionsCommand,
		CreatePermissionCommand,
		DeletePermissionCommand,
		GetPolicyCommand,
		PutPolicyCommand,
		type CertificateAuthority,
		type Permission
	} from '@aws-sdk/client-acm-pca';
	import { toast } from 'svelte-sonner';
	import {
		ShieldCheck, Search, RefreshCw, Plus, Trash2,
		Activity, Info, Box, Clock, ShieldAlert,
		ChevronRight, ListFilter, Globe,
		Key, Lock, Shield, Fingerprint,
		FileCheck, AlertCircle, CheckCircle2,
		XCircle, Server, Database, Network,
		Layers, Zap, Terminal, ExternalLink, Link,
		Workflow, Share2, Timer, FileCode, UserCheck,
		Copy, Settings
	} from 'lucide-svelte';

	const acmpca = regionalClient(getACMPCAClient);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let cas = $state<CertificateAuthority[]>([]);
	let selectedCA = $state<CertificateAuthority | null>(null);
	let loadingDetails = $state(false);

	// Tab state for detail view
	type DetailTab = 'overview' | 'csr' | 'permissions' | 'policy';
	let activeTab = $state<DetailTab>('overview');

	// Modal State
	let showCreateModal = $state(false);
	let newCAName = $state('');
	let caType = $state('SUBORDINATE');
	let creating = $state(false);

	// CSR state
	let csrPem = $state('');
	let loadingCsr = $state(false);

	// Permissions state
	let permissions = $state<Permission[]>([]);
	let loadingPermissions = $state(false);
	let showAddPermissionModal = $state(false);
	let newPermPrincipal = $state('');
	let newPermSourceAccount = $state('');
	let newPermActions = $state<string[]>(['IssueCertificate', 'GetCertificate', 'ListPermissions']);
	let addingPermission = $state(false);

	// Policy state
	let policy = $state('');
	let loadingPolicy = $state(false);
	let editingPolicy = $state(false);
	let policyDraft = $state('');
	let savingPolicy = $state(false);

	// Derived
	const filteredCAs = $derived(
		cas.filter(ca => {
			const query = searchQuery.toLowerCase();
			return ca.Arn?.toLowerCase().includes(query) ||
				ca.CertificateAuthorityConfiguration?.Subject?.CommonName?.toLowerCase().includes(query);
		})
	);

	// Actions
	async function loadCAs() {
		loading = true;
		try {
			const res = await acmpca().send(new ListCertificateAuthoritiesCommand({}));
			cas = res.CertificateAuthorities ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load Private CAs: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectCA(ca: CertificateAuthority) {
		selectedCA = ca;
		activeTab = 'overview';
		loadingDetails = true;
		csrPem = '';
		permissions = [];
		policy = '';
		try {
			const res = await acmpca().send(new DescribeCertificateAuthorityCommand({ CertificateAuthorityArn: ca.Arn }));
			selectedCA = res.CertificateAuthority || ca;
		} catch (err: unknown) {
			toast.error(`Failed to load CA details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function createCA() {
		if (!newCAName.trim()) return;
		creating = true;
		try {
			await acmpca().send(new CreateCertificateAuthorityCommand({
				CertificateAuthorityType: CertificateAuthorityType[caType as keyof typeof CertificateAuthorityType],
				CertificateAuthorityConfiguration: {
					KeyAlgorithm: 'RSA_2048',
					SigningAlgorithm: 'SHA256WITHRSA',
					Subject: { CommonName: newCAName.trim() }
				}
			}));
			toast.success(`Private CA creation initiated`);
			showCreateModal = false;
			newCAName = '';
			await loadCAs();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteCA(arn: string | undefined) {
		if (!arn || !await confirmDestructive({ title: 'Delete Private CA', message: 'Delete this Private Certificate Authority? All issued certificates may be invalidated.' })) return;
		try {
			await acmpca().send(new DeleteCertificateAuthorityCommand({ CertificateAuthorityArn: arn }));
			toast.success(`CA deletion initiated`);
			if (selectedCA?.Arn === arn) selectedCA = null;
			await loadCAs();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function fetchCsr() {
		if (!selectedCA?.Arn) return;
		loadingCsr = true;
		csrPem = '';
		try {
			const res = await acmpca().send(new GetCertificateAuthorityCsrCommand({ CertificateAuthorityArn: selectedCA.Arn }));
			csrPem = res.Csr ?? '';
		} catch (err: unknown) {
			toast.error(`Failed to retrieve CSR: ${(err as Error).message}`);
		} finally {
			loadingCsr = false;
		}
	}

	async function copyCsr() {
		if (!csrPem) return;
		await navigator.clipboard.writeText(csrPem);
		toast.success('CSR copied to clipboard');
	}

	async function loadPermissions() {
		if (!selectedCA?.Arn) return;
		loadingPermissions = true;
		try {
			const res = await acmpca().send(new ListPermissionsCommand({ CertificateAuthorityArn: selectedCA.Arn }));
			permissions = res.Permissions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load permissions: ${(err as Error).message}`);
		} finally {
			loadingPermissions = false;
		}
	}

	async function addPermission() {
		if (!selectedCA?.Arn || !newPermPrincipal.trim()) return;
		addingPermission = true;
		try {
			await acmpca().send(new CreatePermissionCommand({
				CertificateAuthorityArn: selectedCA.Arn,
				Principal: newPermPrincipal.trim(),
				SourceAccount: newPermSourceAccount.trim() || undefined,
				Actions: newPermActions as ('IssueCertificate' | 'GetCertificate' | 'ListPermissions')[]
			}));
			toast.success('Permission granted');
			showAddPermissionModal = false;
			newPermPrincipal = '';
			newPermSourceAccount = '';
			newPermActions = ['IssueCertificate', 'GetCertificate', 'ListPermissions'];
			await loadPermissions();
		} catch (err: unknown) {
			toast.error(`Failed to create permission: ${(err as Error).message}`);
		} finally {
			addingPermission = false;
		}
	}

	async function revokePermission(principal: string, sourceAccount: string | undefined) {
		if (!selectedCA?.Arn || !await confirmDestructive({ title: 'Revoke Permission', message: `Revoke all permissions for ${principal}?` })) return;
		try {
			await acmpca().send(new DeletePermissionCommand({
				CertificateAuthorityArn: selectedCA.Arn,
				Principal: principal,
				SourceAccount: sourceAccount
			}));
			toast.success('Permission revoked');
			await loadPermissions();
		} catch (err: unknown) {
			toast.error(`Failed to revoke permission: ${(err as Error).message}`);
		}
	}

	async function loadPolicy() {
		if (!selectedCA?.Arn) return;
		loadingPolicy = true;
		policy = '';
		try {
			const res = await acmpca().send(new GetPolicyCommand({ ResourceArn: selectedCA.Arn }));
			policy = res.Policy ?? '';
		} catch (err: unknown) {
			// NoSuchPolicyException is expected if no policy set
			const msg = (err as Error).message ?? '';
			if (!msg.includes('NoSuchPolicy') && !msg.includes('ResourceNotFoundException')) {
				toast.error(`Failed to load policy: ${msg}`);
			}
		} finally {
			loadingPolicy = false;
		}
	}

	async function savePolicy() {
		if (!selectedCA?.Arn) return;
		savingPolicy = true;
		try {
			await acmpca().send(new PutPolicyCommand({ ResourceArn: selectedCA.Arn, Policy: policyDraft }));
			policy = policyDraft;
			editingPolicy = false;
			toast.success('Resource policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save policy: ${(err as Error).message}`);
		} finally {
			savingPolicy = false;
		}
	}

	function startEditPolicy() {
		policyDraft = policy || JSON.stringify({ Version: '2012-10-17', Statement: [] }, null, 2);
		editingPolicy = true;
	}

	async function handleTabChange(tab: DetailTab) {
		activeTab = tab;
		if (tab === 'csr' && !csrPem) await fetchCsr();
		if (tab === 'permissions' && permissions.length === 0) await loadPermissions();
		if (tab === 'policy') await loadPolicy();
	}

	function toggleAction(action: string) {
		if (newPermActions.includes(action)) {
			newPermActions = newPermActions.filter(a => a !== action);
		} else {
			newPermActions = [...newPermActions, action];
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-emerald-500';
		if (status === 'CREATING') return 'bg-amber-500 animate-pulse';
		if (status === 'DISABLED') return 'bg-rose-500';
		if (status === 'PENDING_CERTIFICATE') return 'bg-violet-500';
		return 'bg-slate-400';
	}

	onRegionChange(() => {
		loadCAs();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-emerald-600/20 rounded-xl shadow-inner">
				<ShieldCheck class="w-8 h-8 text-emerald-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-emerald-600 to-teal-600 dark:from-emerald-400 dark:to-teal-400 bg-clip-text text-transparent italic tracking-tight">Private CA Registry</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Managed private certificate authorities for internal PKI orchestration.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={loadCAs}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl font-black shadow-lg shadow-emerald-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Provision CA
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- CA List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							bind:value={searchQuery}
							placeholder="Search CAs..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-emerald-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !cas.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredCAs as ca}
							<div
								role="button"
								tabindex="0"
								onclick={() => selectCA(ca)}
								onkeydown={(e) => e.key === 'Enter' && selectCA(ca)}
								class="p-4 flex items-center justify-between hover:bg-emerald-500/5 dark:hover:bg-emerald-500/10 cursor-pointer transition-all {selectedCA?.Arn === ca.Arn ? 'bg-emerald-500/10 border-l-4 border-emerald-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<div class="w-2 h-2 rounded-full {getStatusColor(ca.Status)}"></div>
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[10px] truncate max-w-[150px]">{ca.CertificateAuthorityConfiguration?.Subject?.CommonName || 'Private CA'}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{ca.Type}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !cas.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No Private CAs tracked.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedCA}
				<!-- Tab bar -->
				<div class="flex gap-2 p-1.5 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
					{#each ([['overview', 'Overview', Shield], ['csr', 'CSR Helper', FileCode], ['permissions', 'Permissions', UserCheck], ['policy', 'Resource Policy', Settings]] as const) as [tab, label, Icon]}
						<button
							onclick={() => handleTabChange(tab)}
							class="flex items-center gap-2 flex-1 justify-center px-3 py-2 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all {activeTab === tab ? 'bg-emerald-600 text-white shadow-lg' : 'text-slate-500 dark:text-slate-400 hover:bg-emerald-500/10'}"
						>
							<Icon class="w-3.5 h-3.5" />
							{label}
						</button>
					{/each}
				</div>

				<!-- Overview Tab -->
				{#if activeTab === 'overview'}
					<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
						<!-- CA Blueprint -->
						<div class="lg:col-span-7 space-y-6">
							<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
								<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-emerald-500/5 to-teal-500/5 flex justify-between items-start">
									<div>
										<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedCA.CertificateAuthorityConfiguration?.Subject?.CommonName || 'Private CA'}</h2>
										<div class="flex items-center gap-3 mt-4">
											<div class="px-3 py-1 rounded-xl {getStatusColor(selectedCA.Status)} text-white text-[10px] font-black uppercase tracking-widest border border-white/10 shadow-sm">
												{selectedCA.Status}
											</div>
											<div class="px-2 py-1 rounded-lg bg-emerald-900/10 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-300 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
												{selectedCA.Type}
											</div>
										</div>
									</div>
									<button
										onclick={() => deleteCA(selectedCA?.Arn)}
										class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
										title="Purge Authority"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>

								<div class="p-8 space-y-8">
									<!-- Info Grid -->
									<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
										<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/info">
											<div class="flex items-center gap-2 mb-2">
												<Key class="w-3.5 h-3.5 text-emerald-500" />
												<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Cryptographic Spec</span>
											</div>
											<div class="text-[11px] font-black text-slate-800 dark:text-white uppercase truncate">{selectedCA.CertificateAuthorityConfiguration?.KeyAlgorithm}</div>
											<div class="text-[8px] text-slate-500 uppercase font-bold tracking-tighter mt-1 italic">{selectedCA.CertificateAuthorityConfiguration?.SigningAlgorithm} Signed</div>
										</div>
										<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/info">
											<div class="flex items-center gap-2 mb-2">
												<Clock class="w-3.5 h-3.5 text-emerald-500" />
												<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Authority Lifeline</span>
											</div>
											<div class="text-[11px] font-black text-slate-800 dark:text-white uppercase">Created: {selectedCA.CreatedAt?.toLocaleDateString()}</div>
											<div class="text-[8px] text-slate-500 uppercase font-bold tracking-tighter mt-1 italic">V1 Topography Active</div>
										</div>
									</div>

									<!-- ARN Explorer -->
									<div class="pt-4">
										<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-2 italic px-1">Resource Blueprint (ARN)</div>
										<div class="p-4 bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner group/arn">
											<div class="font-mono text-[9px] text-emerald-400 break-all select-all flex items-start gap-3 lowercase italic opacity-60 group-hover:opacity-100 transition-opacity">
												<Link class="w-3 h-3 mt-0.5 shrink-0 text-slate-600 group-hover:text-emerald-500 transition-colors" />
												{selectedCA.Arn}
											</div>
										</div>
									</div>
								</div>
							</div>
						</div>

						<!-- Issuance Registry -->
						<div class="lg:col-span-5 space-y-6">
							<div class="bg-slate-900 dark:bg-black rounded-[2.5rem] shadow-2xl border border-slate-800 overflow-hidden h-[600px] flex flex-col animate-in slide-in-from-right-8 duration-500">
								<div class="p-6 border-b border-white/5 bg-white/5 flex items-center justify-between">
									<div class="flex items-center gap-3">
										<Fingerprint class="w-5 h-5 text-emerald-500" />
										<div>
											<h4 class="text-xs font-black text-white uppercase tracking-widest italic leading-none">Issuance Ledger</h4>
											<span class="text-[8px] font-bold text-slate-500 uppercase">Cryptographic Identity Trace</span>
										</div>
									</div>
									<div class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20">
										<span class="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse"></span>
										<span class="text-[9px] font-black text-emerald-600 uppercase tracking-widest italic">SECURE</span>
									</div>
								</div>

								<div class="flex-1 overflow-y-auto p-8 space-y-4">
									{#if loadingDetails}
										<div class="flex flex-col items-center justify-center h-full opacity-30">
											<RefreshCw class="w-10 h-10 text-emerald-500 animate-spin mb-4" />
											<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Listing Certificates...</span>
										</div>
									{:else}
										<div class="flex flex-col items-center justify-center h-full opacity-20 p-12 text-center">
											<FileCheck class="w-16 h-16 text-slate-600 mb-6" />
											<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">Clean Ledger</h4>
											<p class="text-[10px] text-slate-600 italic leading-relaxed">No certificates have been issued through this authority yet. Coordinate with ACM to provision managed cryptographic identities.</p>
										</div>
									{/if}
								</div>
							</div>

							<!-- Security Posture -->
							<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl flex items-center justify-between group/met relative overflow-hidden">
								<div class="absolute inset-0 bg-gradient-to-r from-emerald-500/5 to-transparent pointer-events-none"></div>
								<div class="flex items-center gap-4 relative z-10">
									<div class="p-3 bg-emerald-500/10 rounded-2xl group-hover/met:scale-110 transition-transform">
										<Lock class="w-6 h-6 text-emerald-600" />
									</div>
									<div>
										<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic leading-none">PKI Security</div>
										<div class="text-lg font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">HARDENED</div>
									</div>
								</div>
							</div>
						</div>
					</div>
				{/if}

				<!-- CSR Helper Tab -->
				{#if activeTab === 'csr'}
					<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in duration-300">
						<div class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-emerald-500/5 to-teal-500/5 flex items-center justify-between">
							<div class="flex items-center gap-3">
								<FileCode class="w-5 h-5 text-emerald-500" />
								<div>
									<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-widest italic">Certificate Signing Request</h3>
									<p class="text-[9px] text-slate-500 uppercase font-bold tracking-widest mt-0.5 italic">PEM-encoded CSR for CA signing chain</p>
								</div>
							</div>
							<div class="flex gap-2">
								{#if csrPem}
									<button
										onclick={copyCsr}
										class="flex items-center gap-2 px-4 py-2 bg-slate-900 dark:bg-black text-emerald-400 hover:text-emerald-300 rounded-xl border border-slate-700 transition-all text-[10px] font-black uppercase tracking-widest"
									>
										<Copy class="w-3.5 h-3.5" />
										Copy PEM
									</button>
								{/if}
								<button
									onclick={fetchCsr}
									disabled={loadingCsr}
									class="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl transition-all text-[10px] font-black uppercase tracking-widest disabled:opacity-50"
								>
									<RefreshCw class="w-3.5 h-3.5 {loadingCsr ? 'animate-spin' : ''}" />
									{csrPem ? 'Refresh' : 'Retrieve CSR'}
								</button>
							</div>
						</div>

						<div class="p-8">
							{#if loadingCsr}
								<div class="flex flex-col items-center justify-center py-20 opacity-40">
									<RefreshCw class="w-10 h-10 text-emerald-500 animate-spin mb-4" />
									<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Retrieving CSR from ACM PCA...</span>
								</div>
							{:else if csrPem}
								<div class="space-y-4">
									<div class="p-3 bg-emerald-500/10 border border-emerald-500/20 rounded-2xl flex items-center gap-2">
										<CheckCircle2 class="w-4 h-4 text-emerald-500 shrink-0" />
										<span class="text-[10px] text-emerald-700 dark:text-emerald-400 font-black uppercase tracking-widest italic">CSR retrieved — submit to your signing CA to complete the trust chain</span>
									</div>
									<div class="bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner overflow-hidden">
										<div class="px-4 py-2 border-b border-slate-800 flex items-center gap-2">
											<div class="w-2 h-2 rounded-full bg-rose-500"></div>
											<div class="w-2 h-2 rounded-full bg-amber-500"></div>
											<div class="w-2 h-2 rounded-full bg-emerald-500"></div>
											<span class="ml-2 text-[8px] font-mono text-slate-600 uppercase tracking-widest italic">certificate-signing-request.pem</span>
										</div>
										<pre class="p-6 font-mono text-[9px] text-emerald-400 break-all whitespace-pre-wrap leading-relaxed italic opacity-80 select-all overflow-auto max-h-96">{csrPem}</pre>
									</div>
								</div>
							{:else}
								<div class="flex flex-col items-center justify-center py-20 opacity-30 text-center">
									<FileCode class="w-16 h-16 text-slate-600 mb-6" />
									<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No CSR Loaded</h4>
									<p class="text-[10px] text-slate-600 italic leading-relaxed max-w-sm">Click "Retrieve CSR" to fetch the Certificate Signing Request for this Private CA. Required to complete the signing chain for subordinate CAs.</p>
								</div>
							{/if}
						</div>
					</div>
				{/if}

				<!-- Permissions Tab -->
				{#if activeTab === 'permissions'}
					<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in duration-300">
						<div class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-emerald-500/5 to-teal-500/5 flex items-center justify-between">
							<div class="flex items-center gap-3">
								<UserCheck class="w-5 h-5 text-emerald-500" />
								<div>
									<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-widest italic">Permission Grants</h3>
									<p class="text-[9px] text-slate-500 uppercase font-bold tracking-widest mt-0.5 italic">Principal-level access control for this CA</p>
								</div>
							</div>
							<div class="flex gap-2">
								<button
									onclick={loadPermissions}
									disabled={loadingPermissions}
									class="p-2 bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl transition-all"
								>
									<RefreshCw class="w-4 h-4 text-slate-500 {loadingPermissions ? 'animate-spin' : ''}" />
								</button>
								<button
									onclick={() => showAddPermissionModal = true}
									class="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl transition-all text-[10px] font-black uppercase tracking-widest"
								>
									<Plus class="w-3.5 h-3.5" />
									Grant Permission
								</button>
							</div>
						</div>

						<div class="p-6">
							{#if loadingPermissions}
								<div class="flex flex-col items-center justify-center py-16 opacity-40">
									<RefreshCw class="w-8 h-8 text-emerald-500 animate-spin mb-3" />
									<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Loading permissions...</span>
								</div>
							{:else if permissions.length === 0}
								<div class="flex flex-col items-center justify-center py-16 opacity-30 text-center">
									<UserCheck class="w-14 h-14 text-slate-600 mb-4" />
									<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No Permissions Granted</h4>
									<p class="text-[10px] text-slate-600 italic leading-relaxed max-w-sm">No external principals have been granted access to this CA. Use "Grant Permission" to allow AWS services or accounts to request certificates.</p>
								</div>
							{:else}
								<div class="space-y-3">
									{#each permissions as perm}
										<div class="p-5 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 flex items-start justify-between gap-4 group/perm">
											<div class="flex-1 min-w-0">
												<div class="flex items-center gap-2 mb-2">
													<Shield class="w-3.5 h-3.5 text-emerald-500 shrink-0" />
													<span class="font-mono text-[10px] text-slate-800 dark:text-white font-black italic truncate">{perm.Principal}</span>
												</div>
												{#if perm.SourceAccount}
													<div class="text-[8px] text-slate-500 uppercase font-bold tracking-widest mb-2 italic">Account: {perm.SourceAccount}</div>
												{/if}
												<div class="flex flex-wrap gap-1.5 mt-2">
													{#each (perm.Actions ?? []) as action}
														<span class="px-2 py-0.5 bg-emerald-500/10 border border-emerald-500/20 text-emerald-700 dark:text-emerald-400 rounded-full text-[8px] font-black uppercase tracking-widest italic">{action}</span>
													{/each}
												</div>
											</div>
											<button
												onclick={() => revokePermission(perm.Principal ?? '', perm.SourceAccount)}
												class="p-2 text-rose-400 hover:bg-rose-500/10 rounded-xl transition-all border border-transparent hover:border-rose-500/20 opacity-0 group-hover/perm:opacity-100"
												title="Revoke permission"
											>
												<Trash2 class="w-3.5 h-3.5" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					</div>
				{/if}

				<!-- Resource Policy Tab -->
				{#if activeTab === 'policy'}
					<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in duration-300">
						<div class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-emerald-500/5 to-teal-500/5 flex items-center justify-between">
							<div class="flex items-center gap-3">
								<Settings class="w-5 h-5 text-emerald-500" />
								<div>
									<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-widest italic">Resource-Based Policy</h3>
									<p class="text-[9px] text-slate-500 uppercase font-bold tracking-widest mt-0.5 italic">JSON IAM policy attached to this CA</p>
								</div>
							</div>
							<div class="flex gap-2">
								<button
									onclick={loadPolicy}
									disabled={loadingPolicy}
									class="p-2 bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl transition-all"
								>
									<RefreshCw class="w-4 h-4 text-slate-500 {loadingPolicy ? 'animate-spin' : ''}" />
								</button>
								{#if !editingPolicy}
									<button
										onclick={startEditPolicy}
										class="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-xl transition-all text-[10px] font-black uppercase tracking-widest"
									>
										<Settings class="w-3.5 h-3.5" />
										{policy ? 'Edit Policy' : 'Set Policy'}
									</button>
								{/if}
							</div>
						</div>

						<div class="p-8">
							{#if loadingPolicy}
								<div class="flex flex-col items-center justify-center py-16 opacity-40">
									<RefreshCw class="w-8 h-8 text-emerald-500 animate-spin mb-3" />
									<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Loading policy...</span>
								</div>
							{:else if editingPolicy}
								<div class="space-y-4">
									<div class="p-3 bg-amber-500/10 border border-amber-500/20 rounded-2xl flex items-center gap-2">
										<AlertCircle class="w-4 h-4 text-amber-500 shrink-0" />
										<span class="text-[10px] text-amber-700 dark:text-amber-400 font-black uppercase tracking-widest italic">Editing resource policy — malformed JSON will be rejected</span>
									</div>
									<textarea
										bind:value={policyDraft}
										rows={16}
										class="w-full px-5 py-4 bg-slate-900 dark:bg-black border border-slate-700 rounded-3xl font-mono text-[10px] text-emerald-400 italic outline-none focus:ring-2 focus:ring-emerald-500 transition-all resize-none leading-relaxed"
										spellcheck={false}
									></textarea>
									<div class="flex gap-3">
										<button
											onclick={() => editingPolicy = false}
											class="px-5 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all"
										>Discard</button>
										<button
											onclick={savePolicy}
											disabled={savingPolicy}
											class="flex items-center gap-2 px-5 py-3 bg-emerald-600 hover:bg-emerald-700 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all disabled:opacity-50"
										>
											{#if savingPolicy}<RefreshCw class="w-3.5 h-3.5 animate-spin" />{/if}
											{savingPolicy ? 'Saving...' : 'Save Policy'}
										</button>
									</div>
								</div>
							{:else if policy}
								<div class="bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner overflow-hidden">
									<div class="px-4 py-2 border-b border-slate-800 flex items-center gap-2">
										<div class="w-2 h-2 rounded-full bg-rose-500"></div>
										<div class="w-2 h-2 rounded-full bg-amber-500"></div>
										<div class="w-2 h-2 rounded-full bg-emerald-500"></div>
										<span class="ml-2 text-[8px] font-mono text-slate-600 uppercase tracking-widest italic">resource-policy.json</span>
									</div>
									<pre class="p-6 font-mono text-[9px] text-emerald-400 whitespace-pre-wrap leading-relaxed italic opacity-80 select-all overflow-auto max-h-96">{(() => { try { return JSON.stringify(JSON.parse(policy), null, 2); } catch { return policy; } })()}</pre>
								</div>
							{:else}
								<div class="flex flex-col items-center justify-center py-16 opacity-30 text-center">
									<Settings class="w-14 h-14 text-slate-600 mb-4" />
									<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No Policy Attached</h4>
									<p class="text-[10px] text-slate-600 italic leading-relaxed max-w-sm">No resource-based IAM policy is attached to this CA. Set a policy to enable cross-account access or RAM sharing.</p>
								</div>
							{/if}
						</div>
					</div>
				{/if}

			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<ShieldCheck class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">PKI Authority Topography</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Coordinate managed private certificate authorities, oversee complex cryptographic issuance lineages, and monitor internal PKI health through an enterprise-grade control plane.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false} onkeydown={(e) => { if (e.key === 'Escape') showCreateModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-emerald-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Provision Private CA</h3>

				<form onsubmit={(e) => { e.preventDefault(); createCA(); }} class="space-y-6">
					<div>
						<label for="cName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Authority Common Name</label>
						<input
							id="cName"
							type="text"
							bind:value={newCAName}
							placeholder="e.g. gopherstack-internal-pki"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-emerald-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div>
						<label for="type" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Authority Type</label>
						<select
							id="type"
							bind:value={caType}
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-emerald-500 transition-all font-mono text-xs italic"
						>
							<option value="ROOT">ROOT (Self-Signed)</option>
							<option value="SUBORDINATE">SUBORDINATE (Chained)</option>
						</select>
					</div>

					<div class="p-5 bg-emerald-500/5 rounded-2xl border border-emerald-500/10">
						<div class="flex items-center gap-2 mb-2">
							<Shield class="w-3.5 h-3.5 text-emerald-500" />
							<span class="text-[10px] font-black text-emerald-600 uppercase tracking-widest leading-none">PKI Baseline</span>
						</div>
						<p class="text-[9px] text-emerald-800 dark:text-emerald-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Defaulting to RSA_2048 / SHA256 signing algorithm. Authority lifecycle involves complex signing rituals for subordinate chains.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-emerald-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Provisioning...' : 'Assemble Authority'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Add Permission Modal -->
{#if showAddPermissionModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showAddPermissionModal = false} onkeydown={(e) => { if (e.key === 'Escape') showAddPermissionModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-emerald-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Grant Permission</h3>
				<form onsubmit={(e) => { e.preventDefault(); addPermission(); }} class="space-y-5">
					<div>
						<label for="permPrincipal" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Principal</label>
						<input
							id="permPrincipal"
							type="text"
							bind:value={newPermPrincipal}
							placeholder="e.g. acm.amazonaws.com"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-emerald-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>
					<div>
						<label for="permSourceAccount" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Source Account (optional)</label>
						<input
							id="permSourceAccount"
							type="text"
							bind:value={newPermSourceAccount}
							placeholder="e.g. 123456789012"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-emerald-500 transition-all font-mono text-xs italic"
						/>
					</div>
					<div>
						<div class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-3 px-1 italic leading-none">Allowed Actions</div>
						<div class="space-y-2">
							{#each ['IssueCertificate', 'GetCertificate', 'ListPermissions'] as action}
								<label class="flex items-center gap-3 p-3 bg-slate-50 dark:bg-slate-900 rounded-2xl border border-slate-200 dark:border-slate-700 cursor-pointer hover:border-emerald-500/50 transition-all">
									<input
										type="checkbox"
										checked={newPermActions.includes(action)}
										onchange={() => toggleAction(action)}
										class="w-4 h-4 rounded accent-emerald-600"
									/>
									<span class="font-mono text-[10px] text-slate-700 dark:text-slate-300 font-black italic">{action}</span>
								</label>
							{/each}
						</div>
					</div>
					<div class="flex gap-4 pt-2">
						<button type="button" onclick={() => showAddPermissionModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Cancel</button>
						<button type="submit" disabled={addingPermission || newPermActions.length === 0} class="flex-1 px-4 py-4 bg-emerald-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{addingPermission ? 'Granting...' : 'Grant Access'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar */
	::-webkit-scrollbar {
		width: 6px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(148, 163, 184, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(148, 163, 184, 0.2);
	}
</style>
