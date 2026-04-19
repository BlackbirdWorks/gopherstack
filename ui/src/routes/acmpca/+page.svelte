<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getACMPCAClient } from '$lib/aws-client';
	import {
		ListCertificateAuthoritiesCommand,
		DescribeCertificateAuthorityCommand,
		DeleteCertificateAuthorityCommand,
		CreateCertificateAuthorityCommand,
		CertificateAuthorityType,
		type CertificateAuthority
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
		Workflow, Share2, Timer
	} from 'lucide-svelte';

	const acmpca = getACMPCAClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let cas = $state<CertificateAuthority[]>([]);
	let selectedCA = $state<CertificateAuthority | null>(null);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let newCAName = $state('');
	let caType = $state('SUBORDINATE');
	let creating = $state(false);

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
			const res = await acmpca.send(new ListCertificateAuthoritiesCommand({}));
			cas = res.CertificateAuthorities ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load Private CAs: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectCA(ca: CertificateAuthority) {
		selectedCA = ca;
		loadingDetails = true;
		try {
			const res = await acmpca.send(new DescribeCertificateAuthorityCommand({ CertificateAuthorityArn: ca.Arn }));
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
			await acmpca.send(new CreateCertificateAuthorityCommand({
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
			await acmpca.send(new DeleteCertificateAuthorityCommand({ CertificateAuthorityArn: arn }));
			toast.success(`CA deletion initiated`);
			if (selectedCA?.Arn === arn) selectedCA = null;
			await loadCAs();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-emerald-500';
		if (status === 'CREATING') return 'bg-amber-500 animate-pulse';
		if (status === 'DISABLED') return 'bg-rose-500';
		if (status === 'PENDING_CERTIFICATE') return 'bg-violet-500';
		return 'bg-slate-400';
	}

	onMount(() => {
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
