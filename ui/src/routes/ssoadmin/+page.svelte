<script lang="ts">
	import { onMount } from 'svelte';
	import {
		AddRegionCommand,
		CreateAccountAssignmentCommand,
		CreateApplicationCommand,
		CreateInstanceCommand,
		CreatePermissionSetCommand,
		CreateTrustedTokenIssuerCommand,
		DeleteAccountAssignmentCommand,
		DeleteApplicationCommand,
		DeleteInstanceCommand,
		DeletePermissionSetCommand,
		DeleteTrustedTokenIssuerCommand,
		DescribeInstanceCommand,
		DescribePermissionSetCommand,
		GetInlinePolicyForPermissionSetCommand,
		ListAccountAssignmentCreationStatusCommand,
		ListAccountAssignmentDeletionStatusCommand,
		ListAccountAssignmentsCommand,
		ListApplicationAssignmentsCommand,
		ListApplicationsCommand,
		ListInstancesCommand,
		ListPermissionSetProvisioningStatusCommand,
		ListPermissionSetsCommand,
		ListRegionsCommand,
		ListTrustedTokenIssuersCommand,
		ProvisionPermissionSetCommand,
		PutInlinePolicyToPermissionSetCommand,
		DeleteInlinePolicyFromPermissionSetCommand,
		RemoveRegionCommand,
		UpdateApplicationCommand,
		type SSOAdminClient
	} from '@aws-sdk/client-sso-admin';
	import { toast } from 'svelte-sonner';

	import { getSSOAdminClient } from '$lib/aws-client';
	import { confirmDestructive } from '$lib/confirm-dialog';

	let ssoadminClient: SSOAdminClient | undefined;
	function ssoadmin(): SSOAdminClient {
		return (ssoadminClient ??= getSSOAdminClient());
	}
	const defaultAccountID = '123456789012';
	const defaultProviderArn = 'arn:aws:sso::123456789012:applicationProvider/custom';

	type TabId =
		| 'instances'
		| 'permissionsets'
		| 'assignments'
		| 'applications'
		| 'tokenissuers'
		| 'provisioning'
		| 'metrics'
		| 'docs';

	let activeTab = $state<TabId>('instances');
	let instances = $state<
		Array<{ InstanceArn?: string; Name?: string; IdentityStoreId?: string; CreatedDate?: Date }>
	>([]);
	let permissionSetArns = $state<string[]>([]);
	let permissionSetDetails = $state<
		Record<
			string,
			{
				Name?: string;
				Description?: string;
				SessionDuration?: string;
				hasInlinePolicy?: boolean;
				assignmentCount?: number;
			}
		>
	>({});
	let accountAssignments = $state<
		Array<{ PrincipalId?: string; PrincipalType?: string; AccountId?: string }>
	>([]);
	let applications = $state<
		Array<{
			ApplicationArn?: string;
			Name?: string;
			Status?: string;
			ApplicationProviderArn?: string;
			Description?: string;
		}>
	>([]);
	let applicationAssignmentCounts = $state<Record<string, number>>({});
	let trustedTokenIssuers = $state<
		Array<{
			TrustedTokenIssuerArn?: string;
			Name?: string;
			TrustedTokenIssuerType?: string;
			TrustedTokenIssuerConfiguration?: {
				OidcJwtConfiguration?: {
					IssuerUrl?: string;
					ClaimAttributePath?: string;
				};
			};
		}>
	>([]);
	let instanceRegions = $state<Array<{ Region?: string; RegionScopeType?: string }>>([]);

	let creationStatuses = $state<
		Array<{ RequestId?: string; Status?: string; CreatedDate?: Date }>
	>([]);
	let deletionStatuses = $state<
		Array<{ RequestId?: string; Status?: string; CreatedDate?: Date }>
	>([]);
	let provisioningStatuses = $state<
		Array<{ RequestId?: string; Status?: string; CreatedDate?: Date }>
	>([]);

	// Principal lookup state.
	let principalLookupID = $state('');
	let principalLookupType = $state<'USER' | 'GROUP'>('USER');
	let principalAssignments = $state<
		Array<{ PrincipalId?: string; PrincipalType?: string; AccountId?: string; PermissionSetArn?: string }>
	>([]);
	let principalLookupLoading = $state(false);

	// Accounts for provisioned PS state.
	let accountsForPS = $state<string[]>([]);
	let accountsForPSLoading = $state(false);

	let selectedInstanceArn = $state('');
	let selectedPermissionSetArn = $state('');
	let selectedAccountID = $state(defaultAccountID);
	let instanceDetails = $state<{
		Name?: string;
		IdentityStoreId?: string;
		OwnerAccountId?: string;
		CreatedDate?: Date;
	} | null>(null);

	let newInstanceName = $state('');
	let newPermissionSetName = $state('');
	let assignmentPrincipalID = $state('');
	let assignmentPrincipalType = $state<'USER' | 'GROUP'>('USER');
	let newApplicationName = $state('');
	let newTrustedTokenIssuerName = $state('');
	let newRegionName = $state('');

	let metricsInstanceCount = $state(0);
	let metricsPermSetCount = $state(0);
	let metricsAppCount = $state(0);
	let metricsTTICount = $state(0);
	let metricsAssignmentCount = $state(0);
	let metricsRegionCount = $state(0);

	function formatDate(d?: Date | null): string {
		if (!d) return '\u2014';
		return new Date(d).toLocaleString();
	}

	async function copyToClipboard(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Failed to copy');
		}
	}

	async function loadInstances() {
		const out = await ssoadmin().send(new ListInstancesCommand({}));
		instances = out.Instances ?? [];
		if (!selectedInstanceArn && instances.length > 0) {
			selectedInstanceArn = instances[0].InstanceArn ?? '';
		}
		metricsInstanceCount = instances.length;
	}

	async function describeInstance(instanceArn: string) {
		if (!instanceArn) {
			instanceDetails = null;
			return;
		}
		const out = await ssoadmin().send(new DescribeInstanceCommand({ InstanceArn: instanceArn }));
		instanceDetails = out.InstanceArn
			? {
					Name: out.Name,
					IdentityStoreId: out.IdentityStoreId,
					OwnerAccountId: out.OwnerAccountId,
					CreatedDate: out.CreatedDate
				}
			: null;
	}

	async function loadRegions() {
		if (!selectedInstanceArn) {
			instanceRegions = [];
			metricsRegionCount = 0;
			return;
		}
		try {
			const out = await ssoadmin().send(new ListRegionsCommand({ InstanceArn: selectedInstanceArn }));
			instanceRegions = (out as unknown as { Regions?: Array<{ Region?: string; RegionScopeType?: string }> }).Regions ?? [];
			metricsRegionCount = instanceRegions.length;
		} catch {
			instanceRegions = [];
		}
	}

	async function loadPermissionSets() {
		if (!selectedInstanceArn) {
			permissionSetArns = [];
			permissionSetDetails = {};
			return;
		}
		const out = await ssoadmin().send(new ListPermissionSetsCommand({ InstanceArn: selectedInstanceArn }));
		permissionSetArns = out.PermissionSets ?? [];
		if (!selectedPermissionSetArn && permissionSetArns.length > 0) {
			selectedPermissionSetArn = permissionSetArns[0] ?? '';
		}
		metricsPermSetCount = permissionSetArns.length;

		const details: typeof permissionSetDetails = {};
		await Promise.all(
			permissionSetArns.map(async (arn) => {
				try {
					const d = await ssoadmin().send(
						new DescribePermissionSetCommand({ InstanceArn: selectedInstanceArn, PermissionSetArn: arn })
					);
					let hasInlinePolicy = false;
					try {
						const ip = await ssoadmin().send(
							new GetInlinePolicyForPermissionSetCommand({
								InstanceArn: selectedInstanceArn,
								PermissionSetArn: arn
							})
						);
						hasInlinePolicy = !!(ip.InlinePolicy && ip.InlinePolicy.trim().length > 2);
					} catch {
						// no inline policy
					}
					let assignmentCount = 0;
					try {
						const ar = await ssoadmin().send(
							new ListAccountAssignmentsCommand({
								InstanceArn: selectedInstanceArn,
								PermissionSetArn: arn,
								AccountId: selectedAccountID || undefined
							})
						);
						assignmentCount = ar.AccountAssignments?.length ?? 0;
					} catch {
						// ignore
					}
					details[arn] = {
						Name: d.PermissionSet?.Name,
						Description: d.PermissionSet?.Description,
						SessionDuration: d.PermissionSet?.SessionDuration,
						hasInlinePolicy,
						assignmentCount
					};
				} catch {
					details[arn] = {};
				}
			})
		);
		permissionSetDetails = details;
	}

	// Inline-policy editor for a permission set.
	let showInlineEditor = $state(false);
	let inlineEditorArn = $state('');
	let inlinePolicyText = $state('');
	let loadingInlinePolicy = $state(false);
	let savingInlinePolicy = $state(false);

	async function openInlineEditor(arn: string) {
		inlineEditorArn = arn;
		inlinePolicyText = '';
		showInlineEditor = true;
		loadingInlinePolicy = true;
		try {
			const res = await ssoadmin().send(
				new GetInlinePolicyForPermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: arn
				})
			);
			inlinePolicyText = res.InlinePolicy?.trim()
				? JSON.stringify(JSON.parse(res.InlinePolicy), null, 2)
				: JSON.stringify(
						{
							Version: '2012-10-17',
							Statement: [{ Effect: 'Allow', Action: '*', Resource: '*' }]
						},
						null,
						2
					);
		} catch (e) {
			toast.error(`Failed to load inline policy: ${e}`);
		} finally {
			loadingInlinePolicy = false;
		}
	}

	async function saveInlinePolicy() {
		if (!selectedInstanceArn || !inlineEditorArn) return;
		let parsed: unknown;
		try {
			parsed = JSON.parse(inlinePolicyText);
		} catch {
			toast.error('Inline policy must be valid JSON');
			return;
		}
		savingInlinePolicy = true;
		try {
			await ssoadmin().send(
				new PutInlinePolicyToPermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: inlineEditorArn,
					InlinePolicy: JSON.stringify(parsed)
				})
			);
			toast.success('Inline policy saved');
			showInlineEditor = false;
			await loadPermissionSets();
		} catch (e) {
			toast.error(`Failed to save inline policy: ${e}`);
		} finally {
			savingInlinePolicy = false;
		}
	}

	async function deleteInlinePolicy() {
		if (!selectedInstanceArn || !inlineEditorArn) return;
		if (!(await confirmDestructive({ title: 'Remove Inline Policy', message: 'Remove the inline policy from this permission set?' }))) return;
		savingInlinePolicy = true;
		try {
			await ssoadmin().send(
				new DeleteInlinePolicyFromPermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: inlineEditorArn
				})
			);
			toast.success('Inline policy removed');
			showInlineEditor = false;
			await loadPermissionSets();
		} catch (e) {
			toast.error(`Failed to remove inline policy: ${e}`);
		} finally {
			savingInlinePolicy = false;
		}
	}

	async function loadAssignments() {
		if (!selectedInstanceArn || !selectedPermissionSetArn) {
			accountAssignments = [];
			return;
		}
		const out = await ssoadmin().send(
			new ListAccountAssignmentsCommand({
				InstanceArn: selectedInstanceArn,
				PermissionSetArn: selectedPermissionSetArn,
				AccountId: selectedAccountID || undefined
			})
		);
		accountAssignments = out.AccountAssignments ?? [];
		metricsAssignmentCount = accountAssignments.length;
	}

	async function loadApplications() {
		if (!selectedInstanceArn) {
			applications = [];
			applicationAssignmentCounts = {};
			return;
		}
		const out = await ssoadmin().send(new ListApplicationsCommand({ InstanceArn: selectedInstanceArn }));
		applications = out.Applications ?? [];
		metricsAppCount = applications.length;

		// Load assignment counts per application.
		const counts: Record<string, number> = {};
		await Promise.all(
			applications.map(async (app) => {
				if (!app.ApplicationArn) return;
				try {
					const aa = await ssoadmin().send(
						new ListApplicationAssignmentsCommand({ ApplicationArn: app.ApplicationArn })
					);
					counts[app.ApplicationArn] = (aa.ApplicationAssignments ?? []).length;
				} catch {
					counts[app.ApplicationArn] = 0;
				}
			})
		);
		applicationAssignmentCounts = counts;
	}

	async function toggleApplicationStatus(appArn: string, currentStatus?: string) {
		const newStatus = currentStatus === 'ENABLED' ? 'DISABLED' : 'ENABLED';
		try {
			await ssoadmin().send(
				new UpdateApplicationCommand({ ApplicationArn: appArn, Status: newStatus as 'ENABLED' | 'DISABLED' })
			);
			toast.success(`Application ${newStatus === 'ENABLED' ? 'enabled' : 'disabled'}`);
			await loadApplications();
		} catch (err: unknown) {
			toast.error(`Failed to update application: ${(err as Error).message}`);
		}
	}

	async function loadTrustedTokenIssuers() {
		if (!selectedInstanceArn) {
			trustedTokenIssuers = [];
			return;
		}
		const out = await ssoadmin().send(
			new ListTrustedTokenIssuersCommand({ InstanceArn: selectedInstanceArn })
		);
		trustedTokenIssuers = (out.TrustedTokenIssuers ?? []) as typeof trustedTokenIssuers;
		metricsTTICount = trustedTokenIssuers.length;
	}

	function lookupPrincipalAssignments() {
		if (!selectedInstanceArn || !principalLookupID || !principalLookupType) {
			toast.error('Please select an instance and enter a principal ID');
			return;
		}
		principalLookupLoading = true;
		try {
			// Use the already-loaded accountAssignments and filter client-side by principal.
			principalAssignments = accountAssignments
				.filter(
					(a) => a.PrincipalId === principalLookupID && a.PrincipalType === principalLookupType
				)
				.map((a) => ({
					PrincipalId: a.PrincipalId,
					PrincipalType: a.PrincipalType,
					AccountId: a.AccountId,
					PermissionSetArn: selectedPermissionSetArn || undefined
				}));
		} catch (err: unknown) {
			toast.error(`Lookup failed: ${(err as Error).message}`);
			principalAssignments = [];
		} finally {
			principalLookupLoading = false;
		}
	}

	async function loadAccountsForPS() {
		if (!selectedInstanceArn || !selectedPermissionSetArn || !selectedAccountID) return;
		accountsForPSLoading = true;
		try {
			const out = await ssoadmin().send(
				new ListAccountAssignmentsCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: selectedPermissionSetArn,
					AccountId: selectedAccountID
				})
			);
			const unique = new Set(
				(out.AccountAssignments ?? []).map((a) => a.AccountId ?? '').filter(Boolean)
			);
			accountsForPS = [...unique].toSorted();
		} catch (err: unknown) {
			toast.error(`Failed to load accounts: ${(err as Error).message}`);
			accountsForPS = [];
		} finally {
			accountsForPSLoading = false;
		}
	}

	async function loadProvisioningStatuses() {
		if (!selectedInstanceArn) {
			creationStatuses = [];
			deletionStatuses = [];
			provisioningStatuses = [];
			return;
		}
		try {
			const [cr, dr, pr] = await Promise.all([
				ssoadmin().send(
					new ListAccountAssignmentCreationStatusCommand({ InstanceArn: selectedInstanceArn })
				),
				ssoadmin().send(
					new ListAccountAssignmentDeletionStatusCommand({ InstanceArn: selectedInstanceArn })
				),
				ssoadmin().send(
					new ListPermissionSetProvisioningStatusCommand({ InstanceArn: selectedInstanceArn })
				)
			]);
			creationStatuses = cr.AccountAssignmentsCreationStatus ?? [];
			deletionStatuses = dr.AccountAssignmentsDeletionStatus ?? [];
			provisioningStatuses = pr.PermissionSetsProvisioningStatus ?? [];
		} catch {
			// ignore errors loading status lists
		}
	}

	async function refreshAll() {
		try {
			await loadInstances();
			await Promise.all([
				describeInstance(selectedInstanceArn),
				loadPermissionSets(),
				loadApplications(),
				loadTrustedTokenIssuers(),
				loadRegions()
			]);
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to load SSO Admin data: ${(err as Error).message}`);
		}
	}

	async function seedDemoData() {
		try {
			const instResp = await ssoadmin().send(new CreateInstanceCommand({ Name: 'demo-instance' }));
			const demoInstanceArn = instResp.InstanceArn;
			if (!demoInstanceArn) return;

			await Promise.all([
				ssoadmin().send(
					new CreatePermissionSetCommand({
						InstanceArn: demoInstanceArn,
						Name: 'AdminAccess',
						Description: 'Full administrator access'
					})
				),
				ssoadmin().send(
					new CreatePermissionSetCommand({
						InstanceArn: demoInstanceArn,
						Name: 'ReadOnlyAccess',
						Description: 'Read-only access'
					})
				),
				ssoadmin().send(new AddRegionCommand({ InstanceArn: demoInstanceArn, RegionName: 'us-east-1' })),
				ssoadmin().send(new AddRegionCommand({ InstanceArn: demoInstanceArn, RegionName: 'eu-west-1' }))
			]);

			await ssoadmin().send(
				new CreateApplicationCommand({
					InstanceArn: demoInstanceArn,
					Name: 'demo-app',
					ApplicationProviderArn: defaultProviderArn
				})
			);

			await ssoadmin().send(
				new CreateTrustedTokenIssuerCommand({
					InstanceArn: demoInstanceArn,
					Name: 'demo-issuer',
					TrustedTokenIssuerType: 'OIDC_JWT',
					TrustedTokenIssuerConfiguration: {
						OidcJwtConfiguration: {
							IssuerUrl: 'https://issuer.example.com',
							ClaimAttributePath: 'sub',
							IdentityStoreAttributePath: 'UserName',
							JwksRetrievalOption: 'OPEN_ID_DISCOVERY'
						}
					}
				})
			);

			toast.success('Demo data seeded successfully');
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to seed demo data: ${(err as Error).message}`);
		}
	}

	async function createInstance() {
		if (!newInstanceName.trim()) {
			toast.error('Instance name is required');
			return;
		}
		try {
			await ssoadmin().send(new CreateInstanceCommand({ Name: newInstanceName.trim() }));
			newInstanceName = '';
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to create instance: ${(err as Error).message}`);
		}
	}

	async function removeInstance(instanceArn: string, name: string) {
		if (!(await confirmDestructive(`Delete instance "${name}"? This will remove all dependent resources.`))) return;
		try {
			await ssoadmin().send(new DeleteInstanceCommand({ InstanceArn: instanceArn }));
			if (selectedInstanceArn === instanceArn) {
				selectedInstanceArn = '';
				selectedPermissionSetArn = '';
			}
			await refreshAll();
		} catch (err: unknown) {
			toast.error(`Failed to delete instance: ${(err as Error).message}`);
		}
	}

	async function addRegion() {
		if (!selectedInstanceArn || !newRegionName.trim()) {
			toast.error('Instance and region name are required');
			return;
		}
		try {
			await ssoadmin().send(
				new AddRegionCommand({ InstanceArn: selectedInstanceArn, RegionName: newRegionName.trim() })
			);
			newRegionName = '';
			await loadRegions();
		} catch (err: unknown) {
			toast.error(`Failed to add region: ${(err as Error).message}`);
		}
	}

	async function removeRegion(regionName: string) {
		if (!selectedInstanceArn) return;
		if (!(await confirmDestructive(`Remove region "${regionName}" from this instance?`))) return;
		try {
			await ssoadmin().send(
				new RemoveRegionCommand({ InstanceArn: selectedInstanceArn, RegionName: regionName })
			);
			await loadRegions();
		} catch (err: unknown) {
			toast.error(`Failed to remove region: ${(err as Error).message}`);
		}
	}

	async function createPermissionSet() {
		if (!selectedInstanceArn || !newPermissionSetName.trim()) {
			toast.error('Instance and permission set name are required');
			return;
		}
		try {
			await ssoadmin().send(
				new CreatePermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					Name: newPermissionSetName.trim()
				})
			);
			newPermissionSetName = '';
			await loadPermissionSets();
		} catch (err: unknown) {
			toast.error(`Failed to create permission set: ${(err as Error).message}`);
		}
	}

	async function removePermissionSet(permissionSetArn: string, name: string) {
		if (!selectedInstanceArn) return;
		if (!(await confirmDestructive(`Delete permission set "${name}"?`))) return;
		try {
			await ssoadmin().send(
				new DeletePermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: permissionSetArn
				})
			);
			if (selectedPermissionSetArn === permissionSetArn) {
				selectedPermissionSetArn = '';
			}
			await loadPermissionSets();
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to delete permission set: ${(err as Error).message}`);
		}
	}

	async function provisionPermissionSet(permissionSetArn: string) {
		if (!selectedInstanceArn) return;
		try {
			await ssoadmin().send(
				new ProvisionPermissionSetCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: permissionSetArn,
					TargetType: 'ALL_PROVISIONED_ACCOUNTS'
				})
			);
			toast.success('Provisioning request created');
		} catch (err: unknown) {
			toast.error(`Failed to provision permission set: ${(err as Error).message}`);
		}
	}

	async function createAssignment() {
		if (!selectedInstanceArn || !selectedPermissionSetArn || !assignmentPrincipalID.trim()) {
			toast.error('Instance, permission set, and principal are required');
			return;
		}
		try {
			await ssoadmin().send(
				new CreateAccountAssignmentCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: selectedPermissionSetArn,
					PrincipalType: assignmentPrincipalType,
					PrincipalId: assignmentPrincipalID.trim(),
					TargetType: 'AWS_ACCOUNT',
					TargetId: selectedAccountID || defaultAccountID
				})
			);
			assignmentPrincipalID = '';
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to create assignment: ${(err as Error).message}`);
		}
	}

	async function removeAssignment(
		principalID: string,
		principalType: string,
		accountId: string
	) {
		if (!selectedInstanceArn || !selectedPermissionSetArn) return;
		if (!(await confirmDestructive(`Remove assignment for ${principalType} "${principalID}"?`))) return;
		try {
			await ssoadmin().send(
				new DeleteAccountAssignmentCommand({
					InstanceArn: selectedInstanceArn,
					PermissionSetArn: selectedPermissionSetArn,
					PrincipalType: principalType as 'USER' | 'GROUP',
					PrincipalId: principalID,
					TargetType: 'AWS_ACCOUNT',
					TargetId: accountId || defaultAccountID
				})
			);
			await loadAssignments();
		} catch (err: unknown) {
			toast.error(`Failed to delete assignment: ${(err as Error).message}`);
		}
	}

	async function createApplication() {
		if (!selectedInstanceArn || !newApplicationName.trim()) {
			toast.error('Instance and application name are required');
			return;
		}
		try {
			await ssoadmin().send(
				new CreateApplicationCommand({
					InstanceArn: selectedInstanceArn,
					Name: newApplicationName.trim(),
					ApplicationProviderArn: defaultProviderArn
				})
			);
			newApplicationName = '';
			await loadApplications();
		} catch (err: unknown) {
			toast.error(`Failed to create application: ${(err as Error).message}`);
		}
	}

	async function removeApplication(applicationArn: string, name: string) {
		if (!(await confirmDestructive(`Delete application "${name}"?`))) return;
		try {
			await ssoadmin().send(new DeleteApplicationCommand({ ApplicationArn: applicationArn }));
			await loadApplications();
		} catch (err: unknown) {
			toast.error(`Failed to delete application: ${(err as Error).message}`);
		}
	}

	async function createTrustedTokenIssuer() {
		if (!selectedInstanceArn || !newTrustedTokenIssuerName.trim()) {
			toast.error('Instance and issuer name are required');
			return;
		}
		try {
			await ssoadmin().send(
				new CreateTrustedTokenIssuerCommand({
					InstanceArn: selectedInstanceArn,
					Name: newTrustedTokenIssuerName.trim(),
					TrustedTokenIssuerType: 'OIDC_JWT',
					TrustedTokenIssuerConfiguration: {
						OidcJwtConfiguration: {
							IssuerUrl: 'https://issuer.example.com',
							ClaimAttributePath: 'sub',
							IdentityStoreAttributePath: 'UserName',
							JwksRetrievalOption: 'OPEN_ID_DISCOVERY'
						}
					}
				})
			);
			newTrustedTokenIssuerName = '';
			await loadTrustedTokenIssuers();
		} catch (err: unknown) {
			toast.error(`Failed to create trusted token issuer: ${(err as Error).message}`);
		}
	}

	async function removeTrustedTokenIssuer(
		trustedTokenIssuerArn: string,
		name: string
	) {
		if (!(await confirmDestructive(`Delete trusted token issuer "${name}"?`))) return;
		try {
			await ssoadmin().send(
				new DeleteTrustedTokenIssuerCommand({ TrustedTokenIssuerArn: trustedTokenIssuerArn })
			);
			await loadTrustedTokenIssuers();
		} catch (err: unknown) {
			toast.error(`Failed to delete trusted token issuer: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		if (selectedInstanceArn) {
			void describeInstance(selectedInstanceArn);
			void loadPermissionSets();
			void loadApplications();
			void loadTrustedTokenIssuers();
			void loadRegions();
		}
	});

	$effect(() => {
		if (selectedInstanceArn && selectedPermissionSetArn) {
			void loadAssignments();
		}
	});

	$effect(() => {
		if (activeTab === 'provisioning') {
			void loadProvisioningStatuses();
		}
	});

	onMount(() => {
		void refreshAll();
	});
</script>

<div class="space-y-6 p-6">
	<div
		class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
	>
		<div class="flex items-start justify-between">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SSO Admin</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
					Manage IAM Identity Center instances, permission sets, account assignments, applications,
					and trusted token issuers.
				</p>
			</div>
			<button
				type="button"
				onclick={() => void seedDemoData()}
				class="rounded-lg border border-indigo-300 px-4 py-2 text-sm text-indigo-600 hover:bg-indigo-50 dark:border-indigo-500 dark:text-indigo-400 dark:hover:bg-indigo-900/30"
			>
				Seed Demo Data
			</button>
		</div>
	</div>

	<div class="flex flex-wrap gap-2">
		{#each [
			['instances', 'Instances'],
			['permissionsets', 'Permission Sets'],
			['assignments', 'Assignments'],
			['applications', 'Applications'],
			['tokenissuers', 'Token Issuers'],
			['provisioning', 'Provisioning'],
			['metrics', 'Metrics'],
			['docs', 'Docs']
		] as [tabId, label] (tabId)}
			<button
				type="button"
				onclick={() => (activeTab = tabId as TabId)}
				class={`rounded-lg border px-4 py-2 text-sm ${activeTab === tabId ? 'border-indigo-600 bg-indigo-600 text-white' : 'border-slate-300 dark:border-slate-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<div
		class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
	>
		<label class="text-sm text-slate-600 dark:text-slate-300">
			Instance
			<select
				bind:value={selectedInstanceArn}
				class="mt-1 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
			>
				<option value="">Select instance</option>
				{#each instances as instance}
					<option value={instance.InstanceArn}
						>{instance.Name} ({instance.InstanceArn})</option
					>
				{/each}
			</select>
		</label>
		{#if instanceDetails}
			<p class="text-xs text-slate-500 dark:text-slate-400">
				Identity Store: {instanceDetails.IdentityStoreId} &middot; Owner: {instanceDetails.OwnerAccountId}
				{#if instanceDetails.CreatedDate}
					&middot; Created: {formatDate(instanceDetails.CreatedDate)}
				{/if}
			</p>
		{/if}
	</div>

	{#if activeTab === 'instances'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
					Instances
					<span
						class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{instances.length}</span
					>
				</h2>
			</div>
			<div class="flex gap-2">
				<input
					bind:value={newInstanceName}
					placeholder="Instance name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button
					type="button"
					class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
					onclick={createInstance}
				>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each instances as instance}
					<li class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div class="flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<p class="font-medium">{instance.Name}</p>
								<div class="mt-0.5 flex items-center gap-1">
									<p class="truncate font-mono text-xs text-slate-500">{instance.InstanceArn}</p>
									<button
										type="button"
										class="shrink-0 text-xs text-slate-400 hover:text-slate-600"
										onclick={() => void copyToClipboard(instance.InstanceArn ?? '')}
										title="Copy ARN"
									>
										&#9112;
									</button>
								</div>
								<p class="text-xs text-slate-400">Identity Store: {instance.IdentityStoreId}</p>
								{#if instance.CreatedDate}
									<p class="text-xs text-slate-400">Created: {formatDate(instance.CreatedDate)}</p>
								{/if}
							</div>
							<button
								type="button"
								class="ml-3 shrink-0 rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
								onclick={() => void removeInstance(instance.InstanceArn ?? '', instance.Name ?? '')}
							>
								Delete
							</button>
						</div>
					</li>
				{/each}
			</ul>
		</div>

		{#if selectedInstanceArn}
			<div
				class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
			>
				<div class="flex items-center justify-between">
					<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
						Regions
						<span
							class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
							>{instanceRegions.length}</span
						>
					</h2>
					<button
						type="button"
						class="text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
						onclick={() => void loadRegions()}
					>
						&#8635; Refresh
					</button>
				</div>
				<div class="flex gap-2">
					<input
						bind:value={newRegionName}
						placeholder="e.g. us-west-2"
						class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					/>
					<button
						type="button"
						class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
						onclick={addRegion}
					>
						Add Region
					</button>
				</div>
				{#if instanceRegions.length === 0}
					<p class="text-sm italic text-slate-400">No regions configured for this instance.</p>
				{:else}
					<ul class="space-y-1 text-sm">
						{#each instanceRegions as region}
							<li
								class="flex items-center justify-between rounded-lg border border-slate-200 px-3 py-2 dark:border-slate-700"
							>
								<div>
									<span class="font-mono font-medium">{region.Region}</span>
									<span class="ml-2 text-xs text-slate-400">{region.RegionScopeType}</span>
								</div>
								<button
									type="button"
									class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
									onclick={() => void removeRegion(region.Region ?? '')}
								>
									Remove
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</div>
		{/if}
	{/if}

	{#if activeTab === 'permissionsets'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
					Permission Sets
					<span
						class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{permissionSetArns.length}</span
					>
				</h2>
			</div>
			<div class="flex gap-2">
				<input
					bind:value={newPermissionSetName}
					placeholder="Permission set name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button
					type="button"
					class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
					onclick={createPermissionSet}
				>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each permissionSetArns as permissionSetArn}
					<li class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div class="flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-center gap-2">
									<p class="font-medium">
										{permissionSetDetails[permissionSetArn]?.Name ?? '\u2014'}
									</p>
									{#if permissionSetDetails[permissionSetArn]?.hasInlinePolicy}
										<span
											class="rounded bg-amber-100 px-1.5 py-0.5 text-xs text-amber-700 dark:bg-amber-900/30 dark:text-amber-400"
										>
											Inline &#10003;
										</span>
									{/if}
									{#if (permissionSetDetails[permissionSetArn]?.assignmentCount ?? 0) > 0}
										<span
											class="rounded bg-blue-100 px-1.5 py-0.5 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-400"
										>
											{permissionSetDetails[permissionSetArn]?.assignmentCount} assignment{(permissionSetDetails[permissionSetArn]?.assignmentCount ?? 0) !== 1
												? 's'
												: ''}
										</span>
									{/if}
								</div>
								<div class="mt-0.5 flex items-center gap-1">
									<p class="truncate font-mono text-xs text-slate-500">{permissionSetArn}</p>
									<button
										type="button"
										class="shrink-0 text-xs text-slate-400 hover:text-slate-600"
										onclick={() => void copyToClipboard(permissionSetArn)}
										title="Copy ARN"
									>
										&#9112;
									</button>
								</div>
								{#if permissionSetDetails[permissionSetArn]?.Description}
									<p class="text-xs text-slate-400">
										{permissionSetDetails[permissionSetArn].Description}
									</p>
								{/if}
								<p class="text-xs text-slate-400">
									Session: {permissionSetDetails[permissionSetArn]?.SessionDuration ?? 'PT1H'}
								</p>
							</div>
							<div class="ml-3 flex shrink-0 gap-2">
								<button
									type="button"
									class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
									onclick={() => void openInlineEditor(permissionSetArn)}
								>
									Inline Policy
								</button>
								<button
									type="button"
									class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
									onclick={() => void provisionPermissionSet(permissionSetArn)}
								>
									Provision
								</button>
								<button
									type="button"
									class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
									onclick={() =>
										void removePermissionSet(
											permissionSetArn,
											permissionSetDetails[permissionSetArn]?.Name ?? permissionSetArn
										)}
								>
									Delete
								</button>
							</div>
						</div>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'assignments'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
					Account Assignments
					<span
						class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{accountAssignments.length}</span
					>
				</h2>
			</div>
			<div class="grid gap-2 md:grid-cols-2 lg:grid-cols-4">
				<div>
					<label class="text-xs text-slate-500">Permission Set</label>
					<select
						bind:value={selectedPermissionSetArn}
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					>
						<option value="">Select permission set</option>
						{#each permissionSetArns as permissionSetArn}
							<option value={permissionSetArn}
								>{permissionSetDetails[permissionSetArn]?.Name ?? permissionSetArn}</option
							>
						{/each}
					</select>
				</div>
				<div>
					<label class="text-xs text-slate-500">Account ID</label>
					<input
						bind:value={selectedAccountID}
						placeholder="123456789012"
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					/>
				</div>
				<div>
					<label class="text-xs text-slate-500">Principal Type</label>
					<select
						bind:value={assignmentPrincipalType}
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					>
						<option value="USER">USER</option>
						<option value="GROUP">GROUP</option>
					</select>
				</div>
				<div>
					<label class="text-xs text-slate-500">Principal ID</label>
					<input
						bind:value={assignmentPrincipalID}
						placeholder="Principal ID"
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					/>
				</div>
			</div>
			<button
				type="button"
				class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
				onclick={createAssignment}
			>
				Create Assignment
			</button>
			<ul class="space-y-2 text-sm">
				{#each accountAssignments as assignment}
					<li
						class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700"
					>
						<div>
							<div class="flex items-center gap-2">
								<span
									class={`rounded px-1.5 py-0.5 text-xs ${assignment.PrincipalType === 'USER' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400' : 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400'}`}
								>
									{assignment.PrincipalType}
								</span>
								<p class="font-mono font-medium">{assignment.PrincipalId}</p>
							</div>
							<p class="mt-0.5 text-xs text-slate-500">Account: {assignment.AccountId}</p>
						</div>
						<button
							type="button"
							class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
							onclick={() =>
								void removeAssignment(
									assignment.PrincipalId ?? '',
									assignment.PrincipalType ?? 'USER',
									assignment.AccountId ?? defaultAccountID
								)}
						>
							Delete
						</button>
					</li>
				{/each}
			</ul>

			<!-- Principal Lookup Section -->
			<div class="border-t border-slate-200 pt-4 dark:border-slate-700">
				<h3 class="mb-3 text-sm font-semibold text-slate-700 dark:text-slate-200">
					Principal Lookup
				</h3>
				<div class="flex gap-2">
					<input
						bind:value={principalLookupID}
						placeholder="Principal ID (e.g. user-123)"
						class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					/>
					<select
						bind:value={principalLookupType}
						class="rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
					>
						<option value="USER">USER</option>
						<option value="GROUP">GROUP</option>
					</select>
					<button
						type="button"
						class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50"
						onclick={() => lookupPrincipalAssignments()}
						disabled={principalLookupLoading}
					>
						{principalLookupLoading ? 'Loading…' : 'Lookup'}
					</button>
				</div>
				{#if principalAssignments.length > 0}
					<ul class="mt-3 space-y-1 text-xs">
						{#each principalAssignments as pa}
							<li class="flex items-center gap-3 rounded border border-slate-100 px-3 py-2 dark:border-slate-700">
								<span class="font-mono text-slate-600 dark:text-slate-300">{pa.AccountId}</span>
								<span class="truncate font-mono text-slate-400 text-xs">{pa.PermissionSetArn}</span>
							</li>
						{/each}
					</ul>
				{:else if principalLookupID}
					<p class="mt-2 text-xs italic text-slate-400">No assignments found for this principal.</p>
				{/if}
			</div>

			<!-- Accounts for Permission Set -->
			{#if selectedPermissionSetArn}
				<div class="border-t border-slate-200 pt-4 dark:border-slate-700">
					<div class="mb-2 flex items-center justify-between">
						<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">
							Accounts Using Selected Permission Set
						</h3>
						<button
							type="button"
							class="text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400"
							onclick={() => void loadAccountsForPS()}
							disabled={accountsForPSLoading}
						>
							{accountsForPSLoading ? 'Loading…' : '&#8635; Load'}
						</button>
					</div>
					{#if accountsForPS.length > 0}
						<div class="flex flex-wrap gap-2">
							{#each accountsForPS as accountID}
								<span class="rounded bg-slate-100 px-2 py-1 font-mono text-xs dark:bg-slate-700">{accountID}</span>
							{/each}
						</div>
					{:else}
						<p class="text-xs italic text-slate-400">No accounts loaded yet. Click Load.</p>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	{#if activeTab === 'applications'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
					Applications
					<span
						class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{applications.length}</span
					>
				</h2>
				<button
					type="button"
					class="text-xs text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
					onclick={() => void loadApplications()}
				>
					&#8635; Refresh
				</button>
			</div>
			<div class="flex gap-2">
				<input
					bind:value={newApplicationName}
					placeholder="Application name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button
					type="button"
					class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
					onclick={createApplication}
				>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each applications as application}
					<li class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div class="flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-center gap-2">
									<p class="font-medium">{application.Name}</p>
									<span
										class={`rounded px-1.5 py-0.5 text-xs ${application.Status === 'ENABLED' ? 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-400' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}`}
									>
										{application.Status}
									</span>
									{#if (applicationAssignmentCounts[application.ApplicationArn ?? ''] ?? 0) > 0}
										<span class="rounded bg-blue-100 px-1.5 py-0.5 text-xs text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">
											{applicationAssignmentCounts[application.ApplicationArn ?? '']} assigned
										</span>
									{/if}
								</div>
								{#if application.Description}
									<p class="mt-0.5 text-xs text-slate-500">{application.Description}</p>
								{/if}
								<div class="mt-0.5 flex items-center gap-1">
									<p class="truncate font-mono text-xs text-slate-500">
										{application.ApplicationArn}
									</p>
									<button
										type="button"
										class="shrink-0 text-xs text-slate-400 hover:text-slate-600"
										onclick={() => void copyToClipboard(application.ApplicationArn ?? '')}
										title="Copy ARN"
									>
										&#9112;
									</button>
								</div>
								{#if application.ApplicationProviderArn}
									<p class="mt-0.5 text-xs text-slate-400">
										Provider: <span class="font-mono">{application.ApplicationProviderArn}</span>
									</p>
								{/if}
							</div>
							<div class="ml-3 flex shrink-0 flex-col gap-1">
								<button
									type="button"
									class={`rounded border px-2 py-1 text-xs ${application.Status === 'ENABLED' ? 'border-amber-300 text-amber-700 hover:bg-amber-50 dark:border-amber-700 dark:text-amber-400' : 'border-green-300 text-green-700 hover:bg-green-50 dark:border-green-700 dark:text-green-400'}`}
									onclick={() => void toggleApplicationStatus(application.ApplicationArn ?? '', application.Status)}
								>
									{application.Status === 'ENABLED' ? 'Disable' : 'Enable'}
								</button>
								<button
									type="button"
									class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
									onclick={() =>
										void removeApplication(application.ApplicationArn ?? '', application.Name ?? '')}
								>
									Delete
								</button>
							</div>
						</div>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'tokenissuers'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">
					Trusted Token Issuers
					<span
						class="ml-2 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{trustedTokenIssuers.length}</span
					>
				</h2>
			</div>
			<div class="flex gap-2">
				<input
					bind:value={newTrustedTokenIssuerName}
					placeholder="Trusted token issuer name"
					class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-700 dark:bg-slate-900"
				/>
				<button
					type="button"
					class="rounded-lg bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700"
					onclick={createTrustedTokenIssuer}
				>
					Create
				</button>
			</div>
			<ul class="space-y-2 text-sm">
				{#each trustedTokenIssuers as tti}
					<li class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div class="flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex flex-wrap items-center gap-2">
									<p class="font-medium">{tti.Name}</p>
									<span
										class="rounded bg-orange-100 px-1.5 py-0.5 text-xs text-orange-700 dark:bg-orange-900/30 dark:text-orange-400"
									>
										{tti.TrustedTokenIssuerType}
									</span>
								</div>
								{#if tti.TrustedTokenIssuerConfiguration?.OidcJwtConfiguration?.IssuerUrl}
									<p class="mt-0.5 text-xs text-slate-500">
										Issuer: <span class="font-mono">{tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration.IssuerUrl}</span>
									</p>
								{/if}
								{#if tti.TrustedTokenIssuerConfiguration?.OidcJwtConfiguration?.ClaimAttributePath}
									<p class="mt-0.5 text-xs text-slate-400">
										Claim: <span class="font-mono">{tti.TrustedTokenIssuerConfiguration.OidcJwtConfiguration.ClaimAttributePath}</span>
									</p>
								{/if}
								<div class="mt-0.5 flex items-center gap-1">
									<p class="truncate font-mono text-xs text-slate-500">
										{tti.TrustedTokenIssuerArn}
									</p>
									<button
										type="button"
										class="shrink-0 text-xs text-slate-400 hover:text-slate-600"
										onclick={() => void copyToClipboard(tti.TrustedTokenIssuerArn ?? '')}
										title="Copy ARN"
									>
										&#9112;
									</button>
								</div>
							</div>
							<button
								type="button"
								class="ml-3 shrink-0 rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
								onclick={() =>
									void removeTrustedTokenIssuer(
										tti.TrustedTokenIssuerArn ?? '',
										tti.Name ?? ''
									)}
							>
								Delete
							</button>
						</div>
					</li>
				{/each}
			</ul>
		</div>
	{/if}

	{#if activeTab === 'provisioning'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-6"
		>
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Provisioning Status</h2>
				<button
					type="button"
					class="rounded-lg border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
					onclick={() => void loadProvisioningStatuses()}
				>
					&#8635; Refresh
				</button>
			</div>

			<div>
				<h3 class="mb-2 text-sm font-semibold text-slate-600 dark:text-slate-300">
					Assignment Creation
					<span class="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{creationStatuses.length}</span
					>
				</h3>
				{#if creationStatuses.length === 0}
					<p class="text-sm italic text-slate-400">No creation statuses.</p>
				{:else}
					<ul class="space-y-1 text-xs">
						{#each creationStatuses as s}
							<li
								class="flex items-center gap-3 rounded border border-slate-100 px-3 py-2 dark:border-slate-700"
							>
								<span
									class={`rounded px-1.5 py-0.5 font-medium ${s.Status === 'SUCCEEDED' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : s.Status === 'FAILED' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}`}
								>
									{s.Status}
								</span>
								<span class="font-mono text-slate-500">{s.RequestId}</span>
								{#if s.CreatedDate}<span class="text-slate-400">{formatDate(s.CreatedDate)}</span>{/if}
							</li>
						{/each}
					</ul>
				{/if}
			</div>

			<div>
				<h3 class="mb-2 text-sm font-semibold text-slate-600 dark:text-slate-300">
					Assignment Deletion
					<span class="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{deletionStatuses.length}</span
					>
				</h3>
				{#if deletionStatuses.length === 0}
					<p class="text-sm italic text-slate-400">No deletion statuses.</p>
				{:else}
					<ul class="space-y-1 text-xs">
						{#each deletionStatuses as s}
							<li
								class="flex items-center gap-3 rounded border border-slate-100 px-3 py-2 dark:border-slate-700"
							>
								<span
									class={`rounded px-1.5 py-0.5 font-medium ${s.Status === 'SUCCEEDED' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : s.Status === 'FAILED' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}`}
								>
									{s.Status}
								</span>
								<span class="font-mono text-slate-500">{s.RequestId}</span>
								{#if s.CreatedDate}<span class="text-slate-400">{formatDate(s.CreatedDate)}</span>{/if}
							</li>
						{/each}
					</ul>
				{/if}
			</div>

			<div>
				<h3 class="mb-2 text-sm font-semibold text-slate-600 dark:text-slate-300">
					Permission Set Provisioning
					<span class="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700"
						>{provisioningStatuses.length}</span
					>
				</h3>
				{#if provisioningStatuses.length === 0}
					<p class="text-sm italic text-slate-400">No provisioning statuses.</p>
				{:else}
					<ul class="space-y-1 text-xs">
						{#each provisioningStatuses as s}
							<li
								class="flex items-center gap-3 rounded border border-slate-100 px-3 py-2 dark:border-slate-700"
							>
								<span
									class={`rounded px-1.5 py-0.5 font-medium ${s.Status === 'SUCCEEDED' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : s.Status === 'FAILED' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}`}
								>
									{s.Status}
								</span>
								<span class="font-mono text-slate-500">{s.RequestId}</span>
								{#if s.CreatedDate}<span class="text-slate-400">{formatDate(s.CreatedDate)}</span>{/if}
							</li>
						{/each}
					</ul>
				{/if}
			</div>
		</div>
	{/if}

	{#if activeTab === 'metrics'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-4"
		>
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">Metrics</h2>
			<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-9">
				{#each [
					['Instances', metricsInstanceCount, 'bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'],
					['Permission Sets', metricsPermSetCount, 'bg-purple-50 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300'],
					['Applications', metricsAppCount, 'bg-green-50 text-green-700 dark:bg-green-900/30 dark:text-green-300'],
					['Token Issuers', metricsTTICount, 'bg-orange-50 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300'],
					['Assignments', metricsAssignmentCount, 'bg-rose-50 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300'],
					['Regions', metricsRegionCount, 'bg-teal-50 text-teal-700 dark:bg-teal-900/30 dark:text-teal-300'],
					['Provisioning Requests', provisioningStatuses.length, 'bg-indigo-50 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300'],
					['Creation Requests', creationStatuses.length, 'bg-sky-50 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300'],
					['Deletion Requests', deletionStatuses.length, 'bg-amber-50 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300']
				] as [label, count, cls] (label)}
					<div class={`rounded-xl p-4 ${cls}`}>
						<p class="text-sm font-medium">{label}</p>
						<p class="mt-1 text-3xl font-bold">{count}</p>
					</div>
				{/each}
			</div>
			<button
				type="button"
				class="rounded-lg border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
				onclick={() => void refreshAll()}
			>
				Refresh Metrics
			</button>
		</div>
	{/if}

	{#if activeTab === 'docs'}
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-6"
		>
			<h2 class="text-lg font-semibold text-slate-800 dark:text-slate-100">API Documentation</h2>
			<p class="text-sm text-slate-600 dark:text-slate-300">
				This service emulates AWS IAM Identity Center (SSO Admin). The following operations are
				supported:
			</p>

			{#each [
				{
					category: 'Instances',
					ops: [
						{ name: 'CreateInstance', desc: 'Create a new SSO instance.' },
						{ name: 'DescribeInstance', desc: 'Retrieve details (including CreatedDate) for a specific SSO instance.' },
						{ name: 'ListInstances', desc: 'List all SSO instances.' },
						{ name: 'DeleteInstance', desc: 'Delete an SSO instance and all its dependent resources.' },
						{ name: 'UpdateInstance', desc: 'Update the name of an SSO instance.' },
						{ name: 'AddRegion', desc: 'Add a region (RegionScopeType=ALL_REGIONS) to an SSO instance. Idempotent.' },
						{ name: 'RemoveRegion', desc: 'Remove a region from an SSO instance.' },
						{ name: 'ListRegions', desc: 'List regions with RegionScopeType associated with an SSO instance.' }
					]
				},
				{
					category: 'Permission Sets',
					ops: [
						{ name: 'CreatePermissionSet', desc: 'Create a permission set within an instance. Name must be ≤32 chars.' },
						{ name: 'DescribePermissionSet', desc: 'Get details of a permission set.' },
						{ name: 'UpdatePermissionSet', desc: 'Update a permission set description, session duration, or relay state.' },
						{ name: 'DeletePermissionSet', desc: 'Delete a permission set. Returns ConflictException if there are active account assignments.' },
						{ name: 'ListPermissionSets', desc: 'List all permission sets for an instance.' },
						{ name: 'ListPermissionSetsProvisionedToAccount', desc: 'List permission sets provisioned to a specific AWS account (sorted).' },
						{ name: 'ProvisionPermissionSet', desc: 'Provision a permission set to accounts.' },
						{ name: 'AttachManagedPolicyToPermissionSet', desc: 'Attach an AWS managed policy.' },
						{ name: 'DetachManagedPolicyFromPermissionSet', desc: 'Detach an AWS managed policy.' },
						{ name: 'ListManagedPoliciesInPermissionSet', desc: 'List AWS managed policies.' },
						{ name: 'AttachCustomerManagedPolicyReferenceToPermissionSet', desc: 'Attach a customer-managed policy reference.' },
						{ name: 'DetachCustomerManagedPolicyReferenceFromPermissionSet', desc: 'Detach a customer-managed policy reference.' },
						{ name: 'ListCustomerManagedPolicyReferencesInPermissionSet', desc: 'List customer-managed policy references.' },
						{ name: 'PutInlinePolicyToPermissionSet', desc: 'Set an inline policy on a permission set.' },
						{ name: 'GetInlinePolicyForPermissionSet', desc: 'Get the inline policy of a permission set.' },
						{ name: 'DeleteInlinePolicyFromPermissionSet', desc: 'Remove the inline policy from a permission set.' },
						{ name: 'PutPermissionsBoundaryToPermissionSet', desc: 'Set a permissions boundary on a permission set.' },
						{ name: 'GetPermissionsBoundaryForPermissionSet', desc: 'Get the permissions boundary of a permission set.' },
						{ name: 'DeletePermissionsBoundaryFromPermissionSet', desc: 'Remove the permissions boundary from a permission set.' }
					]
				},
				{
					category: 'Account Assignments',
					ops: [
						{ name: 'CreateAccountAssignment', desc: 'Assign a permission set to a principal in an account. Validates PrincipalType (USER/GROUP) and TargetType (AWS_ACCOUNT). Deterministically idempotent.' },
						{ name: 'DeleteAccountAssignment', desc: 'Remove an account assignment.' },
						{ name: 'ListAccountAssignments', desc: 'List account assignments for a permission set, optionally filtered by account ID.' },
						{ name: 'ListAccountAssignmentsForPrincipal', desc: 'List all account assignments for a specific principal (user or group), sorted.' },
						{ name: 'ListAccountsForProvisionedPermissionSet', desc: 'List all account IDs that have been assigned the given permission set.' },
						{ name: 'DescribeAccountAssignmentCreationStatus', desc: 'Get the status of an assignment creation request.' },
						{ name: 'DescribeAccountAssignmentDeletionStatus', desc: 'Get the status of an assignment deletion request.' },
						{ name: 'ListAccountAssignmentCreationStatus', desc: 'List account assignment creation statuses (sorted by date desc).' },
						{ name: 'ListAccountAssignmentDeletionStatus', desc: 'List account assignment deletion statuses (sorted by date desc).' }
					]
				},
				{
					category: 'Applications',
					ops: [
						{ name: 'CreateApplication', desc: 'Create an application within an instance. Supports Tags.' },
						{ name: 'DescribeApplication', desc: 'Get details of an application, including Tags.' },
						{ name: 'UpdateApplication', desc: 'Update an application name, description, or status (ENABLED/DISABLED).' },
						{ name: 'DeleteApplication', desc: 'Delete an application.' },
						{ name: 'ListApplications', desc: 'List applications for an instance (sorted by ARN).' },
						{ name: 'CreateApplicationAssignment', desc: 'Assign a principal to an application.' },
						{ name: 'DeleteApplicationAssignment', desc: 'Remove an application assignment.' },
						{ name: 'DescribeApplicationAssignment', desc: 'Describe a specific application assignment.' },
						{ name: 'ListApplicationAssignments', desc: 'List assignments for an application (sorted).' },
						{ name: 'ListApplicationAssignmentsForPrincipal', desc: 'List all application assignments for a specific principal across all applications in an instance.' },
						{ name: 'PutApplicationAccessScope', desc: 'Add an access scope to an application.' },
						{ name: 'DeleteApplicationAccessScope', desc: 'Remove an access scope.' },
						{ name: 'GetApplicationAccessScope', desc: 'Get a specific access scope for an application. Returns ResourceNotFoundException if not found.' },
						{ name: 'ListApplicationAccessScopes', desc: 'List access scopes for an application.' },
						{ name: 'PutApplicationAuthenticationMethod', desc: 'Add an authentication method.' },
						{ name: 'DeleteApplicationAuthenticationMethod', desc: 'Remove an authentication method.' },
						{ name: 'GetApplicationAuthenticationMethod', desc: 'Get a specific authentication method for an application.' },
						{ name: 'ListApplicationAuthenticationMethods', desc: 'List authentication methods.' },
						{ name: 'PutApplicationGrant', desc: 'Add a grant type to an application.' },
						{ name: 'DeleteApplicationGrant', desc: 'Remove a grant type.' },
						{ name: 'GetApplicationGrant', desc: 'Get a specific grant type for an application. Returns ResourceNotFoundException if not found.' },
						{ name: 'ListApplicationGrants', desc: 'List grant types for an application.' },
						{ name: 'PutApplicationAssignmentConfiguration', desc: 'Configure whether assignments are required.' },
						{ name: 'GetApplicationAssignmentConfiguration', desc: 'Get assignment configuration for an application.' },
						{ name: 'PutApplicationSessionConfiguration', desc: 'Set the session duration for an application.' },
						{ name: 'GetApplicationSessionConfiguration', desc: 'Get the session configuration for an application.' },
						{ name: 'ListApplicationProviders', desc: 'List application providers. DisplayData is a struct with DisplayName, Description, IconUrl.' },
						{ name: 'DescribeApplicationProvider', desc: 'Describe an application provider by ARN.' }
					]
				},
				{
					category: 'Trusted Token Issuers',
					ops: [
						{ name: 'CreateTrustedTokenIssuer', desc: 'Create a trusted token issuer. Accepts Tags and TrustedTokenIssuerConfiguration (OIDC JWT with IssuerUrl, ClaimAttributePath, IdentityStoreAttributePath, JwksRetrievalOption).' },
						{ name: 'DescribeTrustedTokenIssuer', desc: 'Get details including TrustedTokenIssuerConfiguration and Tags.' },
						{ name: 'UpdateTrustedTokenIssuer', desc: 'Update a trusted token issuer name, type, or TrustedTokenIssuerConfiguration.' },
						{ name: 'DeleteTrustedTokenIssuer', desc: 'Delete a trusted token issuer.' },
						{ name: 'ListTrustedTokenIssuers', desc: 'List trusted token issuers for an instance (sorted by ARN).' }
					]
				},
				{
					category: 'ABAC / Access Control Attributes',
					ops: [
						{ name: 'CreateInstanceAccessControlAttributeConfiguration', desc: 'Create ABAC configuration. Returns ConflictException if already exists.' },
						{ name: 'DescribeInstanceAccessControlAttributeConfiguration', desc: 'Get ABAC configuration.' },
						{ name: 'UpdateInstanceAccessControlAttributeConfiguration', desc: 'Update ABAC configuration.' },
						{ name: 'DeleteInstanceAccessControlAttributeConfiguration', desc: 'Delete ABAC configuration.' }
					]
				},
				{
					category: 'Tags',
					ops: [
						{ name: 'TagResource', desc: 'Add tags (max 50, key \u2264 128 chars, value \u2264 256 chars) to a resource.' },
						{ name: 'UntagResource', desc: 'Remove tags from a resource.' },
						{ name: 'ListTagsForResource', desc: 'List all tags on a resource (sorted by key).' }
					]
				}
			] as section}
				<div>
					<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-200">
						{section.category}
					</h3>
					<ul class="space-y-1">
						{#each section.ops as op}
							<li class="flex gap-3 text-sm">
								<span class="w-80 shrink-0 font-mono text-indigo-600 dark:text-indigo-400"
									>{op.name}</span
								>
								<span class="text-slate-600 dark:text-slate-300">{op.desc}</span>
							</li>
						{/each}
					</ul>
				</div>
			{/each}
		</div>
	{/if}
</div>

{#if showInlineEditor}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-2xl rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
			<div class="mb-3 flex items-center justify-between">
				<h2 class="text-lg font-semibold">Inline Policy</h2>
				<button class="text-sm text-slate-400 hover:text-slate-600" onclick={() => (showInlineEditor = false)}>Close</button>
			</div>
			<p class="mb-3 truncate font-mono text-xs text-slate-500">{inlineEditorArn}</p>
			{#if loadingInlinePolicy}
				<div class="flex justify-center py-8 text-sm text-slate-400">Loading…</div>
			{:else}
				<textarea
					bind:value={inlinePolicyText}
					rows={16}
					class="w-full rounded-md border border-slate-300 bg-white px-3 py-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-900"
				></textarea>
			{/if}
			<div class="mt-4 flex justify-end gap-2">
				<button
					type="button"
					class="rounded border border-red-300 px-4 py-2 text-sm text-red-600 hover:bg-red-50 disabled:opacity-50 dark:border-red-700 dark:text-red-400"
					onclick={deleteInlinePolicy}
					disabled={savingInlinePolicy}
				>
					Remove
				</button>
				<button
					type="button"
					class="rounded bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50"
					onclick={saveInlinePolicy}
					disabled={savingInlinePolicy || loadingInlinePolicy}
				>
					{savingInlinePolicy ? 'Saving…' : 'Save Policy'}
				</button>
			</div>
		</div>
	</div>
{/if}
