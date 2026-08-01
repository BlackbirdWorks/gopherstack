<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTransferClient } from '$lib/aws-client';
	import {
		ListServersCommand,
		DescribeServerCommand,
		CreateServerCommand,
		DeleteServerCommand,
		StartServerCommand,
		StopServerCommand,
		ListUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		ListAccessesCommand,
		CreateAccessCommand,
		DeleteAccessCommand,
		ListAgreementsCommand,
		CreateAgreementCommand,
		DeleteAgreementCommand,
		ListConnectorsCommand,
		CreateConnectorCommand,
		DeleteConnectorCommand,
		ListProfilesCommand,
		CreateProfileCommand,
		DeleteProfileCommand,
		ListWebAppsCommand,
		CreateWebAppCommand,
		DeleteWebAppCommand,
		ListWorkflowsCommand,
		CreateWorkflowCommand,
		DeleteWorkflowCommand,
		ListCertificatesCommand,
		ImportCertificateCommand,
		DeleteCertificateCommand,
		ListHostKeysCommand,
		ImportHostKeyCommand,
		DeleteHostKeyCommand,
		ImportSshPublicKeyCommand,
		DeleteSshPublicKeyCommand,
		DescribeUserCommand,
		type DescribedServer,
		type DescribedUser,
		type ListedServer,
		type ListedUser,
		type ListedAccess,
		type ListedAgreement,
		type ListedConnector,
		type ListedProfile,
		type ListedWorkflow,
		type ListedCertificate
	} from '@aws-sdk/client-transfer';
	import { toast } from 'svelte-sonner';
	import {
		ArrowLeftRight,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Play,
		Square,
		Users,
		Server,
		Link,
		FileKey,
		Globe,
		GitBranch,
		ShieldCheck
	} from 'lucide-svelte';

	type TabName = 'servers' | 'access' | 'agreements' | 'connectors' | 'profiles' | 'webapps' | 'workflows' | 'certificates';

	const transfer = regionalClient(getTransferClient);

	let activeTab = $state<TabName>('servers');
	let searchQuery = $state('');

	// Per-tab loading spinners
	// generic loading alias referenced in template
	let loading = $state(false);
	let loadingServers = $state(false);
	let loadingConnectors = $state(false);
	let loadingProfiles = $state(false);
	let loadingWebApps = $state(false);
	let loadingWorkflows = $state(false);
	let loadingCertificates = $state(false);

	// Servers
	let servers = $state<ListedServer[]>([]);
	let selectedServer = $state<ListedServer | null>(null);
	let serverDetail = $state<DescribedServer | null>(null);
	let serverUsers = $state<ListedUser[]>([]);
	let loadingUsers = $state(false);
	let serverHostKeys = $state<{ HostKeyId?: string; Type?: string; Description?: string }[]>([]);
	let loadingHostKeys = $state(false);
	let showImportHostKeyModal = $state(false);
	let importingHostKey = $state(false);
	let newHostKeyBody = $state('');
	let newHostKeyDescription = $state('');

	// User detail
	let selectedUser = $state<ListedUser | null>(null);
	let userDetail = $state<DescribedUser | null>(null);
	let loadingUserDetail = $state(false);

	// SSH Public Keys in user panel
	let showImportSshKeyModal = $state(false);
	let importingSshKey = $state(false);
	let sshKeyTargetUser = $state('');
	let newSshKeyBody = $state('');
	let userSshKeys = $state<{ SshPublicKeyId?: string; Fingerprint?: string; DateImported?: string; UserName?: string }[]>([]);
	let loadingSshKeys = $state(false);

	let showCreateServerModal = $state(false);
	let creatingServer = $state(false);
	let newServerProtocols = $state<string[]>(['SFTP']);
	let newServerEndpointType = $state<'PUBLIC' | 'VPC'>('PUBLIC');
	let newServerIdentityProviderType = $state<'SERVICE_MANAGED' | 'API_GATEWAY'>('SERVICE_MANAGED');

	// Users
	let selectedServerId = $state('');
	let showCreateUserModal = $state(false);
	let creatingUser = $state(false);
	let newUserName = $state('');
	let newUserRole = $state('');
	let newUserHomeDirectory = $state('/');

	// Access
	let accesses = $state<ListedAccess[]>([]);
	let accessServerIdFilter = $state('');
	let showCreateAccessModal = $state(false);
	let creatingAccess = $state(false);
	let newAccessServerId = $state('');
	let newAccessExternalId = $state('');
	let newAccessRole = $state('');
	let newAccessHomeDir = $state('/');

	// Agreements
	let agreements = $state<ListedAgreement[]>([]);
	let agreementServerIdFilter = $state('');
	let showCreateAgreementModal = $state(false);
	let creatingAgreement = $state(false);
	let newAgreementServerId = $state('');
	let newAgreementDescription = $state('');
	let newAgreementLocalProfile = $state('');
	let newAgreementPartnerProfile = $state('');
	let newAgreementBaseDir = $state('/');
	let newAgreementAccessRole = $state('');

	// Connectors
	let connectors = $state<ListedConnector[]>([]);
	let showCreateConnectorModal = $state(false);
	let creatingConnector = $state(false);
	let newConnectorUrl = $state('');
	let newConnectorAccessRole = $state('');

	// Profiles
	let profiles = $state<ListedProfile[]>([]);
	let showCreateProfileModal = $state(false);
	let creatingProfile = $state(false);
	let newProfileType = $state<'LOCAL' | 'PARTNER'>('LOCAL');
	let newProfileAs2Id = $state('');

	// WebApps
	let webApps = $state<{ WebAppId?: string }[]>([]);
	let showCreateWebAppModal = $state(false);
	let creatingWebApp = $state(false);

	// Workflows
	let workflows = $state<ListedWorkflow[]>([]);
	let showCreateWorkflowModal = $state(false);
	let creatingWorkflow = $state(false);
	let newWorkflowDescription = $state('');

	// Certificates
	let certificates = $state<ListedCertificate[]>([]);
	let showImportCertModal = $state(false);
	let importingCert = $state(false);
	let newCertUsage = $state<'SIGNING' | 'ENCRYPTION' | 'TLS'>('SIGNING');
	let newCertBody = $state('');
	let newCertDescription = $state('');

	const filteredServers = $derived(
		servers.filter(
			(s) =>
				(s.ServerId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.Domain ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function serverStateBadge(state?: string) {
		if (state === 'ONLINE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'OFFLINE') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (state === 'STARTING' || state === 'STOPPING')
			return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadServers() {
		loadingServers = true;
		try {
			const res = await transfer().send(new ListServersCommand({ MaxResults: 100 }));
			servers = res.Servers ?? [];
		} catch (e) {
			toast.error(`Failed to load servers: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingServers = false;
		}
	}

	async function viewServer(server: ListedServer) {
		selectedServer = server;
		if (!server.ServerId) return;
		selectedServerId = server.ServerId;
		loadingUsers = true;
		try {
			const [detailRes, usersRes] = await Promise.all([
				transfer().send(new DescribeServerCommand({ ServerId: server.ServerId })),
				transfer().send(new ListUsersCommand({ ServerId: server.ServerId }))
			]);
			serverDetail = detailRes.Server ?? null;
			serverUsers = usersRes.Users ?? [];
		} catch (e) {
			toast.error(`Failed to load server details: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingUsers = false;
		}
		// Load host keys
		await loadHostKeys();
	}

	async function toggleServer(server: ListedServer) {
		if (!server.ServerId) return;
		try {
			if (server.State === 'ONLINE') {
				await transfer().send(new StopServerCommand({ ServerId: server.ServerId }));
				toast.success(`Stopping server ${server.ServerId}`);
			} else {
				await transfer().send(new StartServerCommand({ ServerId: server.ServerId }));
				toast.success(`Starting server ${server.ServerId}`);
			}
			await loadServers();
		} catch (e) {
			toast.error(`Failed to toggle server: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	async function deleteServer(server: ListedServer) {
		if (!server.ServerId || !await confirmDestructive({ title: 'Delete Transfer Server', message: `Delete server ${server.ServerId}? All user sessions will be terminated.` })) return;
		try {
			await transfer().send(new DeleteServerCommand({ ServerId: server.ServerId }));
			toast.success(`Server deleted`);
			selectedServer = null;
			await loadServers();
		} catch (e) {
			toast.error(`Failed to delete server: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	async function createServer() {
		creatingServer = true;
		try {
			await transfer().send(
				new CreateServerCommand({
					Protocols: newServerProtocols as ('SFTP' | 'FTP' | 'FTPS' | 'AS2')[],
					EndpointType: newServerEndpointType,
					IdentityProviderType: newServerIdentityProviderType
				})
			);
			toast.success('Server created');
			showCreateServerModal = false;
			newServerProtocols = ['SFTP'];
			await loadServers();
		} catch (e) {
			toast.error(`Failed to create server: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingServer = false;
		}
	}

	async function createUser() {
		if (!newUserName.trim() || !newUserRole.trim() || !selectedServerId) return;
		creatingUser = true;
		try {
			await transfer().send(
				new CreateUserCommand({
					ServerId: selectedServerId,
					UserName: newUserName.trim(),
					Role: newUserRole.trim(),
					HomeDirectory: newUserHomeDirectory.trim()
				})
			);
			toast.success(`User "${newUserName}" created`);
			showCreateUserModal = false;
			newUserName = '';
			newUserRole = '';
			if (selectedServer) await viewServer(selectedServer);
		} catch (e) {
			toast.error(`Failed to create user: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingUser = false;
		}
	}

	async function deleteUser(userName: string) {
		if (!selectedServerId || !await confirmDestructive({ title: 'Delete User', message: `Delete user "${userName}"? The user will immediately lose access to the transfer server.` })) return;
		try {
			await transfer().send(new DeleteUserCommand({ ServerId: selectedServerId, UserName: userName }));
			toast.success(`User "${userName}" deleted`);
			if (selectedServer) await viewServer(selectedServer);
		} catch (e) {
			toast.error(`Failed to delete user: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Access ---
	async function loadAccesses() {
		if (!accessServerIdFilter.trim()) {
			accesses = [];
			return;
		}
		try {
			const res = await transfer().send(new ListAccessesCommand({ ServerId: accessServerIdFilter.trim() }));
			accesses = res.Accesses ?? [];
		} catch (e) {
			toast.error(`Failed to load accesses: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	async function createAccess() {
		if (!newAccessServerId.trim() || !newAccessExternalId.trim()) return;
		creatingAccess = true;
		try {
			await transfer().send(new CreateAccessCommand({
				ServerId: newAccessServerId.trim(),
				ExternalId: newAccessExternalId.trim(),
				Role: newAccessRole.trim(),
				HomeDirectory: newAccessHomeDir.trim()
			}));
			toast.success(`Access created`);
			showCreateAccessModal = false;
			newAccessExternalId = '';
			newAccessRole = '';
			newAccessHomeDir = '/';
			accessServerIdFilter = newAccessServerId;
			await loadAccesses();
		} catch (e) {
			toast.error(`Failed to create access: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingAccess = false;
		}
	}

	async function deleteAccess(access: ListedAccess) {
		if (!access.ExternalId || !accessServerIdFilter || !await confirmDestructive({ title: 'Delete Access', message: `Delete access "${access.ExternalId}"?` })) return;
		try {
			await transfer().send(new DeleteAccessCommand({ ServerId: accessServerIdFilter, ExternalId: access.ExternalId }));
			toast.success('Access deleted');
			await loadAccesses();
		} catch (e) {
			toast.error(`Failed to delete access: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Agreements ---
	async function loadAgreements() {
		if (!agreementServerIdFilter.trim()) {
			agreements = [];
			return;
		}
		try {
			const res = await transfer().send(new ListAgreementsCommand({ ServerId: agreementServerIdFilter.trim() }));
			agreements = res.Agreements ?? [];
		} catch (e) {
			toast.error(`Failed to load agreements: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	async function createAgreement() {
		if (!newAgreementServerId.trim()) return;
		creatingAgreement = true;
		try {
			await transfer().send(new CreateAgreementCommand({
				ServerId: newAgreementServerId.trim(),
				Description: newAgreementDescription.trim(),
				LocalProfileId: newAgreementLocalProfile.trim(),
				PartnerProfileId: newAgreementPartnerProfile.trim(),
				BaseDirectory: newAgreementBaseDir.trim(),
				AccessRole: newAgreementAccessRole.trim()
			}));
			toast.success('Agreement created');
			showCreateAgreementModal = false;
			newAgreementDescription = '';
			newAgreementLocalProfile = '';
			newAgreementPartnerProfile = '';
			newAgreementBaseDir = '/';
			newAgreementAccessRole = '';
			agreementServerIdFilter = newAgreementServerId;
			await loadAgreements();
		} catch (e) {
			toast.error(`Failed to create agreement: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingAgreement = false;
		}
	}

	async function deleteAgreement(agreement: ListedAgreement) {
		if (!agreement.AgreementId || !agreementServerIdFilter || !await confirmDestructive({ title: 'Delete Agreement', message: `Delete agreement "${agreement.AgreementId}"?` })) return;
		try {
			await transfer().send(new DeleteAgreementCommand({ ServerId: agreementServerIdFilter, AgreementId: agreement.AgreementId }));
			toast.success('Agreement deleted');
			await loadAgreements();
		} catch (e) {
			toast.error(`Failed to delete agreement: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Host Keys ---
	async function loadHostKeys() {
		if (!selectedServerId) return;
		loadingHostKeys = true;
		try {
			const res = await transfer().send(new ListHostKeysCommand({ ServerId: selectedServerId }));
			serverHostKeys = (res.HostKeys ?? []) as { HostKeyId?: string; Type?: string; Description?: string }[];
		} catch (e) {
			toast.error(`Failed to load host keys: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingHostKeys = false;
		}
	}

	async function importHostKey() {
		if (!newHostKeyBody.trim() || !selectedServerId) return;
		importingHostKey = true;
		try {
			await transfer().send(new ImportHostKeyCommand({
				ServerId: selectedServerId,
				HostKeyBody: newHostKeyBody.trim(),
				Description: newHostKeyDescription.trim()
			}));
			toast.success('Host key imported');
			showImportHostKeyModal = false;
			newHostKeyBody = '';
			newHostKeyDescription = '';
			await loadHostKeys();
		} catch (e) {
			toast.error(`Failed to import host key: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			importingHostKey = false;
		}
	}

	async function deleteHostKey(hostKeyId: string) {
		if (!selectedServerId || !await confirmDestructive({ title: 'Delete Host Key', message: `Delete host key "${hostKeyId}"?` })) return;
		try {
			await transfer().send(new DeleteHostKeyCommand({ ServerId: selectedServerId, HostKeyId: hostKeyId }));
			toast.success('Host key deleted');
			await loadHostKeys();
		} catch (e) {
			toast.error(`Failed to delete host key: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- SSH Public Keys ---
	async function loadSshKeys(serverId: string, userName: string) {
		loadingSshKeys = true;
		try {
			// Use DescribeUser to get SSH keys (the API stores them on the user)
			const res = await transfer().send(new ListUsersCommand({ ServerId: serverId }));
			// DescribeUser has SshPublicKeys; for now show from the user list
			const user = (res.Users ?? []).find((u) => u.UserName === userName);
			userSshKeys = ((user as unknown as Record<string, unknown>)?.['SshPublicKeys'] as { SshPublicKeyId?: string; Fingerprint?: string; DateImported?: string; UserName?: string }[]) ?? [];
		} catch (e) {
			toast.error(`Failed to load SSH keys: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingSshKeys = false;
		}
	}

	async function selectUser(user: ListedUser) {
		selectedUser = user;
		userDetail = null;
		if (!selectedServerId || !user.UserName) return;
		loadingUserDetail = true;
		try {
			const res = await transfer().send(new DescribeUserCommand({ ServerId: selectedServerId, UserName: user.UserName }));
			userDetail = res.User ?? null;
		} catch (e) {
			toast.error(`Failed to load user details: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingUserDetail = false;
		}
	}

	async function importSshKey() {
		if (!newSshKeyBody.trim() || !sshKeyTargetUser || !selectedServerId) return;
		importingSshKey = true;
		try {
			await transfer().send(new ImportSshPublicKeyCommand({
				ServerId: selectedServerId,
				UserName: sshKeyTargetUser,
				SshPublicKeyBody: newSshKeyBody.trim()
			}));
			toast.success('SSH public key imported');
			showImportSshKeyModal = false;
			newSshKeyBody = '';
		} catch (e) {
			toast.error(`Failed to import SSH key: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			importingSshKey = false;
		}
	}

	async function deleteSshKey(userName: string, keyId: string) {
		if (!selectedServerId || !await confirmDestructive({ title: 'Delete SSH Key', message: `Delete SSH key "${keyId}" for user "${userName}"?` })) return;
		try {
			await transfer().send(new DeleteSshPublicKeyCommand({ ServerId: selectedServerId, UserName: userName, SshPublicKeyId: keyId }));
			toast.success('SSH key deleted');
		} catch (e) {
			toast.error(`Failed to delete SSH key: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Connectors ---
	async function loadConnectors() {
		loadingConnectors = true;
		try {
			const res = await transfer().send(new ListConnectorsCommand({}));
			connectors = res.Connectors ?? [];
		} catch (e) {
			toast.error(`Failed to load connectors: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingConnectors = false;
		}
	}

	async function createConnector() {
		if (!newConnectorUrl.trim()) return;
		creatingConnector = true;
		try {
			await transfer().send(new CreateConnectorCommand({
				Url: newConnectorUrl.trim(),
				AccessRole: newConnectorAccessRole.trim()
			}));
			toast.success('Connector created');
			showCreateConnectorModal = false;
			newConnectorUrl = '';
			newConnectorAccessRole = '';
			await loadConnectors();
		} catch (e) {
			toast.error(`Failed to create connector: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingConnector = false;
		}
	}

	async function deleteConnector(connector: ListedConnector) {
		if (!connector.ConnectorId || !await confirmDestructive({ title: 'Delete Connector', message: `Delete connector "${connector.ConnectorId}"?` })) return;
		try {
			await transfer().send(new DeleteConnectorCommand({ ConnectorId: connector.ConnectorId }));
			toast.success('Connector deleted');
			await loadConnectors();
		} catch (e) {
			toast.error(`Failed to delete connector: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Profiles ---
	async function loadProfiles() {
		loadingProfiles = true;
		try {
			const res = await transfer().send(new ListProfilesCommand({}));
			profiles = res.Profiles ?? [];
		} catch (e) {
			toast.error(`Failed to load profiles: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingProfiles = false;
		}
	}

	async function createProfile() {
		creatingProfile = true;
		try {
			await transfer().send(new CreateProfileCommand({
				ProfileType: newProfileType,
				As2Id: newProfileAs2Id.trim()
			}));
			toast.success('Profile created');
			showCreateProfileModal = false;
			newProfileAs2Id = '';
			await loadProfiles();
		} catch (e) {
			toast.error(`Failed to create profile: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingProfile = false;
		}
	}

	async function deleteProfile(profile: ListedProfile) {
		if (!profile.ProfileId || !await confirmDestructive({ title: 'Delete Profile', message: `Delete profile "${profile.ProfileId}"?` })) return;
		try {
			await transfer().send(new DeleteProfileCommand({ ProfileId: profile.ProfileId }));
			toast.success('Profile deleted');
			await loadProfiles();
		} catch (e) {
			toast.error(`Failed to delete profile: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- WebApps ---
	async function loadWebApps() {
		loadingWebApps = true;
		try {
			const res = await transfer().send(new ListWebAppsCommand({}));
			webApps = res.WebApps ?? [];
		} catch (e) {
			toast.error(`Failed to load web apps: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingWebApps = false;
		}
	}

	async function createWebApp() {
		creatingWebApp = true;
		try {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
		await transfer().send(new CreateWebAppCommand({ IdentityProviderDetails: { $unknown: ['IdentityCenterConfig', {}] } as any }));
			toast.success('Web app created');
			showCreateWebAppModal = false;
			await loadWebApps();
		} catch (e) {
			toast.error(`Failed to create web app: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingWebApp = false;
		}
	}

	async function deleteWebApp(app: { WebAppId?: string }) {
		if (!app.WebAppId || !await confirmDestructive({ title: 'Delete Web App', message: `Delete web app "${app.WebAppId}"?` })) return;
		try {
			await transfer().send(new DeleteWebAppCommand({ WebAppId: app.WebAppId }));
			toast.success('Web app deleted');
			await loadWebApps();
		} catch (e) {
			toast.error(`Failed to delete web app: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Workflows ---
	async function loadWorkflows() {
		loadingWorkflows = true;
		try {
			const res = await transfer().send(new ListWorkflowsCommand({}));
			workflows = res.Workflows ?? [];
		} catch (e) {
			toast.error(`Failed to load workflows: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingWorkflows = false;
		}
	}

	async function createWorkflow() {
		creatingWorkflow = true;
		try {
			await transfer().send(new CreateWorkflowCommand({
				Description: newWorkflowDescription.trim(),
				Steps: []
			}));
			toast.success('Workflow created');
			showCreateWorkflowModal = false;
			newWorkflowDescription = '';
			await loadWorkflows();
		} catch (e) {
			toast.error(`Failed to create workflow: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			creatingWorkflow = false;
		}
	}

	async function deleteWorkflow(workflow: ListedWorkflow) {
		if (!workflow.WorkflowId || !await confirmDestructive({ title: 'Delete Workflow', message: `Delete workflow "${workflow.WorkflowId}"?` })) return;
		try {
			await transfer().send(new DeleteWorkflowCommand({ WorkflowId: workflow.WorkflowId }));
			toast.success('Workflow deleted');
			await loadWorkflows();
		} catch (e) {
			toast.error(`Failed to delete workflow: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	// --- Certificates ---
	async function loadCertificates() {
		loadingCertificates = true;
		try {
			const res = await transfer().send(new ListCertificatesCommand({}));
			certificates = res.Certificates ?? [];
		} catch (e) {
			toast.error(`Failed to load certificates: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			loadingCertificates = false;
		}
	}

	async function importCertificate() {
		importingCert = true;
		try {
			await transfer().send(new ImportCertificateCommand({
				Usage: newCertUsage,
				Certificate: newCertBody.trim(),
				Description: newCertDescription.trim()
			}));
			toast.success('Certificate imported');
			showImportCertModal = false;
			newCertBody = '';
			newCertDescription = '';
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to import certificate: ${e instanceof Error ? e.message : String(e)}`);
		} finally {
			importingCert = false;
		}
	}

	async function deleteCertificate(cert: ListedCertificate) {
		if (!cert.CertificateId || !await confirmDestructive({ title: 'Delete Certificate', message: `Delete certificate "${cert.CertificateId}"?` })) return;
		try {
			await transfer().send(new DeleteCertificateCommand({ CertificateId: cert.CertificateId }));
			toast.success('Certificate deleted');
			await loadCertificates();
		} catch (e) {
			toast.error(`Failed to delete certificate: ${e instanceof Error ? e.message : String(e)}`);
		}
	}

	function switchTab(tab: TabName) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'servers') loadServers();
		else if (tab === 'connectors') loadConnectors();
		else if (tab === 'profiles') loadProfiles();
		else if (tab === 'webapps') loadWebApps();
		else if (tab === 'workflows') loadWorkflows();
		else if (tab === 'certificates') loadCertificates();
	}

	// Servers and every server-scoped detail (users, host keys, SSH keys),
	// plus access, agreements, connectors, profiles, web apps, workflows, and
	// certificates, are all region-scoped, so a region switch must clear them
	// (not just reload) and then reload whichever tab is currently active.
	// `activeTab` is read via `untrack` because `switchTab` also writes it —
	// without that, this effect would depend on `activeTab` too and
	// re-clear/re-fetch everything on every tab click, not just on a region
	// change.
	onRegionChange(() => {
		servers = [];
		selectedServer = null;
		serverDetail = null;
		serverUsers = [];
		serverHostKeys = [];
		selectedServerId = '';
		selectedUser = null;
		userDetail = null;
		userSshKeys = [];
		accesses = [];
		accessServerIdFilter = '';
		agreements = [];
		agreementServerIdFilter = '';
		connectors = [];
		profiles = [];
		webApps = [];
		workflows = [];
		certificates = [];
		showCreateServerModal = false;
		showImportHostKeyModal = false;
		showCreateUserModal = false;
		showImportSshKeyModal = false;
		showCreateAccessModal = false;
		showCreateAgreementModal = false;
		showCreateConnectorModal = false;
		showCreateProfileModal = false;
		showCreateWebAppModal = false;
		showCreateWorkflowModal = false;
		showImportCertModal = false;
		const tab = untrack(() => activeTab);
		if (tab === 'connectors') void loadConnectors();
		else if (tab === 'profiles') void loadProfiles();
		else if (tab === 'webapps') void loadWebApps();
		else if (tab === 'workflows') void loadWorkflows();
		else if (tab === 'certificates') void loadCertificates();
		else void loadServers();
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ArrowLeftRight class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">Transfer Family</h1>
				<p class="text-sm text-muted-foreground">Managed file transfer servers (SFTP, FTP, FTPS)</p>
			</div>
		</div>
		<button
			onclick={() => switchTab(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tab bar -->
	<div class="flex gap-1 border-b overflow-x-auto">
		{#each [
			{ id: 'servers', label: 'Servers', icon: Server },
			{ id: 'access', label: 'Access', icon: Users },
			{ id: 'agreements', label: 'Agreements', icon: FileKey },
			{ id: 'connectors', label: 'Connectors', icon: Link },
			{ id: 'profiles', label: 'Profiles', icon: ShieldCheck },
			{ id: 'webapps', label: 'WebApps', icon: Globe },
			{ id: 'workflows', label: 'Workflows', icon: GitBranch },
			{ id: 'certificates', label: 'Certificates', icon: ShieldCheck },
		] as { id, label, icon }}
			<button
				onclick={() => switchTab(id as TabName)}
				class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === id
					? 'border-primary text-primary'
					: 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<svelte:component this={icon} class="h-4 w-4" />
				{label}
			</button>
		{/each}
	</div>

	<!-- ===== SERVERS TAB ===== -->
	{#if activeTab === 'servers'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search servers..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateServerModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Server
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredServers.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Server class="h-12 w-12 mb-3 opacity-30" />
				<p>No Transfer Family servers found</p>
				<p class="text-sm">Create a server to enable managed file transfers</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Server ID</th>
							<th class="px-4 py-3 text-left font-medium">Domain</th>
							<th class="px-4 py-3 text-left font-medium">Endpoint</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">Users</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredServers as server}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewServer(server)}
							>
								<td class="px-4 py-3 font-mono text-xs">{server.ServerId}</td>
								<td class="px-4 py-3">{server.Domain ?? 'S3'}</td>
								<td class="px-4 py-3 text-muted-foreground">{server.EndpointType ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {serverStateBadge(server.State)}">
										{server.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{server.UserCount ?? 0}</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); toggleServer(server); }}
										class="rounded p-1 hover:bg-accent"
										title={server.State === 'ONLINE' ? 'Stop' : 'Start'}
									>
										{#if server.State === 'ONLINE'}
											<Square class="h-4 w-4 text-yellow-500" />
										{:else}
											<Play class="h-4 w-4 text-green-500" />
										{/if}
									</button>
									<button
										onclick={(e) => { e.stopPropagation(); deleteServer(server); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete server"
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

		{#if selectedServer}
			<div class="rounded-lg border p-4 space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold flex items-center gap-2">
						<Users class="h-5 w-5" />
						{selectedServer.ServerId} — Users
					</h3>
					<div class="flex gap-2">
						<button
							onclick={() => (showCreateUserModal = true)}
							class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
						>
							<Plus class="h-3 w-3" />
							Add User
						</button>
						<button onclick={() => { selectedServer = null; serverUsers = []; selectedUser = null; userDetail = null; }} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
				</div>

				{#if serverDetail}
					<div class="grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Protocols</p>
							<div class="flex flex-wrap gap-1 mt-1">
								{#each serverDetail.Protocols ?? [] as proto}
									<span class="rounded bg-blue-100 dark:bg-blue-900/30 px-1.5 py-0.5 text-xs text-blue-700 dark:text-blue-300">{proto}</span>
								{/each}
							</div>
						</div>
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Endpoint Type</p>
							<p class="font-medium mt-0.5">{serverDetail.EndpointType ?? '—'}</p>
						</div>
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Identity Provider</p>
							<p class="font-medium mt-0.5">{serverDetail.IdentityProviderType ?? '—'}</p>
						</div>
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Domain</p>
							<p class="font-medium mt-0.5">{serverDetail.Domain ?? 'S3'}</p>
						</div>
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Security Policy</p>
							<p class="font-medium mt-0.5 text-xs truncate">{serverDetail.SecurityPolicyName ?? '—'}</p>
						</div>
						<div class="rounded-md bg-muted/30 px-3 py-2">
							<p class="text-xs text-muted-foreground">Users</p>
							<p class="font-medium mt-0.5">{selectedServer?.UserCount ?? 0}</p>
						</div>
					</div>
					{#if serverDetail.HostKeyFingerprint}
						<div class="rounded-md bg-muted/30 px-3 py-2 text-sm">
							<p class="text-xs text-muted-foreground mb-0.5">Host Key Fingerprint</p>
							<p class="font-mono text-xs break-all">{serverDetail.HostKeyFingerprint}</p>
						</div>
					{/if}
					{#if serverDetail.EndpointDetails?.VpcId || serverDetail.EndpointDetails?.VpcEndpointId}
						<div class="rounded-md bg-muted/30 px-3 py-2 text-sm grid grid-cols-2 gap-2">
							{#if serverDetail.EndpointDetails.VpcId}
								<div>
									<p class="text-xs text-muted-foreground">VPC ID</p>
									<p class="font-mono text-xs">{serverDetail.EndpointDetails.VpcId}</p>
								</div>
							{/if}
							{#if serverDetail.EndpointDetails.VpcEndpointId}
								<div>
									<p class="text-xs text-muted-foreground">VPC Endpoint</p>
									<p class="font-mono text-xs">{serverDetail.EndpointDetails.VpcEndpointId}</p>
								</div>
							{/if}
						</div>
					{/if}
				{/if}

				{#if loadingUsers}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else if serverUsers.length === 0}
					<p class="text-sm text-muted-foreground">No users configured.</p>
				{:else}
					<div class="rounded border overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-muted/50">
								<tr>
									<th class="px-3 py-2 text-left font-medium">Username</th>
									<th class="px-3 py-2 text-left font-medium">Home Directory</th>
									<th class="px-3 py-2 text-left font-medium">Dir Type</th>
									<th class="px-3 py-2 text-left font-medium">SSH Keys</th>
									<th class="px-3 py-2 text-right font-medium">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y">
								{#each serverUsers as user}
									<tr
										class="hover:bg-muted/30 cursor-pointer {selectedUser?.UserName === user.UserName ? 'bg-blue-50 dark:bg-blue-900/10' : ''}"
										onclick={() => selectUser(user)}
									>
										<td class="px-3 py-2 font-medium">{user.UserName}</td>
										<td class="px-3 py-2 text-muted-foreground font-mono text-xs">
											{user.HomeDirectory ?? '/'}
										</td>
										<td class="px-3 py-2 text-muted-foreground text-xs">{user.HomeDirectoryType ?? 'PATH'}</td>
										<td class="px-3 py-2 text-muted-foreground text-xs">{user.SshPublicKeyCount ?? 0}</td>
										<td class="px-3 py-2 text-right flex justify-end gap-1">
											<button
												onclick={(e) => { e.stopPropagation(); sshKeyTargetUser = user.UserName ?? ''; showImportSshKeyModal = true; }}
												class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
												title="Import SSH key for this user"
											>
												<Plus class="h-3.5 w-3.5" />
											</button>
											<button
												onclick={(e) => { e.stopPropagation(); deleteUser(user.UserName ?? ''); }}
												class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											>
												<Trash2 class="h-3.5 w-3.5" />
											</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>

					<!-- User Detail Panel -->
					{#if selectedUser}
						<div class="rounded-lg border p-4 space-y-3 bg-muted/10">
							<div class="flex items-center justify-between">
								<h4 class="font-semibold text-sm">{selectedUser.UserName}</h4>
								<button onclick={() => { selectedUser = null; userDetail = null; }} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
							</div>
							{#if loadingUserDetail}
								<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
							{:else if userDetail}
								<div class="grid grid-cols-2 gap-2 text-sm">
									<div>
										<p class="text-xs text-muted-foreground">Home Directory</p>
										<p class="font-mono text-xs mt-0.5">{userDetail.HomeDirectory ?? '/'}</p>
									</div>
									<div>
										<p class="text-xs text-muted-foreground">Directory Type</p>
										<p class="font-medium text-xs mt-0.5">{userDetail.HomeDirectoryType ?? 'PATH'}</p>
									</div>
								</div>
								{#if userDetail.Role}
									<div>
										<p class="text-xs text-muted-foreground">IAM Role ARN</p>
										<p class="font-mono text-xs break-all mt-0.5 text-foreground">{userDetail.Role}</p>
									</div>
								{/if}
								{#if userDetail.PosixProfile}
									<div class="text-sm">
										<p class="text-xs text-muted-foreground mb-1">POSIX Profile</p>
										<div class="flex gap-4">
											<span>UID: <span class="font-mono">{userDetail.PosixProfile.Uid}</span></span>
											<span>GID: <span class="font-mono">{userDetail.PosixProfile.Gid}</span></span>
										</div>
									</div>
								{/if}
								{#if (userDetail.SshPublicKeys?.length ?? 0) > 0}
									<div>
										<p class="text-xs text-muted-foreground mb-1">SSH Public Keys</p>
										<div class="rounded border overflow-hidden">
											<table class="w-full text-xs">
												<thead class="bg-muted/50">
													<tr>
														<th class="px-2 py-1.5 text-left font-medium">Key ID</th>
														<th class="px-2 py-1.5 text-left font-medium">Imported</th>
														<th class="px-2 py-1.5 text-right font-medium">Actions</th>
													</tr>
												</thead>
												<tbody class="divide-y">
													{#each userDetail.SshPublicKeys ?? [] as key}
														<tr>
															<td class="px-2 py-1.5 font-mono">{key.SshPublicKeyId}</td>
															<td class="px-2 py-1.5 text-muted-foreground">{key.DateImported ? new Date(key.DateImported).toLocaleDateString() : '—'}</td>
															<td class="px-2 py-1.5 text-right">
																<button
																	onclick={() => deleteSshKey(userDetail?.UserName ?? '', key.SshPublicKeyId ?? '')}
																	class="rounded p-0.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
																>
																	<Trash2 class="h-3 w-3" />
																</button>
															</td>
														</tr>
													{/each}
												</tbody>
											</table>
										</div>
									</div>
								{:else}
									<p class="text-xs text-muted-foreground">No SSH public keys configured.</p>
								{/if}
							{/if}
						</div>
					{/if}
				{/if}

				<!-- Host Keys section -->
				<div class="border-t pt-4 mt-2">
					<div class="flex items-center justify-between mb-2">
						<h4 class="text-sm font-semibold flex items-center gap-1.5">
							<FileKey class="h-4 w-4" />
							Host Keys
						</h4>
						<button
							onclick={() => (showImportHostKeyModal = true)}
							class="flex items-center gap-1 rounded-md border px-2 py-1 text-xs hover:bg-accent"
						>
							<Plus class="h-3 w-3" />
							Import Host Key
						</button>
					</div>
					{#if loadingHostKeys}
						<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
					{:else if serverHostKeys.length === 0}
						<p class="text-xs text-muted-foreground">No host keys imported.</p>
					{:else}
						<div class="rounded border overflow-hidden">
							<table class="w-full text-xs">
								<thead class="bg-muted/50">
									<tr>
										<th class="px-3 py-2 text-left font-medium">Host Key ID</th>
										<th class="px-3 py-2 text-left font-medium">Type</th>
										<th class="px-3 py-2 text-left font-medium">Description</th>
										<th class="px-3 py-2 text-right font-medium">Actions</th>
									</tr>
								</thead>
								<tbody class="divide-y">
									{#each serverHostKeys as hk}
										<tr>
											<td class="px-3 py-2 font-mono">{hk.HostKeyId ?? '—'}</td>
											<td class="px-3 py-2">{hk.Type ?? '—'}</td>
											<td class="px-3 py-2 text-muted-foreground">{hk.Description ?? '—'}</td>
											<td class="px-3 py-2 text-right">
												<button
													onclick={() => deleteHostKey(hk.HostKeyId ?? '')}
													class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
												>
													<Trash2 class="h-3 w-3" />
												</button>
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/if}

	<!-- ===== ACCESS TAB ===== -->
	{#if activeTab === 'access'}
		<div class="flex items-center gap-4">
			<div class="flex-1 flex gap-2">
				<input
					type="text"
					placeholder="Server ID (e.g. s-abc123)"
					bind:value={accessServerIdFilter}
					class="flex-1 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<button
					onclick={loadAccesses}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
				>
					<Search class="h-4 w-4" />
					Search
				</button>
			</div>
			<button
				onclick={() => (showCreateAccessModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Access
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if accesses.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Users class="h-12 w-12 mb-3 opacity-30" />
				<p>No access entries found</p>
				<p class="text-sm">Enter a server ID and search, or create a new access entry</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">External ID</th>
							<th class="px-4 py-3 text-left font-medium">Home Directory</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each accesses as access}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{access.ExternalId ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{access.HomeDirectory ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteAccess(access)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== AGREEMENTS TAB ===== -->
	{#if activeTab === 'agreements'}
		<div class="flex items-center gap-4">
			<div class="flex-1 flex gap-2">
				<input
					type="text"
					placeholder="Server ID (e.g. s-abc123)"
					bind:value={agreementServerIdFilter}
					class="flex-1 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<button
					onclick={loadAgreements}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
				>
					<Search class="h-4 w-4" />
					Search
				</button>
			</div>
			<button
				onclick={() => (showCreateAgreementModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Agreement
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if agreements.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<FileKey class="h-12 w-12 mb-3 opacity-30" />
				<p>No agreements found</p>
				<p class="text-sm">Enter a server ID and search, or create a new agreement</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Agreement ID</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each agreements as agreement}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{agreement.AgreementId ?? '—'}</td>
								<td class="px-4 py-3">{agreement.Description ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{agreement.Status ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteAgreement(agreement)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== CONNECTORS TAB ===== -->
	{#if activeTab === 'connectors'}
		<div class="flex items-center justify-between gap-4">
			<p class="text-sm text-muted-foreground">Connectors initiate outbound file transfers to external endpoints.</p>
			<button
				onclick={() => (showCreateConnectorModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Connector
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if connectors.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Link class="h-12 w-12 mb-3 opacity-30" />
				<p>No connectors configured</p>
				<p class="text-sm">Create a connector to transfer files to remote servers.</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Connector ID</th>
							<th class="px-4 py-3 text-left font-medium">URL</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each connectors as connector}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{connector.ConnectorId ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{connector.Url ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteConnector(connector)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== PROFILES TAB ===== -->
	{#if activeTab === 'profiles'}
		<div class="flex items-center justify-between gap-4">
			<p class="text-sm text-muted-foreground">AS2 profiles define local and partner identities for AS2 transfers.</p>
			<button
				onclick={() => (showCreateProfileModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Profile
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if profiles.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<ShieldCheck class="h-12 w-12 mb-3 opacity-30" />
				<p>No AS2 profiles</p>
				<p class="text-sm">Create a local or partner profile for AS2 transfers.</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Profile ID</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">AS2 ID</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each profiles as profile}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{profile.ProfileId ?? '—'}</td>
								<td class="px-4 py-3">{profile.ProfileType ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{profile.As2Id ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteProfile(profile)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== WEBAPPS TAB ===== -->
	{#if activeTab === 'webapps'}
		<div class="flex items-center justify-between gap-4">
			<p class="text-sm text-muted-foreground">Web applications provide browser-based file transfer interfaces.</p>
			<button
				onclick={() => (showCreateWebAppModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Web App
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if webApps.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Globe class="h-12 w-12 mb-3 opacity-30" />
				<p>No web apps</p>
				<p class="text-sm">Create a web app for browser-based file transfers.</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Web App ID</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each webApps as app}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{app.WebAppId ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteWebApp(app)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== WORKFLOWS TAB ===== -->
	{#if activeTab === 'workflows'}
		<div class="flex items-center justify-between gap-4">
			<p class="text-sm text-muted-foreground">Workflows automate file processing steps after upload.</p>
			<button
				onclick={() => (showCreateWorkflowModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Workflow
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if workflows.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<GitBranch class="h-12 w-12 mb-3 opacity-30" />
				<p>No workflows</p>
				<p class="text-sm">Workflows process files after transfer completes.</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Workflow ID</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each workflows as workflow}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{workflow.WorkflowId ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{workflow.Description ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteWorkflow(workflow)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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

	<!-- ===== CERTIFICATES TAB ===== -->
	{#if activeTab === 'certificates'}
		<div class="flex items-center justify-between gap-4">
			<p class="text-sm text-muted-foreground">Certificates are used for AS2 message signing and encryption.</p>
			<button
				onclick={() => (showImportCertModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Import Certificate
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if certificates.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<ShieldCheck class="h-12 w-12 mb-3 opacity-30" />
				<p>No certificates imported</p>
				<p class="text-sm">Import certificates for AS2 or TLS connections.</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Certificate ID</th>
							<th class="px-4 py-3 text-left font-medium">Usage</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each certificates as cert}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{cert.CertificateId ?? '—'}</td>
								<td class="px-4 py-3">{cert.Usage ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{cert.Status ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteCertificate(cert)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950">
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
</div>

<!-- ===== MODALS ===== -->

<!-- Create Server Modal -->
{#if showCreateServerModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Transfer Server</h2>
			<div class="space-y-3">
				<div>
					<p class="block text-sm font-medium mb-2">Protocols</p>
					<div class="flex gap-3">
						{#each ['SFTP', 'FTP', 'FTPS', 'AS2'] as proto}
							<label class="flex items-center gap-1.5 text-sm">
								<input
									type="checkbox"
									checked={newServerProtocols.includes(proto)}
									onchange={() => {
										if (newServerProtocols.includes(proto)) {
											newServerProtocols = newServerProtocols.filter((p) => p !== proto);
										} else {
											newServerProtocols = [...newServerProtocols, proto];
										}
									}}
								/>
								{proto}
							</label>
						{/each}
					</div>
				</div>
				<div>
					<label for="server-endpoint" class="block text-sm font-medium mb-1">Endpoint Type</label>
					<select
						id="server-endpoint"
						bind:value={newServerEndpointType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="PUBLIC">Public</option>
						<option value="VPC">VPC</option>
					</select>
				</div>
				<div>
					<label for="server-identity" class="block text-sm font-medium mb-1">Identity Provider</label>
					<select
						id="server-identity"
						bind:value={newServerIdentityProviderType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="SERVICE_MANAGED">Service Managed</option>
						<option value="API_GATEWAY">API Gateway</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateServerModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createServer}
					disabled={creatingServer || newServerProtocols.length === 0}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingServer ? 'Creating...' : 'Create Server'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create User Modal -->
{#if showCreateUserModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Transfer User</h2>
			<div class="space-y-3">
				<div>
					<label for="user-name" class="block text-sm font-medium mb-1">Username *</label>
					<input id="user-name" type="text" bind:value={newUserName} placeholder="john.doe" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="user-role" class="block text-sm font-medium mb-1">IAM Role ARN *</label>
					<input id="user-role" type="text" bind:value={newUserRole} placeholder="arn:aws:iam::123456789012:role/transfer-role" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="user-home" class="block text-sm font-medium mb-1">Home Directory</label>
					<input id="user-home" type="text" bind:value={newUserHomeDirectory} placeholder="/my-bucket/uploads" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateUserModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createUser}
					disabled={creatingUser || !newUserName.trim() || !newUserRole.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingUser ? 'Creating...' : 'Add User'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Access Modal -->
{#if showCreateAccessModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Access Entry</h2>
			<div class="space-y-3">
				<div>
					<label for="access-server" class="block text-sm font-medium mb-1">Server ID *</label>
					<input id="access-server" type="text" bind:value={newAccessServerId} placeholder="s-abc123" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="access-external" class="block text-sm font-medium mb-1">External ID *</label>
					<input id="access-external" type="text" bind:value={newAccessExternalId} placeholder="S-1-5-21-1234567890" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="access-role" class="block text-sm font-medium mb-1">IAM Role ARN</label>
					<input id="access-role" type="text" bind:value={newAccessRole} placeholder="arn:aws:iam::123456789012:role/transfer-role" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="access-home" class="block text-sm font-medium mb-1">Home Directory</label>
					<input id="access-home" type="text" bind:value={newAccessHomeDir} placeholder="/" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateAccessModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createAccess}
					disabled={creatingAccess || !newAccessServerId.trim() || !newAccessExternalId.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingAccess ? 'Creating...' : 'Create Access'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Agreement Modal -->
{#if showCreateAgreementModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create AS2 Agreement</h2>
			<div class="space-y-3">
				<div>
					<label for="ag-server" class="block text-sm font-medium mb-1">Server ID *</label>
					<input id="ag-server" type="text" bind:value={newAgreementServerId} placeholder="s-abc123" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="ag-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="ag-desc" type="text" bind:value={newAgreementDescription} placeholder="My AS2 agreement" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="ag-local" class="block text-sm font-medium mb-1">Local Profile ID</label>
					<input id="ag-local" type="text" bind:value={newAgreementLocalProfile} placeholder="p-local123" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="ag-partner" class="block text-sm font-medium mb-1">Partner Profile ID</label>
					<input id="ag-partner" type="text" bind:value={newAgreementPartnerProfile} placeholder="p-partner123" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="ag-basedir" class="block text-sm font-medium mb-1">Base Directory</label>
					<input id="ag-basedir" type="text" bind:value={newAgreementBaseDir} placeholder="/transfers" class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="ag-role" class="block text-sm font-medium mb-1">Access Role ARN</label>
					<input id="ag-role" type="text" bind:value={newAgreementAccessRole} placeholder="arn:aws:iam::123456789012:role/transfer-role" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateAgreementModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createAgreement}
					disabled={creatingAgreement || !newAgreementServerId.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingAgreement ? 'Creating...' : 'Create Agreement'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Connector Modal -->
{#if showCreateConnectorModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Connector</h2>
			<div class="space-y-3">
				<div>
					<label for="conn-url" class="block text-sm font-medium mb-1">URL *</label>
					<input id="conn-url" type="text" bind:value={newConnectorUrl} placeholder="https://partner.example.com/as2" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="conn-role" class="block text-sm font-medium mb-1">Access Role ARN</label>
					<input id="conn-role" type="text" bind:value={newConnectorAccessRole} placeholder="arn:aws:iam::123456789012:role/transfer-role" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateConnectorModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createConnector}
					disabled={creatingConnector || !newConnectorUrl.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingConnector ? 'Creating...' : 'Create Connector'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Profile Modal -->
{#if showCreateProfileModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create AS2 Profile</h2>
			<div class="space-y-3">
				<div>
					<label for="profile-type" class="block text-sm font-medium mb-1">Profile Type *</label>
					<select id="profile-type" bind:value={newProfileType} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="LOCAL">LOCAL</option>
						<option value="PARTNER">PARTNER</option>
					</select>
				</div>
				<div>
					<label for="profile-as2id" class="block text-sm font-medium mb-1">AS2 ID</label>
					<input id="profile-as2id" type="text" bind:value={newProfileAs2Id} placeholder="my-as2-id" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateProfileModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createProfile}
					disabled={creatingProfile}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingProfile ? 'Creating...' : 'Create Profile'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create WebApp Modal -->
{#if showCreateWebAppModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Web App</h2>
			<p class="text-sm text-muted-foreground mb-4">Create a new Transfer Family web application for browser-based file transfers.</p>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateWebAppModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createWebApp}
					disabled={creatingWebApp}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingWebApp ? 'Creating...' : 'Create Web App'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Workflow Modal -->
{#if showCreateWorkflowModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Workflow</h2>
			<div class="space-y-3">
				<div>
					<label for="wf-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="wf-desc" type="text" bind:value={newWorkflowDescription} placeholder="My processing workflow" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateWorkflowModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={createWorkflow}
					disabled={creatingWorkflow}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingWorkflow ? 'Creating...' : 'Create Workflow'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Import Certificate Modal -->
{#if showImportCertModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Import Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="cert-usage" class="block text-sm font-medium mb-1">Usage *</label>
					<select id="cert-usage" bind:value={newCertUsage} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="SIGNING">SIGNING</option>
						<option value="ENCRYPTION">ENCRYPTION</option>
						<option value="TLS">TLS</option>
					</select>
				</div>
				<div>
					<label for="cert-body" class="block text-sm font-medium mb-1">Certificate Body</label>
					<textarea id="cert-body" bind:value={newCertBody} placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----" rows={4} class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
				</div>
				<div>
					<label for="cert-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="cert-desc" type="text" bind:value={newCertDescription} placeholder="My certificate" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showImportCertModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={importCertificate}
					disabled={importingCert}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{importingCert ? 'Importing...' : 'Import Certificate'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Import Host Key Modal -->
{#if showImportHostKeyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Import Host Key</h2>
			<div class="space-y-3">
				<div>
					<label for="hk-body" class="block text-sm font-medium mb-1">Host Key Body *</label>
					<textarea id="hk-body" bind:value={newHostKeyBody} placeholder="ssh-rsa AAAAB3NzaC1yc2EAAAA..." rows={4} class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
				</div>
				<div>
					<label for="hk-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="hk-desc" type="text" bind:value={newHostKeyDescription} placeholder="My host key" class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showImportHostKeyModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={importHostKey}
					disabled={importingHostKey || !newHostKeyBody.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{importingHostKey ? 'Importing...' : 'Import Host Key'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Import SSH Public Key Modal -->
{#if showImportSshKeyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Import SSH Public Key</h2>
			<p class="text-sm text-muted-foreground mb-3">Importing key for user: <span class="font-mono font-medium">{sshKeyTargetUser}</span></p>
			<div class="space-y-3">
				<div>
					<label for="ssh-key-body" class="block text-sm font-medium mb-1">SSH Public Key *</label>
					<textarea id="ssh-key-body" bind:value={newSshKeyBody} placeholder="ssh-rsa AAAAB3NzaC1yc2EAAAA... user@host" rows={4} class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showImportSshKeyModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button
					onclick={importSshKey}
					disabled={importingSshKey || !newSshKeyBody.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{importingSshKey ? 'Importing...' : 'Import Key'}
				</button>
			</div>
		</div>
	</div>
{/if}
