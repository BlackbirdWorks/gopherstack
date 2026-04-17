import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import NeptunePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getNeptuneClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Neptune Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getByText('Amazon Neptune')).toBeInTheDocument();
	});

	it('shows subtitle', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getByText('Managed graph database service')).toBeInTheDocument();
	});

	it('shows Create Cluster button', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getByText('Create Cluster')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getAllByText('Clusters').length).toBeGreaterThan(0);
		expect(screen.getAllByText('Instances').length).toBeGreaterThan(0);
		expect(screen.getAllByText('Snapshots').length).toBeGreaterThan(0);
		expect(screen.getByText('Available')).toBeInTheDocument();
	});

	it('shows tab navigation', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getByRole('button', { name: /Clusters/ })).toBeInTheDocument();
		expect(screen.getByRole('button', { name: /Instances/ })).toBeInTheDocument();
		expect(screen.getByRole('button', { name: /Snapshots/ })).toBeInTheDocument();
	});

	it('shows empty state when no clusters', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		await waitFor(() => {
			expect(screen.getByText('No clusters found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded cluster identifiers', async () => {
		mockSend.mockResolvedValue({
			DBClusters: [
				{
					DBClusterIdentifier: 'my-neptune-cluster',
					Status: 'available',
					Engine: 'neptune',
					EngineVersion: '1.3.1.0',
					DBClusterArn: 'arn:aws:rds:us-east-1:123:cluster:my-neptune-cluster'
				}
			]
		});
		render(NeptunePage);
		await waitFor(() => {
			expect(screen.getByText('my-neptune-cluster')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('opens create cluster modal', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		await fireEvent.click(screen.getByText('Create Cluster'));
		expect(screen.getByText('Create Neptune Cluster')).toBeInTheDocument();
		expect(screen.getByText('Cluster Identifier')).toBeInTheDocument();
	});

	it('cancels create cluster modal', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		await fireEvent.click(screen.getByText('Create Cluster'));
		await fireEvent.click(screen.getByText('Cancel'));
		expect(screen.queryByText('Create Neptune Cluster')).not.toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		expect(screen.getByPlaceholderText('Search clusters...')).toBeInTheDocument();
	});

	it('shows cluster detail on click', async () => {
		mockSend.mockResolvedValue({
			DBClusters: [
				{
					DBClusterIdentifier: 'graph-cluster',
					Status: 'available',
					Engine: 'neptune',
					EngineVersion: '1.3.1.0',
					DBClusterArn: 'arn:aws:rds:us-east-1:123:cluster:graph-cluster',
					Endpoint: 'graph-cluster.cluster-abc.us-east-1.neptune.amazonaws.com',
					ReaderEndpoint: 'graph-cluster.cluster-ro-abc.us-east-1.neptune.amazonaws.com',
					MultiAZ: true,
					StorageEncrypted: true,
					DBClusterMembers: []
				}
			]
		});
		render(NeptunePage);
		await waitFor(() => expect(screen.getByText('graph-cluster')).toBeInTheDocument(), { timeout: 3000 });
		await fireEvent.click(screen.getByText('graph-cluster'));
		await waitFor(() => {
			expect(screen.getByText('Cluster ARN')).toBeInTheDocument();
			expect(screen.getByText('Engine Version')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('switches to instances tab', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		const instancesTab = screen.getByRole('button', { name: /Instances/ });
		mockSend.mockResolvedValue({ DBInstances: [] });
		await fireEvent.click(instancesTab);
		await waitFor(() => {
			expect(screen.getByText('No instances found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('switches to snapshots tab', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(NeptunePage);
		const snapshotsTab = screen.getByRole('button', { name: /Snapshots/ });
		mockSend.mockResolvedValue({ DBClusterSnapshots: [] });
		await fireEvent.click(snapshotsTab);
		await waitFor(() => {
			expect(screen.getByText('No snapshots found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});
});
