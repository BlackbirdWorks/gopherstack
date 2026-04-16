import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import DocDBPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getDocDBClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('DocumentDB Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByText('Amazon DocumentDB')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByText('Clusters')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no clusters', async () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		await waitFor(() => {
			expect(screen.getByText(/no clusters/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded clusters', async () => {
		mockSend.mockResolvedValue({
			DBClusters: [{ DBClusterIdentifier: 'my-docdb', Status: 'available', EngineVersion: '5.0.0' }]
		});
		render(DocDBPage);
		await waitFor(() => {
			expect(screen.getByText('my-docdb')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Clusters tab', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByText('Clusters')).toBeInTheDocument();
	});

	it('shows Instances tab', () => {
		mockSend.mockResolvedValue({ DBClusters: [] });
		render(DocDBPage);
		expect(screen.getByText('Instances')).toBeInTheDocument();
	});
});
