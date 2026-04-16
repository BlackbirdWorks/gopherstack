import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import NetworkManagerPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getNetworkManagerClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Network Manager Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByText('AWS Network Manager')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByText('Global Networks')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no networks', async () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		await waitFor(() => {
			expect(screen.getByText(/no global networks/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded networks', async () => {
		mockSend.mockResolvedValue({
			GlobalNetworks: [{ GlobalNetworkId: 'global-net-123', State: 'AVAILABLE', Description: 'My network' }]
		});
		render(NetworkManagerPage);
		await waitFor(() => {
			expect(screen.getByText('global-net-123')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Devices tab', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByText('Devices')).toBeInTheDocument();
	});

	it('shows Links tab', () => {
		mockSend.mockResolvedValue({ GlobalNetworks: [] });
		render(NetworkManagerPage);
		expect(screen.getByText('Links')).toBeInTheDocument();
	});
});
