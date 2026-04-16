import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import DirectConnectPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getDirectConnectClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Direct Connect Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ connections: [], virtualInterfaces: [], directConnectGateways: [] });
		render(DirectConnectPage);
		expect(screen.getByText('AWS Direct Connect')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ connections: [] });
		render(DirectConnectPage);
		expect(screen.getByText('Connections')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ connections: [] });
		render(DirectConnectPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ connections: [], virtualInterfaces: [], directConnectGateways: [] });
		render(DirectConnectPage);
		await waitFor(() => {
			expect(screen.getByText(/no connections/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded connections', async () => {
		mockSend.mockResolvedValue({
			connections: [{ connectionName: 'my-dx', bandwidth: '1Gbps', location: 'EqDC2', connectionState: 'available' }],
			virtualInterfaces: [], directConnectGateways: []
		});
		render(DirectConnectPage);
		await waitFor(() => {
			expect(screen.getByText('my-dx')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ connections: [] });
		render(DirectConnectPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Virtual Interfaces tab', () => {
		mockSend.mockResolvedValue({ connections: [] });
		render(DirectConnectPage);
		expect(screen.getByText('Virtual Interfaces')).toBeInTheDocument();
	});

	it('shows Gateways tab', () => {
		mockSend.mockResolvedValue({ connections: [] });
		render(DirectConnectPage);
		expect(screen.getByText('Gateways')).toBeInTheDocument();
	});
});
