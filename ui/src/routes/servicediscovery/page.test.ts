import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import ServiceDiscoveryPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getServiceDiscoveryClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Service Discovery Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Namespaces: [], Services: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getByText('AWS Cloud Map')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Namespaces: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getAllByText('Namespaces')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Namespaces: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no namespaces', async () => {
		mockSend.mockResolvedValue({ Namespaces: [], Services: [] });
		render(ServiceDiscoveryPage);
		await waitFor(() => {
			expect(screen.getByText(/no namespaces/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded namespaces', async () => {
		mockSend.mockResolvedValue({
			Namespaces: [{ Name: 'my-namespace', Id: 'ns-123', Type: 'DNS_PRIVATE', ServiceCount: 3 }],
			Services: []
		});
		render(ServiceDiscoveryPage);
		await waitFor(() => {
			expect(screen.getByText('my-namespace')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Namespaces: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Services tab', () => {
		mockSend.mockResolvedValue({ Namespaces: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getByText('Services')).toBeInTheDocument();
	});

	it('shows DNS Namespaces stat', () => {
		mockSend.mockResolvedValue({ Namespaces: [] });
		render(ServiceDiscoveryPage);
		expect(screen.getByText('DNS Namespaces')).toBeInTheDocument();
	});
});
