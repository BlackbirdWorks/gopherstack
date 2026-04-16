import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import Route53ResolverPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getRoute53ResolverClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Route 53 Resolver Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [], ResolverRules: [] });
		render(Route53ResolverPage);
		expect(screen.getByText('Route 53 Resolver')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [] });
		render(Route53ResolverPage);
		expect(screen.getByText('Endpoints')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [] });
		render(Route53ResolverPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [], ResolverRules: [] });
		render(Route53ResolverPage);
		await waitFor(() => {
			expect(screen.getByText(/no resolver endpoints/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded endpoints', async () => {
		mockSend.mockResolvedValue({
			ResolverEndpoints: [{ Name: 'my-endpoint', Direction: 'INBOUND', Status: 'OPERATIONAL', IpAddressCount: 2 }],
			ResolverRules: []
		});
		render(Route53ResolverPage);
		await waitFor(() => {
			expect(screen.getByText('my-endpoint')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [] });
		render(Route53ResolverPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Rules tab', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [] });
		render(Route53ResolverPage);
		expect(screen.getByText('Rules')).toBeInTheDocument();
	});

	it('shows Inbound/Outbound stats', () => {
		mockSend.mockResolvedValue({ ResolverEndpoints: [] });
		render(Route53ResolverPage);
		expect(screen.getByText('Inbound')).toBeInTheDocument();
		expect(screen.getByText('Outbound')).toBeInTheDocument();
	});
});
