import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import RAMPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getRAMClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('RAM Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ resourceShares: [], resources: [], principals: [] });
		render(RAMPage);
		expect(screen.getAllByText('AWS Resource Access Manager')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ resourceShares: [] });
		render(RAMPage);
		expect(screen.getAllByText('Resource Shares')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ resourceShares: [] });
		render(RAMPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no shares', async () => {
		mockSend.mockResolvedValue({ resourceShares: [], resources: [], principals: [] });
		render(RAMPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no resource shares/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded shares', async () => {
		mockSend.mockResolvedValue({
			resourceShares: [{ name: 'my-share', resourceShareArn: 'arn:aws:ram:us-east-1:123:resource-share/abc', status: 'ACTIVE' }],
			resources: [], principals: []
		});
		render(RAMPage);
		await waitFor(() => {
			expect(screen.getAllByText('my-share')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ resourceShares: [] });
		render(RAMPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Resources tab', () => {
		mockSend.mockResolvedValue({ resourceShares: [] });
		render(RAMPage);
		expect(screen.getAllByText('Resources')[0]).toBeInTheDocument();
	});

	it('shows Principals tab', () => {
		mockSend.mockResolvedValue({ resourceShares: [] });
		render(RAMPage);
		expect(screen.getAllByText('Principals')[0]).toBeInTheDocument();
	});
});
