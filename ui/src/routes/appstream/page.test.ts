import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import AppStreamPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getAppStreamClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('AppStream Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Stacks: [], Fleets: [], Images: [] });
		render(AppStreamPage);
		expect(screen.getAllByText('Amazon AppStream 2.0')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getAllByText('Stacks')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ Stacks: [], Fleets: [], Images: [] });
		render(AppStreamPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no stacks/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded stacks', async () => {
		mockSend.mockResolvedValue({
			Stacks: [{ Name: 'my-stack', Description: 'Test stack', Arn: 'arn:aws:appstream:us-east-1:123:stack/my-stack' }],
			Fleets: [], Images: []
		});
		render(AppStreamPage);
		await waitFor(() => {
			expect(screen.getAllByText('my-stack')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Fleets tab', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getAllByText('Fleets')[0]).toBeInTheDocument();
	});

	it('shows Images tab', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getAllByText('Images')[0]).toBeInTheDocument();
	});
});
