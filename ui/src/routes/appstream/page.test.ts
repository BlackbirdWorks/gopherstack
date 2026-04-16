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
		expect(screen.getByText('Amazon AppStream 2.0')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getByText('Stacks')).toBeInTheDocument();
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
			expect(screen.getByText(/no stacks/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded stacks', async () => {
		mockSend.mockResolvedValue({
			Stacks: [{ Name: 'my-stack', Description: 'Test stack', Arn: 'arn:aws:appstream:us-east-1:123:stack/my-stack' }],
			Fleets: [], Images: []
		});
		render(AppStreamPage);
		await waitFor(() => {
			expect(screen.getByText('my-stack')).toBeInTheDocument();
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
		expect(screen.getByText('Fleets')).toBeInTheDocument();
	});

	it('shows Images tab', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(AppStreamPage);
		expect(screen.getByText('Images')).toBeInTheDocument();
	});
});
