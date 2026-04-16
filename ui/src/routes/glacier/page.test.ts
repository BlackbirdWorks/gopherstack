import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import GlacierPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getGlacierClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Glacier Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByText('Amazon S3 Glacier')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByText('Vaults')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByPlaceholderText(/search vaults/i)).toBeInTheDocument();
	});

	it('shows empty state when no vaults', async () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		await waitFor(() => {
			expect(screen.getByText(/no vaults/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded vaults', async () => {
		mockSend.mockResolvedValue({
			VaultList: [{ VaultName: 'my-archive-vault', SizeInBytes: 1073741824, NumberOfArchives: 5 }]
		});
		render(GlacierPage);
		await waitFor(() => {
			expect(screen.getByText('my-archive-vault')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Archives stat', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByText('Archives')).toBeInTheDocument();
	});

	it('shows Total Storage stat', () => {
		mockSend.mockResolvedValue({ VaultList: [] });
		render(GlacierPage);
		expect(screen.getByText('Total Storage')).toBeInTheDocument();
	});
});
