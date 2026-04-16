import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import EFSPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getEFSClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('EFS Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByText('Amazon EFS')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByText('File Systems')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no file systems', async () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		await waitFor(() => {
			expect(screen.getByText(/no file systems/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded file systems', async () => {
		mockSend.mockResolvedValue({
			FileSystems: [{ FileSystemId: 'fs-12345', LifeCycleState: 'available', PerformanceMode: 'generalPurpose', SizeInBytes: { Value: 1073741824 } }]
		});
		render(EFSPage);
		await waitFor(() => {
			expect(screen.getByText('fs-12345')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows File Systems tab', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByText('File Systems')).toBeInTheDocument();
	});

	it('shows Mount Targets tab', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(EFSPage);
		expect(screen.getByText('Mount Targets')).toBeInTheDocument();
	});
});
