import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import FSxPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getFSxClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('FSx Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ FileSystems: [], Backups: [] });
		render(FSxPage);
		expect(screen.getAllByText('Amazon FSx')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(FSxPage);
		expect(screen.getAllByText('File Systems')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(FSxPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no file systems', async () => {
		mockSend.mockResolvedValue({ FileSystems: [], Backups: [] });
		render(FSxPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no file systems/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded file systems', async () => {
		mockSend.mockResolvedValue({
			FileSystems: [{ FileSystemId: 'fs-abc123', FileSystemType: 'WINDOWS', StorageCapacity: 1024, Lifecycle: 'AVAILABLE' }],
			Backups: []
		});
		render(FSxPage);
		await waitFor(() => {
			expect(screen.getAllByText('fs-abc123')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(FSxPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Backups tab', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(FSxPage);
		expect(screen.getAllByText('Backups')[0]).toBeInTheDocument();
	});

	it('shows Available stat', () => {
		mockSend.mockResolvedValue({ FileSystems: [] });
		render(FSxPage);
		expect(screen.getAllByText('Available')[0]).toBeInTheDocument();
	});
});
