import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import BackupPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getBackupClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('AWS Backup Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
		render(BackupPage);
		expect(screen.getByText('AWS Backup')).toBeInTheDocument();
	});

	it('shows three tabs', () => {
		mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
		render(BackupPage);
		expect(screen.getByText('Backup Plans')).toBeInTheDocument();
		expect(screen.getByText('Backup Vaults')).toBeInTheDocument();
		expect(screen.getByText('Backup Jobs')).toBeInTheDocument();
	});

	it('displays backup plans', async () => {
		mockSend.mockResolvedValueOnce({
			BackupPlansList: [
				{
					BackupPlanId: 'plan-1',
					BackupPlanName: 'daily-backup',
					VersionId: 'v1',
					CreationDate: new Date('2024-01-01')
				}
			]
		});

		render(BackupPage);

		await waitFor(() => {
			expect(screen.getByText('daily-backup')).toBeInTheDocument();
		});
	});

	it('shows create plan modal', async () => {
		mockSend.mockResolvedValueOnce({ BackupPlansList: [] });
		render(BackupPage);
		await fireEvent.click(screen.getByText('Create Plan'));
		expect(screen.getByText('Create Backup Plan')).toBeInTheDocument();
	});

	it('switches to vaults tab', async () => {
		mockSend
			.mockResolvedValueOnce({ BackupPlansList: [] })
			.mockResolvedValueOnce({ BackupVaultList: [] });
		render(BackupPage);
		await fireEvent.click(screen.getByText('Backup Vaults'));
		await waitFor(() => {
			expect(screen.getByText('No backup vaults found')).toBeInTheDocument();
		});
	});
});
