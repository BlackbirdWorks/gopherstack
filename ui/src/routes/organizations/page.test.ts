import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import OrganizationsPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getOrganizationsClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Organizations Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ Organization: null }).mockResolvedValueOnce({ Roots: [] });
		render(OrganizationsPage);
		expect(screen.getByText('AWS Organizations')).toBeInTheDocument();
	});

	it('shows all tabs', () => {
		mockSend.mockResolvedValueOnce({ Organization: null }).mockResolvedValueOnce({ Roots: [] });
		render(OrganizationsPage);
		expect(screen.getByText('Overview')).toBeInTheDocument();
		expect(screen.getByText('Accounts')).toBeInTheDocument();
		expect(screen.getByText('Org Units')).toBeInTheDocument();
		expect(screen.getByText('Policies')).toBeInTheDocument();
	});

	it('displays organization info', async () => {
		mockSend
			.mockResolvedValueOnce({
				Organization: {
					Id: 'o-abc123',
					MasterAccountId: '123456789012',
					MasterAccountEmail: 'master@example.com',
					FeatureSet: 'ALL'
				}
			})
			.mockResolvedValueOnce({ Roots: [] });

		render(OrganizationsPage);

		await waitFor(() => {
			expect(screen.getByText('o-abc123')).toBeInTheDocument();
		});
	});

	it('switches to accounts tab', async () => {
		mockSend
			.mockResolvedValueOnce({ Organization: null })
			.mockResolvedValueOnce({ Roots: [] })
			.mockResolvedValueOnce({ Accounts: [] });

		render(OrganizationsPage);
		await fireEvent.click(screen.getByText('Accounts'));
		await waitFor(() => {
			expect(screen.getByText('No accounts found')).toBeInTheDocument();
		});
	});

	it('shows create account modal', async () => {
		mockSend
			.mockResolvedValueOnce({ Organization: null })
			.mockResolvedValueOnce({ Roots: [] })
			.mockResolvedValueOnce({ Accounts: [] });

		render(OrganizationsPage);
		await fireEvent.click(screen.getByText('Accounts'));
		await waitFor(() => screen.getByText('Create Account'));
		await fireEvent.click(screen.getByText('Create Account'));
		expect(screen.getByText('Create Member Account')).toBeInTheDocument();
	});
});
