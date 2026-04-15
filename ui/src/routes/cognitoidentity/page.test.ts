import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import CognitoIdentityPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCognitoIdentityClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Cognito Identity Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ IdentityPools: [] });
		render(CognitoIdentityPage);
		expect(screen.getByText('Cognito Identity')).toBeInTheDocument();
	});

	it('shows create pool button', () => {
		mockSend.mockResolvedValueOnce({ IdentityPools: [] });
		render(CognitoIdentityPage);
		expect(screen.getByText('Create Pool')).toBeInTheDocument();
	});

	it('displays identity pools', async () => {
		mockSend.mockResolvedValueOnce({
			IdentityPools: [
				{
					IdentityPoolId: 'us-east-1:abc-123',
					IdentityPoolName: 'my-app-pool'
				}
			]
		});

		render(CognitoIdentityPage);

		await waitFor(() => {
			expect(screen.getByText('my-app-pool')).toBeInTheDocument();
		});
	});

	it('shows create modal', async () => {
		mockSend.mockResolvedValueOnce({ IdentityPools: [] });
		render(CognitoIdentityPage);
		await fireEvent.click(screen.getByText('Create Pool'));
		expect(screen.getByText('Create Identity Pool')).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValueOnce({ IdentityPools: [] });
		render(CognitoIdentityPage);
		await waitFor(() => {
			expect(screen.getByText('No identity pools found')).toBeInTheDocument();
		});
	});
});
