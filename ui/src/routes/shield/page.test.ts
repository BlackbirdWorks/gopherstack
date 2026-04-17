import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ShieldPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getShieldClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('Shield Advanced Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ Protections: [] });
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'ACTIVE' });

		render(ShieldPage);

		expect(screen.getByText('Shield Advanced')).toBeInTheDocument();
	});

	it('shows Add Protection button', () => {
		mockSend.mockResolvedValueOnce({ Protections: [] });
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'ACTIVE' });

		render(ShieldPage);

		expect(screen.getByText('+ Add Protection')).toBeInTheDocument();
	});

	it('displays loaded protections', async () => {
		mockSend.mockResolvedValueOnce({
			Protections: [
				{ Id: 'prot-1', Name: 'my-alb-protection', ResourceArn: 'arn:aws:elasticloadbalancing::123:lb/app/my-alb/abc' },
				{ Id: 'prot-2', Name: 'my-cf-protection', ResourceArn: 'arn:aws:cloudfront::123:distribution/DEF' }
			]
		});
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'ACTIVE' });

		render(ShieldPage);

		await waitFor(() => {
			expect(screen.getByText('my-alb-protection')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('my-cf-protection')).toBeInTheDocument();
	});

	it('filters protections via search input', async () => {
		mockSend.mockResolvedValueOnce({
			Protections: [
				{ Id: 'prot-1', Name: 'alb-protection', ResourceArn: 'arn:1' },
				{ Id: 'prot-2', Name: 'cloudfront-protection', ResourceArn: 'arn:2' },
				{ Id: 'prot-3', Name: 'ec2-protection', ResourceArn: 'arn:3' }
			]
		});
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'ACTIVE' });

		render(ShieldPage);

		await waitFor(() => {
			expect(screen.getByText('alb-protection')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Filter protections...');
		await fireEvent.input(searchInput, { target: { value: 'cloudfront' } });

		await waitFor(() => {
			expect(screen.queryByText('alb-protection')).not.toBeInTheDocument();
		});
		expect(screen.getByText('cloudfront-protection')).toBeInTheDocument();
		expect(screen.queryByText('ec2-protection')).not.toBeInTheDocument();
	});

	it('shows inactive subscription banner when not ACTIVE', async () => {
		mockSend.mockResolvedValueOnce({ Protections: [] });
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'INACTIVE' });

		render(ShieldPage);

		await waitFor(() => {
			// When not ACTIVE there should be an activation prompt visible
			expect(screen.getByText('INACTIVE')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows empty state when no protections exist', async () => {
		mockSend.mockResolvedValueOnce({ Protections: [] });
		mockSend.mockResolvedValueOnce({});
		mockSend.mockResolvedValueOnce({ SubscriptionState: 'ACTIVE' });

		render(ShieldPage);

		await waitFor(() => {
			expect(screen.getByText(/No resources are currently registered/)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('forbidden'));

		render(ShieldPage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
