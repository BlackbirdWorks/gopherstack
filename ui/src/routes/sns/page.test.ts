import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import SNSPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSNSClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('SNS Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Topics: [], Attributes: {} });
		render(SNSPage);
		expect(screen.getByText('SNS Topics')).toBeInTheDocument();
	});

	it('shows Create Topic button', () => {
		mockSend.mockResolvedValue({ Topics: [] });
		render(SNSPage);
		expect(screen.getByText('Create Topic')).toBeInTheDocument();
	});

	it('shows empty state when no topics', async () => {
		mockSend.mockResolvedValue({ Topics: [] });
		render(SNSPage);
		await waitFor(() => {
			expect(screen.getByText('No topics found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded topics', async () => {
		mockSend.mockResolvedValueOnce({
			Topics: [
				{ TopicArn: 'arn:aws:sns:us-east-1:123:my-alerts' },
				{ TopicArn: 'arn:aws:sns:us-east-1:123:notifications.fifo' }
			]
		});
		// GetTopicAttributes for each
		mockSend.mockResolvedValue({ Attributes: { SubscriptionsConfirmed: '2' } });
		render(SNSPage);
		await waitFor(() => {
			expect(screen.getByText('my-alerts')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('notifications.fifo')).toBeInTheDocument();
	});

	it('filters topics via search', async () => {
		mockSend.mockResolvedValueOnce({
			Topics: [
				{ TopicArn: 'arn:aws:sns:us-east-1:123:billing-alerts' },
				{ TopicArn: 'arn:aws:sns:us-east-1:123:order-events' }
			]
		});
		mockSend.mockResolvedValue({ Attributes: {} });
		render(SNSPage);
		await waitFor(() => {
			expect(screen.getByText('billing-alerts')).toBeInTheDocument();
		}, { timeout: 3000 });
		const searchInput = screen.getByPlaceholderText('Search topics...');
		await fireEvent.input(searchInput, { target: { value: 'billing' } });
		await waitFor(() => {
			expect(screen.queryByText('order-events')).not.toBeInTheDocument();
		});
		expect(screen.getByText('billing-alerts')).toBeInTheDocument();
	});

	it('opens create modal', async () => {
		mockSend.mockResolvedValue({ Topics: [] });
		render(SNSPage);
		await fireEvent.click(screen.getByText('Create Topic'));
		await waitFor(() => {
			expect(screen.getByText('Create Topic', { selector: 'h2' })).toBeInTheDocument();
		});
		expect(screen.getByPlaceholderText('e.g. my-notifications')).toBeInTheDocument();
	});

	it('closes create modal on cancel', async () => {
		mockSend.mockResolvedValue({ Topics: [] });
		render(SNSPage);
		await fireEvent.click(screen.getByText('Create Topic'));
		await waitFor(() => {
			expect(screen.getByPlaceholderText('e.g. my-notifications')).toBeInTheDocument();
		});
		await fireEvent.click(screen.getByText('Cancel'));
		await waitFor(() => {
			expect(screen.queryByPlaceholderText('e.g. my-notifications')).not.toBeInTheDocument();
		});
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('access denied'));
		render(SNSPage);
		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
