import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import FirehosePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getFirehoseClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Firehose Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		expect(screen.getByText('Amazon Data Firehose')).toBeInTheDocument();
	});

	it('shows Create Stream button', () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		expect(screen.getByText('Create Stream')).toBeInTheDocument();
	});

	it('shows empty state when no streams', async () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		await waitFor(() => {
			expect(screen.getByText('No delivery streams found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded stream names', async () => {
		mockSend.mockResolvedValue({
			DeliveryStreamNames: ['clickstream-to-s3', 'logs-to-opensearch']
		});
		render(FirehosePage);
		await waitFor(() => {
			expect(screen.getByText('clickstream-to-s3')).toBeInTheDocument();
			expect(screen.getByText('logs-to-opensearch')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('opens create stream modal', async () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		await fireEvent.click(screen.getByText('Create Stream'));
		expect(screen.getByText('Stream Name')).toBeInTheDocument();
		expect(screen.getByText('S3 Bucket Name')).toBeInTheDocument();
	});

	it('cancels create stream modal', async () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		await fireEvent.click(screen.getByText('Create Stream'));
		await fireEvent.click(screen.getByText('Cancel'));
		expect(screen.queryByText('Stream Name')).not.toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		expect(screen.getByPlaceholderText('Search delivery streams...')).toBeInTheDocument();
	});

	it('navigates to stream detail on click', async () => {
		mockSend.mockResolvedValueOnce({ DeliveryStreamNames: ['my-stream'] });
		mockSend.mockResolvedValueOnce({
			DeliveryStreamDescription: {
				DeliveryStreamName: 'my-stream',
				DeliveryStreamStatus: 'ACTIVE',
				DeliveryStreamType: 'DirectPut',
				DeliveryStreamARN: 'arn:aws:firehose:us-east-1:123:deliverystream/my-stream',
				Destinations: [],
				CreateTimestamp: new Date()
			}
		});
		render(FirehosePage);
		await waitFor(() => expect(screen.getByText('my-stream')).toBeInTheDocument(), { timeout: 3000 });
		await fireEvent.click(screen.getByText('my-stream'));
		await waitFor(() => {
			expect(screen.getByText('Put Record')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ DeliveryStreamNames: [] });
		render(FirehosePage);
		expect(screen.getByText('Delivery Streams')).toBeInTheDocument();
		expect(screen.getByText('Selected Stream')).toBeInTheDocument();
		expect(screen.getByText('Destinations')).toBeInTheDocument();
		expect(screen.getByText('Status')).toBeInTheDocument();
	});
});
