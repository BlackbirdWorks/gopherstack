import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import SFNPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSFNClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('Step Functions Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ stateMachines: [] });

		render(SFNPage);

		expect(screen.getByText('Step Functions')).toBeInTheDocument();
	});

	it('displays loaded state machines', async () => {
		mockSend.mockResolvedValueOnce({
			stateMachines: [
				{ stateMachineArn: 'arn:1', name: 'order-workflow', type: 'STANDARD' },
				{ stateMachineArn: 'arn:2', name: 'data-pipeline', type: 'EXPRESS' }
			]
		});

		render(SFNPage);

		await waitFor(() => {
			expect(screen.getByText('order-workflow')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('data-pipeline')).toBeInTheDocument();
	});

	it('filters state machines via search input', async () => {
		mockSend.mockResolvedValueOnce({
			stateMachines: [
				{ stateMachineArn: 'arn:1', name: 'order-workflow', type: 'STANDARD' },
				{ stateMachineArn: 'arn:2', name: 'data-pipeline', type: 'EXPRESS' },
				{ stateMachineArn: 'arn:3', name: 'payment-flow', type: 'STANDARD' }
			]
		});

		render(SFNPage);

		await waitFor(() => {
			expect(screen.getByText('order-workflow')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Search workflows...');
		await fireEvent.input(searchInput, { target: { value: 'data' } });

		await waitFor(() => {
			expect(screen.queryByText('order-workflow')).not.toBeInTheDocument();
		});
		expect(screen.getByText('data-pipeline')).toBeInTheDocument();
		expect(screen.queryByText('payment-flow')).not.toBeInTheDocument();
	});

	it('selects a state machine and loads executions', async () => {
		mockSend.mockResolvedValueOnce({
			stateMachines: [
				{ stateMachineArn: 'arn:1', name: 'order-workflow', type: 'STANDARD' }
			]
		});
		// selectSM: DescribeStateMachineCommand
		mockSend.mockResolvedValueOnce({ stateMachineArn: 'arn:1', name: 'order-workflow', definition: '{}' });
		// ListExecutionsCommand
		mockSend.mockResolvedValueOnce({ executions: [] });

		render(SFNPage);

		await waitFor(() => {
			expect(screen.getByText('order-workflow')).toBeInTheDocument();
		}, { timeout: 3000 });

		await fireEvent.click(screen.getByText('order-workflow'));

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(3);
		}, { timeout: 3000 });
	});

	it('shows empty state when no state machines exist', async () => {
		mockSend.mockResolvedValueOnce({ stateMachines: [] });

		render(SFNPage);

		await waitFor(() => {
			expect(screen.getByText('No state machines found.')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('not authorized'));

		render(SFNPage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
