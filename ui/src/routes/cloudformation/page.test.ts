import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import CloudFormationPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCloudFormationClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('CloudFormation Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		expect(screen.getByText('CloudFormation')).toBeInTheDocument();
	});

	it('shows Create Stack button', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		expect(screen.getByText('Create Stack')).toBeInTheDocument();
	});

	it('shows empty state when no stacks', async () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		await waitFor(() => {
			expect(screen.getByText('No stacks found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded stacks', async () => {
		mockSend.mockResolvedValue({
			Stacks: [
				{ StackName: 'my-web-stack', StackStatus: 'CREATE_COMPLETE', CreationTime: new Date() },
				{ StackName: 'my-api-stack', StackStatus: 'UPDATE_IN_PROGRESS', CreationTime: new Date() }
			]
		});
		render(CloudFormationPage);
		await waitFor(() => {
			expect(screen.getByText('my-web-stack')).toBeInTheDocument();
			expect(screen.getByText('my-api-stack')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('opens create stack modal', async () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		await fireEvent.click(screen.getByText('Create Stack'));
		expect(screen.getByText('Stack Name')).toBeInTheDocument();
		expect(screen.getByText('Template Body (JSON/YAML)')).toBeInTheDocument();
	});

	it('cancels create stack modal', async () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		await fireEvent.click(screen.getByText('Create Stack'));
		const cancelBtn = screen.getByText('Cancel');
		await fireEvent.click(cancelBtn);
		expect(screen.queryByText('Stack Name')).not.toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Stacks: [] });
		render(CloudFormationPage);
		expect(screen.getByPlaceholderText('Search stacks...')).toBeInTheDocument();
	});

	it('shows stack status badges', async () => {
		mockSend.mockResolvedValue({
			Stacks: [{ StackName: 'test-stack', StackStatus: 'CREATE_COMPLETE', CreationTime: new Date() }]
		});
		render(CloudFormationPage);
		await waitFor(() => {
			expect(screen.getByText('CREATE_COMPLETE')).toBeInTheDocument();
		}, { timeout: 3000 });
	});
});
