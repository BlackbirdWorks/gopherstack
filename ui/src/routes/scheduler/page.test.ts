import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import SchedulerPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSchedulerClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Scheduler Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Schedules: [], ScheduleGroups: [] });
		render(SchedulerPage);
		expect(screen.getByText('EventBridge Scheduler')).toBeInTheDocument();
	});

	it('shows tabs', () => {
		mockSend.mockResolvedValue({ Schedules: [], ScheduleGroups: [] });
		render(SchedulerPage);
		expect(screen.getByText('Schedules')).toBeInTheDocument();
		expect(screen.getByText('Schedule Groups')).toBeInTheDocument();
	});

	it('displays schedules', async () => {
		mockSend.mockResolvedValueOnce({
			Schedules: [
				{
					Name: 'my-schedule',
					GroupName: 'default',
					State: 'ENABLED',
					LastModificationDate: new Date('2024-01-15')
				}
			]
		}).mockResolvedValueOnce({ ScheduleGroups: [] });

		render(SchedulerPage);

		await waitFor(() => {
			expect(screen.getByText('my-schedule')).toBeInTheDocument();
		});
	});

	it('shows create schedule modal', async () => {
		mockSend.mockResolvedValue({ Schedules: [], ScheduleGroups: [] });
		render(SchedulerPage);
		await fireEvent.click(screen.getByText('Create Schedule'));
		expect(screen.getByText('Create Schedule', { selector: 'h2' })).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ Schedules: [], ScheduleGroups: [] });
		render(SchedulerPage);
		await waitFor(() => {
			expect(screen.getByText('No schedules found')).toBeInTheDocument();
		});
	});
});
