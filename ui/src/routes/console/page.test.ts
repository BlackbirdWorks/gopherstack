import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/svelte';
import ConsolePage from './+page.svelte';

vi.mock('$lib/api/connect-client', () => {
	return {
		dashboardClient: {
			streamConsole: vi.fn().mockImplementation(async function* () {
				yield {
					request: {
						id: 'req-123',
						timestamp: { seconds: Math.floor(Date.now() / 1000) },
						durationMs: 45,
						method: 'GET',
						path: '/api/v1/test',
						status: 200,
						headers: { 'Content-Type': 'application/json' },
						body: '{"foo": "bar"}'
					}
				};
				await new Promise<void>(resolve => { setTimeout(resolve, 1000); });
			})
		}
	};
});

describe('Console Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
	});

	it('renders streamed requests and shows details on click', async () => {
		render(ConsolePage);
		
		expect(screen.getByText('Live API Console')).toBeInTheDocument();
		expect(screen.getByText('Waiting for API requests...')).toBeInTheDocument();
		
		await waitFor(() => {
			expect(screen.queryByText('Waiting for API requests...')).not.toBeInTheDocument();
		}, { timeout: 2000 });

		expect(screen.getByText('GET')).toBeInTheDocument();
		expect(screen.getByText('/api/v1/test')).toBeInTheDocument();
		expect(screen.getByText('200')).toBeInTheDocument();

		// click to open details
		const reqRow = screen.getByText('/api/v1/test');
		await fireEvent.click(reqRow);

		// Details should appear
		expect(screen.getByText('Request Details')).toBeInTheDocument();
		expect(screen.getByText('req-123')).toBeInTheDocument();
		expect(screen.getByText('{"foo": "bar"}')).toBeInTheDocument();
		
		// Unselect
		const closeBtn = screen.getByText('✕');
		await fireEvent.click(closeBtn);
		
		expect(screen.queryByText('req-123')).not.toBeInTheDocument();
	});
});
