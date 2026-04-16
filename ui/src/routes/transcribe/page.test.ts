import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import TranscribePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getTranscribeClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Transcribe Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [], Vocabularies: [] });
		render(TranscribePage);
		expect(screen.getByText('Amazon Transcribe')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [] });
		render(TranscribePage);
		expect(screen.getByText('Transcription Jobs')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [] });
		render(TranscribePage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no jobs', async () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [], Vocabularies: [] });
		render(TranscribePage);
		await waitFor(() => {
			expect(screen.getByText(/no transcription jobs/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded jobs', async () => {
		mockSend.mockResolvedValue({
			TranscriptionJobSummaries: [{ TranscriptionJobName: 'my-job', TranscriptionJobStatus: 'COMPLETED', LanguageCode: 'en-US' }],
			Vocabularies: []
		});
		render(TranscribePage);
		await waitFor(() => {
			expect(screen.getByText('my-job')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [] });
		render(TranscribePage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Vocabularies tab', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [] });
		render(TranscribePage);
		expect(screen.getByText('Vocabularies')).toBeInTheDocument();
	});

	it('shows Completed stat', () => {
		mockSend.mockResolvedValue({ TranscriptionJobSummaries: [] });
		render(TranscribePage);
		expect(screen.getByText('Completed')).toBeInTheDocument();
	});
});
