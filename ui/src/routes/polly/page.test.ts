import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import PollyPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getPollyClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Polly Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Voices: [], Lexicons: [], SynthesisTasks: [] });
		render(PollyPage);
		expect(screen.getAllByText('Amazon Polly')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getAllByText('Voices')[0]).toBeInTheDocument();
	});

	it('shows text-to-speech demo section', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getAllByText('Text-to-Speech Demo')[0]).toBeInTheDocument();
	});

	it('shows synthesize button', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getAllByText('Synthesize & Play')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no voices', async () => {
		mockSend.mockResolvedValue({ Voices: [], Lexicons: [], SynthesisTasks: [] });
		render(PollyPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no voices/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded voices', async () => {
		mockSend.mockResolvedValue({
			Voices: [{ Name: 'Joanna', Gender: 'Female', LanguageCode: 'en-US', LanguageName: 'US English', SupportedEngines: ['standard', 'neural'] }],
			Lexicons: [], SynthesisTasks: []
		});
		render(PollyPage);
		await waitFor(() => {
			expect(screen.getAllByText('Joanna')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows Lexicons tab', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getAllByText('Lexicons')[0]).toBeInTheDocument();
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Voices: [] });
		render(PollyPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});
});
