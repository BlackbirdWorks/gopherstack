import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import SettingsPage from './+page.svelte';

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

const mockLocalStorage = (() => {
	let store: Record<string, string> = {};
	return {
		getItem(key: string) { return store[key] || null; },
		setItem(key: string, value: string) { store[key] = value.toString(); },
		clear() { store = {}; }
	};
})();
Object.defineProperty(window, 'localStorage', { value: mockLocalStorage });

describe('Settings Page', () => {
	beforeEach(() => {
		window.localStorage.clear();
		vi.clearAllMocks();
	});

	it('renders settings page elements correctly', () => {
		render(SettingsPage);
		expect(screen.getByText('Dashboard Settings')).toBeInTheDocument();
		expect(screen.getByLabelText('Auto-refresh Service Tables')).toBeInTheDocument();
		expect(screen.getByLabelText('Refresh Interval (seconds)')).toBeInTheDocument();
		expect(screen.getByLabelText('Max Console Entries Limit')).toBeInTheDocument();
		expect(screen.getByText('Save Settings')).toBeInTheDocument();
	});

	it('updates settings state via form interaction and saves to local storage', async () => {
		render(SettingsPage);
		
		const autoRefreshCheckbox = screen.getByLabelText('Auto-refresh Service Tables') as HTMLInputElement;
		const refreshInput = screen.getByLabelText('Refresh Interval (seconds)') as HTMLInputElement;
		const entriesInput = screen.getByLabelText('Max Console Entries Limit') as HTMLInputElement;
		const saveButton = screen.getByText('Save Settings');

		expect(autoRefreshCheckbox.checked).toBe(true);

		// Click auto refresh
		await fireEvent.click(autoRefreshCheckbox);
		
		// Change intervals
		await fireEvent.input(refreshInput, { target: { value: '10' } });
		await fireEvent.input(entriesInput, { target: { value: '200' } });

		expect(autoRefreshCheckbox.checked).toBe(false);
		expect(refreshInput.value).toBe('10');
		expect(entriesInput.value).toBe('200');

		// Save the form
		await fireEvent.click(saveButton);

		// Assert localStorage is called
		const savedData = window.localStorage.getItem('gopherstack_settings');
		expect(savedData).toBeTruthy();
		const parsed = JSON.parse(savedData!);
		expect(parsed.autoRefresh).toBe(false);
		expect(parsed.refreshInterval).toBe(10);
		expect(parsed.maxConsoleEntries).toBe(200);

		// Assert toast
		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalledWith('Settings saved successfully');
	});
});
