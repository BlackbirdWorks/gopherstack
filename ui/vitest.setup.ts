import "@testing-library/jest-dom/vitest";
import "@testing-library/svelte/vitest";
import { vi } from 'vitest';

vi.mock('$app/state', () => {
	// A simple mock for $app/state. You can use object getters or explicit signals if using latest svelte.
	let currentVal = { params: { tableName: 'test-table', bucketName: 'test-bucket' } };
	return {
		page: currentVal 
	};
});

vi.mock('$app/navigation', () => {
    return {
        goto: vi.fn(),
        invalidateAll: vi.fn(),
    };
});

vi.mock('$app/environment', () => ({
	browser: true
}));
