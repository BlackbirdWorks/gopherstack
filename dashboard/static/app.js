// Gopherstack UI JavaScript utilities

// Copy to clipboard functionality
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('Copied to clipboard!', 'success');
    }).catch(err => {
        showToast('Failed to copy', 'error');
        console.error('Copy failed:', err);
    });
}

// Show toast notification
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `alert alert-${type} fixed top-4 left-1/2 -translate-x-1/2 z-[1000] shadow-xl max-w-lg pointer-events-auto transition-all duration-300 opacity-0 transform -translate-y-4`;
    toast.innerHTML = `
        <div class="flex items-center gap-2">
            <span>${type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️'}</span>
            <span class="font-medium">${message}</span>
        </div>
    `;
    document.body.appendChild(toast);

    // Fade in
    requestAnimationFrame(() => {
        toast.classList.remove('opacity-0', '-translate-y-4');
        toast.classList.add('opacity-100', 'translate-y-0');
    });

    setTimeout(() => {
        // Fade out
        toast.classList.add('opacity-0', '-translate-y-4');
        setTimeout(() => toast.remove(), 300);
    }, 5000);
}

// Format JSON for display
function formatJSON(obj) {
    return JSON.stringify(obj, null, 2);
}

// Confirm delete action
function confirmDelete(message) {
    return confirm(message || 'Are you sure you want to delete this item?');
}

// Toggle folder in file tree
function toggleFolder(element) {
    const children = element.nextElementSibling;
    if (children && children.classList.contains('folder-children')) {
        children.classList.toggle('hidden');
        const icon = element.querySelector('.folder-icon');
        if (icon) {
            icon.textContent = children.classList.contains('hidden') ? '📁' : '📂';
        }
    }
}

// HTMX event listeners
document.addEventListener('htmx:afterSwap', (event) => {
    // Re-initialize any dynamic elements after HTMX swap
    console.log('HTMX swap completed');
});

document.addEventListener('htmx:responseError', (event) => {
    const xhr = event.detail.xhr;
    const trigger = xhr.getResponseHeader('HX-Trigger');
    if (trigger && trigger.includes('showToast')) {
        return; // Handled by custom showToast listener
    }
    showToast('Request failed. Please try again.', 'error');
    console.error('HTMX error:', event.detail);
});

document.addEventListener('htmx:sendError', (event) => {
    showToast('Network error. Please check your connection.', 'error');
    console.error('HTMX send error:', event.detail);
});

// Event listener for custom "showToast" trigger from HTMX responses
document.body.addEventListener('showToast', (event) => {
    const detail = event.detail;
    if (detail && detail.message) {
        showToast(detail.message, detail.type || 'info');
    }
});

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('Gopherstack UI loaded');
});
