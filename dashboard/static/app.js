// Gopherstack UI JavaScript — Flowbite + HTMX

// ── Toast notifications ────────────────────────────────────────
function showToast(message, type) {
    type = type || 'info';
    const colors = {
        success: 'bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-200',
        error: 'bg-red-100 text-red-800 dark:bg-red-800 dark:text-red-200',
        warning: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-800 dark:text-yellow-200',
        info: 'bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-200',
    };
    const icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
    const colorClass = colors[type] || colors.info;
    const icon = icons[type] || icons.info;

    const toast = document.createElement('div');
    toast.className = [
        'flex items-center gap-3 px-4 py-3 rounded-lg shadow-lg border pointer-events-auto',
        'text-sm font-medium transition-all duration-300 opacity-0 -translate-y-2',
        colorClass,
        'border-transparent',
    ].join(' ');
    toast.innerHTML = '<span>' + icon + '</span><span>' + message + '</span>';
    document.getElementById('global-alerts').appendChild(toast);

    requestAnimationFrame(function () {
        toast.classList.remove('opacity-0', '-translate-y-2');
        toast.classList.add('opacity-100', 'translate-y-0');
    });
    setTimeout(function () {
        toast.classList.add('opacity-0', '-translate-y-2');
        setTimeout(function () { toast.remove(); }, 300);
    }, 5000);
}

// ── Copy to clipboard ──────────────────────────────────────────
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function () {
        showToast('Copied to clipboard!', 'success');
    }).catch(function (err) {
        showToast('Failed to copy', 'error');
        console.error('Copy failed:', err);
    });
}

// ── Format JSON ────────────────────────────────────────────────
function formatJSON(obj) {
    return JSON.stringify(obj, null, 2);
}

// ── Global confirm modal (Flowbite) ───────────────────────────
function confirmDelete(message) {
    return new Promise(function (resolve) {
        const modalEl = document.getElementById('global_confirm_modal');
        const confirmBtn = document.getElementById('global_confirm_proceed');
        const cancelBtn = document.getElementById('global_confirm_cancel');
        const msgEl = document.getElementById('global_confirm_message');
        msgEl.textContent = message || 'Are you sure you want to delete this?';

        // Using Flowbite Modal API if available, otherwise fallback to hidden toggle
        let modal = null;
        if (window.Modal) {
            modal = new Modal(modalEl);
        }

        const show = () => {
            if (modal) modal.show();
            else modalEl.classList.remove('hidden');
        };
        const hide = () => {
            if (modal) modal.hide();
            else modalEl.classList.add('hidden');
        };

        const onConfirm = function () { hide(); cleanup(); resolve(true); };
        const onCancel = function () { hide(); cleanup(); resolve(false); };
        const cleanup = function () {
            confirmBtn.removeEventListener('click', onConfirm);
            cancelBtn.removeEventListener('click', onCancel);
        };
        confirmBtn.addEventListener('click', onConfirm);
        cancelBtn.addEventListener('click', onCancel);
        show();
    });
}

// ── HTMX: intercept hx-confirm ────────────────────────────────
document.addEventListener('htmx:confirm', function (event) {
    if (!event.detail.question) return;
    event.preventDefault();

    const modalEl = document.getElementById('global_confirm_modal');
    const confirmBtn = document.getElementById('global_confirm_proceed');
    const cancelBtn = document.getElementById('global_confirm_cancel');
    const msgEl = document.getElementById('global_confirm_message');
    msgEl.textContent = event.detail.question;

    let modal = null;
    if (window.Modal) {
        modal = new Modal(modalEl);
    }

    const show = () => {
        if (modal) modal.show();
        else modalEl.classList.remove('hidden');
    };
    const hide = () => {
        if (modal) modal.hide();
        else modalEl.classList.add('hidden');
    };

    const handleConfirm = function () { hide(); cleanup(); event.detail.issueRequest(true); };
    const handleCancel = function () { hide(); cleanup(); };
    const cleanup = function () {
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
    };
    confirmBtn.addEventListener('click', handleConfirm);
    cancelBtn.addEventListener('click', handleCancel);
    show();
});

// ── Sidebar Scroll Preservation ───────────────────────────────
let lastSidebarScroll = 0;

document.addEventListener('htmx:beforeRequest', function () {
    const sidebarList = document.querySelector('#sidebar .overflow-y-auto');
    if (sidebarList) {
        lastSidebarScroll = sidebarList.scrollTop;
    }
});

document.addEventListener('htmx:afterSwap', function () {
    console.log('HTMX swap completed');
    const sidebarList = document.querySelector('#sidebar .overflow-y-auto');
    if (sidebarList) {
        sidebarList.scrollTop = lastSidebarScroll;
    }
    setupGlobalSearch();
});

// ── Global Search ─────────────────────────────────────────────
function setupGlobalSearch() {
    const searchInput = document.getElementById('global-search');
    if (!searchInput) return;

    // Remove existing listener to avoid duplicates on htmx swaps
    searchInput.replaceWith(searchInput.cloneNode(true));
    const newSearchInput = document.getElementById('global-search');

    newSearchInput.addEventListener('input', function (e) {
        const query = e.target.value.toLowerCase();
        const sidebarItems = document.querySelectorAll('#sidebar li');

        let currentHeader = null;
        let anyVisibleInSection = false;

        sidebarItems.forEach(item => {
            if (item.classList.contains('text-xs')) { // This is a section header
                if (currentHeader) {
                    currentHeader.style.display = anyVisibleInSection ? '' : 'none';
                }
                currentHeader = item;
                anyVisibleInSection = false;
            } else {
                const a = item.querySelector('a');
                if (a) {
                    const text = a.textContent.toLowerCase();
                    if (text.includes(query)) {
                        item.style.display = '';
                        anyVisibleInSection = true;
                    } else {
                        item.style.display = 'none';
                    }
                }
            }
        });

        // Handle the last section
        if (currentHeader) {
            currentHeader.style.display = anyVisibleInSection ? '' : 'none';
        }
    });
}

document.addEventListener('keydown', function (e) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        const searchInput = document.getElementById('global-search');
        if (searchInput) {
            e.preventDefault();
            searchInput.focus();
        }
    }
});

document.addEventListener('DOMContentLoaded', function () {
    ThemeManager.init();
    setupGlobalSearch();
    console.log('Gopherstack UI loaded');
});
