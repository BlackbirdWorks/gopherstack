// Gopherstack UI JavaScript — Flowbite + HTMX

// ── Toast notifications ────────────────────────────────────────
function showToast(message, type) {
    type = type || 'info';
    const colors = {
        success: 'bg-green-100 text-green-800 dark:bg-green-800 dark:text-green-200',
        error:   'bg-red-100 text-red-800 dark:bg-red-800 dark:text-red-200',
        warning: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-800 dark:text-yellow-200',
        info:    'bg-blue-100 text-blue-800 dark:bg-blue-800 dark:text-blue-200',
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

    requestAnimationFrame(function() {
        toast.classList.remove('opacity-0', '-translate-y-2');
        toast.classList.add('opacity-100', 'translate-y-0');
    });
    setTimeout(function() {
        toast.classList.add('opacity-0', '-translate-y-2');
        setTimeout(function() { toast.remove(); }, 300);
    }, 5000);
}

// ── Copy to clipboard ──────────────────────────────────────────
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function() {
        showToast('Copied to clipboard!', 'success');
    }).catch(function(err) {
        showToast('Failed to copy', 'error');
        console.error('Copy failed:', err);
    });
}

// ── Format JSON ────────────────────────────────────────────────
function formatJSON(obj) {
    return JSON.stringify(obj, null, 2);
}

// ── Global confirm modal (native <dialog>) ─────────────────────
function confirmDelete(message) {
    return new Promise(function(resolve) {
        var modal = document.getElementById('global_confirm_modal');
        var confirmBtn = document.getElementById('global_confirm_proceed');
        var cancelBtn  = document.getElementById('global_confirm_cancel');
        var msgEl      = document.getElementById('global_confirm_message');
        msgEl.textContent = message || 'Are you sure you want to delete this?';

        var onConfirm = function() { modal.close(); cleanup(); resolve(true); };
        var onCancel  = function() { modal.close(); cleanup(); resolve(false); };
        var cleanup   = function() {
            confirmBtn.removeEventListener('click', onConfirm);
            cancelBtn.removeEventListener('click', onCancel);
        };
        confirmBtn.addEventListener('click', onConfirm);
        cancelBtn.addEventListener('click', onCancel);
        modal.showModal();
    });
}

// ── HTMX: intercept hx-confirm ────────────────────────────────
document.addEventListener('htmx:confirm', function(event) {
    if (!event.detail.question) return;
    event.preventDefault();

    var modal      = document.getElementById('global_confirm_modal');
    var confirmBtn = document.getElementById('global_confirm_proceed');
    var cancelBtn  = document.getElementById('global_confirm_cancel');
    var msgEl      = document.getElementById('global_confirm_message');
    msgEl.textContent = event.detail.question;

    var handleConfirm = function() { modal.close(); cleanup(); event.detail.issueRequest(true); };
    var handleCancel  = function() { modal.close(); cleanup(); };
    var cleanup = function() {
        confirmBtn.removeEventListener('click', handleConfirm);
        cancelBtn.removeEventListener('click', handleCancel);
    };
    confirmBtn.addEventListener('click', handleConfirm);
    cancelBtn.addEventListener('click', handleCancel);
    modal.showModal();
});

// ── HTMX events ───────────────────────────────────────────────
document.addEventListener('htmx:afterSwap', function() {
    console.log('HTMX swap completed');
});

document.addEventListener('htmx:responseError', function(event) {
    var xhr = event.detail.xhr;
    var trigger = xhr.getResponseHeader('HX-Trigger');
    if (trigger && trigger.includes('showToast')) return;
    showToast('Request failed. Please try again.', 'error');
    console.error('HTMX error:', event.detail);
});

document.addEventListener('htmx:sendError', function() {
    showToast('Network error. Please check your connection.', 'error');
});

document.body.addEventListener('showToast', function(event) {
    var detail = event.detail;
    if (detail && detail.message) showToast(detail.message, detail.type || 'info');
});

// ── S3 file-tree folder toggle ────────────────────────────────
function toggleFolder(element) {
    var children = element.nextElementSibling;
    if (children && children.classList.contains('folder-children')) {
        children.classList.toggle('hidden');
        var icon = element.querySelector('.folder-icon');
        if (icon) icon.textContent = children.classList.contains('hidden') ? '📁' : '📂';
    }
}

// ── Dark mode theme manager ───────────────────────────────────
var ThemeManager = {
    STORAGE_KEY: 'gopherstack-theme',

    getCurrentTheme: function() {
        var stored = localStorage.getItem(this.STORAGE_KEY);
        if (stored) return stored;
        return (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
    },

    setTheme: function(theme) {
        var html = document.documentElement;
        if (theme === 'dark') {
            html.classList.add('dark');
        } else {
            html.classList.remove('dark');
        }
        localStorage.setItem(this.STORAGE_KEY, theme);
        this.updateIcons(theme);
    },

    updateIcons: function(theme) {
        var dark  = document.getElementById('theme-icon-dark');
        var light = document.getElementById('theme-icon-light');
        if (theme === 'dark') {
            dark  && dark.classList.remove('hidden');
            light && light.classList.add('hidden');
        } else {
            dark  && dark.classList.add('hidden');
            light && light.classList.remove('hidden');
        }
    },

    toggleTheme: function() {
        var current = document.documentElement.classList.contains('dark') ? 'dark' : 'light';
        this.setTheme(current === 'dark' ? 'light' : 'dark');
    },

    init: function() {
        this.setTheme(this.getCurrentTheme());
        var btn = document.getElementById('theme-toggle');
        if (btn) btn.addEventListener('click', this.toggleTheme.bind(this));
        if (window.matchMedia) {
            window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function(e) {
                if (!localStorage.getItem(ThemeManager.STORAGE_KEY)) {
                    ThemeManager.setTheme(e.matches ? 'dark' : 'light');
                }
            });
        }
    }
};

document.addEventListener('DOMContentLoaded', function() {
    ThemeManager.init();
    console.log('Gopherstack UI loaded');
});
