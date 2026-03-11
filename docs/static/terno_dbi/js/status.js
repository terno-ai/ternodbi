// Status checking script removed to prioritize static branding
/*
document.addEventListener('DOMContentLoaded', () => {
    checkHealth();
});

async function checkHealth() {
    const badge = document.getElementById('status-badge');
    if (!badge) return;
    const dot = badge.querySelector('.status-dot');
    const text = badge.querySelector('span:last-child');

    try {
        const response = await fetch('/api/query/health/');
        if (response.ok) {
            const data = await response.json();
            badge.classList.add('online');
            text.textContent = `All Systems Operational (v${data.version})`;
        } else {
            throw new Error('Health check failed');
        }
    } catch (error) {
        console.error('Health check error:', error);
        badge.classList.remove('online');
        text.textContent = 'System Offline';
    }
}
*/

// Copy to clipboard functionality
document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        const code = btn.parentElement.querySelector('code').textContent;
        navigator.clipboard.writeText(code);

        const originalText = btn.textContent;
        btn.textContent = 'Copied!';
        setTimeout(() => {
            btn.textContent = originalText;
        }, 2000);
    });
});
