document.addEventListener('DOMContentLoaded', () => {
    const mobileMenuBtn = document.querySelector('.mobile-menu-btn');
    const mobileNavOverlay = document.querySelector('.mobile-nav-overlay');
    const mobileNavDrawer = document.querySelector('.mobile-nav-drawer');
    const body = document.body;

    if (!mobileMenuBtn || !mobileNavOverlay || !mobileNavDrawer) return;

    function toggleMenu() {
        const isOpen = mobileNavDrawer.classList.contains('open');
        
        if (isOpen) {
            mobileNavDrawer.classList.remove('open');
            mobileNavOverlay.classList.remove('open');
            // Change icon back to hamburger
            mobileMenuBtn.innerHTML = `
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="3" y1="12" x2="21" y2="12"></line>
                    <line x1="3" y1="6" x2="21" y2="6"></line>
                    <line x1="3" y1="18" x2="21" y2="18"></line>
                </svg>`;
            body.style.overflow = ''; // Restore scrolling
        } else {
            mobileNavDrawer.classList.add('open');
            mobileNavOverlay.classList.add('open');
            // Change icon to close (X)
            mobileMenuBtn.innerHTML = `
                <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <line x1="18" y1="6" x2="6" y2="18"></line>
                    <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>`;
            body.style.overflow = 'hidden'; // Prevent background scrolling
        }
    }

    mobileMenuBtn.addEventListener('click', toggleMenu);
    mobileNavOverlay.addEventListener('click', toggleMenu);

    // Close menu when clicking a link inside the drawer
    const drawerLinks = mobileNavDrawer.querySelectorAll('a');
    drawerLinks.forEach(link => {
        link.addEventListener('click', toggleMenu);
    });
});
