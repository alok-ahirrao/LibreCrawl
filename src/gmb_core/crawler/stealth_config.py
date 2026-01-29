"""
Stealth Configuration Module for Playwright Anti-Detection

This module provides comprehensive anti-detection measures including:
- Human-like behavior simulation (mouse movements, delays, typing)
- Advanced browser fingerprint protection
- Request interception for tracker blocking
- Randomized browser profiles
"""

import random
import time
import math
from typing import List, Tuple, Optional


# ============================================================
# USER AGENT POOLS - Latest browser versions for high trust
# ============================================================

DESKTOP_USER_AGENTS = [
    # Chrome 131 (Latest stable)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    # Edge (High Trust on Windows)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
    # Firefox ESR
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
]

MOBILE_USER_AGENTS = [
    # iPhone Safari
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    # Android Chrome
    'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
]


# ============================================================
# VIEWPORT CONFIGURATIONS - Common resolutions
# ============================================================

DESKTOP_VIEWPORTS = [
    {'width': 1920, 'height': 1080},
    {'width': 1366, 'height': 768},
    {'width': 1536, 'height': 864},
    {'width': 1440, 'height': 900},
    {'width': 1680, 'height': 1050},
    {'width': 1280, 'height': 720},
    {'width': 2560, 'height': 1440},
]

MOBILE_VIEWPORTS = [
    {'width': 390, 'height': 844},   # iPhone 14
    {'width': 375, 'height': 812},   # iPhone X/11/12
    {'width': 414, 'height': 896},   # iPhone 11 Pro Max
    {'width': 360, 'height': 800},   # Android common
    {'width': 412, 'height': 915},   # Pixel 7
]


# ============================================================
# ENHANCED LAUNCH ARGUMENTS
# ============================================================

STEALTH_LAUNCH_ARGS = [
    # Core anti-detection
    '--disable-blink-features=AutomationControlled',
    '--disable-dev-shm-usage',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-infobars',
    
    # Disable automation flags
    '--disable-automation',
    '--disable-extensions',
    '--no-first-run',
    '--no-default-browser-check',
    
    # WebRTC leak prevention
    '--disable-webrtc-encryption',
    '--disable-webrtc-hw-decoding',
    '--disable-webrtc-hw-encoding',
    '--disable-webrtc-multiple-routes',
    '--disable-webrtc-hw-vp8-encoding',
    '--enforce-webrtc-ip-permission-check',
    '--force-webrtc-ip-handling-policy=disable_non_proxied_udp',
    
    # GPU / Rendering (avoid fingerprinting via GPU)
    '--disable-gpu',
    '--disable-accelerated-2d-canvas',
    '--disable-accelerated-video-decode',
    
    # Performance / Background
    '--disable-background-networking',
    '--disable-background-timer-throttling',
    '--disable-backgrounding-occluded-windows',
    '--disable-renderer-backgrounding',
    '--disable-component-update',
    '--disable-hang-monitor',
    '--disable-ipc-flooding-protection',
    '--disable-popup-blocking',
    '--disable-prompt-on-repost',
    '--disable-sync',
    
    # Privacy
    '--disable-features=TranslateUI',
    '--disable-features=BlinkGenPropertyTrees',
    '--disable-features=IsolateOrigins,site-per-process',
    '--disable-site-isolation-trials',
    
    # Misc
    '--metrics-recording-only',
    '--password-store=basic',
    '--use-mock-keychain',
    '--mute-audio',
]


# ============================================================
# TRACKER DOMAINS TO BLOCK
# ============================================================

BLOCKED_DOMAINS = [
    # Analytics and tracking
    'google-analytics.com',
    'googletagmanager.com',
    'doubleclick.net',
    'googlesyndication.com',
    'analytics.google.com',
    
    # Bot detection services
    'datadome.co',
    'perimeterx.net',
    'akamaihd.net',
    'imperva.com',
    'cloudflareinsights.com',
    
    # Other trackers
    'facebook.net',
    'fbcdn.net',
    'hotjar.com',
    'clarity.ms',
    'segment.io',
    'amplitude.com',
    'mixpanel.com',
]


# ============================================================
# COMPREHENSIVE STEALTH INIT SCRIPT
# ============================================================

STEALTH_INIT_SCRIPT = """
// ============================================================
// COMPREHENSIVE ANTI-DETECTION INIT SCRIPT
// Applies all necessary patches to appear as a real browser
// ============================================================

(() => {
    'use strict';

    // === 1. WEBDRIVER PROPERTY ===
    // Remove all traces of webdriver
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined,
        configurable: true
    });
    
    // Delete chrome automation variables
    const automationVars = [
        'cdc_adoQpoasnfa76pfcZLmcfl_Array',
        'cdc_adoQpoasnfa76pfcZLmcfl_Promise',
        'cdc_adoQpoasnfa76pfcZLmcfl_Symbol',
        '__webdriver_script_fn',
        '__webdriver_evaluate',
        '__selenium_evaluate',
        '__fxdriver_evaluate',
        '__driver_unwrapped',
        '__webdriver_unwrapped',
        '__driver_evaluate',
        '__selenium_unwrapped',
        '__fxdriver_unwrapped',
        'calledSelenium',
        '_WEBDRIVER_ELEM_CACHE',
        'ChromeDriverw',
        '__nightmarejs',
        '_selenium',
        '__$webdriverAsyncExecutor',
        'webdriver',
        '__lastWatirAlert',
        '__lastWatirConfirm',
        '__lastWatirPrompt',
    ];
    
    automationVars.forEach(prop => {
        try { delete window[prop]; } catch(e) {}
        try { delete document[prop]; } catch(e) {}
    });

    // === 2. PLUGINS (Realistic Chrome plugins) ===
    const makePluginArray = () => {
        const plugins = [
            {
                name: 'Chrome PDF Plugin',
                filename: 'internal-pdf-viewer',
                description: 'Portable Document Format',
                length: 1,
                item: (i) => i === 0 ? { type: 'application/pdf', description: 'PDF', suffixes: 'pdf' } : null,
                namedItem: (name) => name === 'application/pdf' ? { type: 'application/pdf' } : null,
            },
            {
                name: 'Chrome PDF Viewer',
                filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
                description: 'Portable Document Format',
                length: 1,
                item: (i) => i === 0 ? { type: 'application/pdf' } : null,
                namedItem: (name) => name === 'application/pdf' ? { type: 'application/pdf' } : null,
            },
            {
                name: 'Native Client',
                filename: 'internal-nacl-plugin',
                description: '',
                length: 2,
                item: (i) => null,
                namedItem: (name) => null,
            }
        ];
        
        const pluginArray = Object.create(PluginArray.prototype);
        plugins.forEach((p, i) => { pluginArray[i] = p; });
        pluginArray.length = plugins.length;
        pluginArray.item = (i) => plugins[i] || null;
        pluginArray.namedItem = (n) => plugins.find(p => p.name === n) || null;
        pluginArray.refresh = () => {};
        
        return pluginArray;
    };
    
    Object.defineProperty(navigator, 'plugins', {
        get: () => makePluginArray(),
        configurable: true
    });

    // === 3. MIME TYPES ===
    Object.defineProperty(navigator, 'mimeTypes', {
        get: () => {
            const mimes = Object.create(MimeTypeArray.prototype);
            mimes.length = 2;
            mimes[0] = { type: 'application/pdf', suffixes: 'pdf', description: 'Portable Document Format', enabledPlugin: navigator.plugins[0] };
            mimes[1] = { type: 'text/pdf', suffixes: 'pdf', description: 'Portable Document Format', enabledPlugin: navigator.plugins[0] };
            mimes.item = (i) => mimes[i] || null;
            mimes.namedItem = (n) => Array.from({length: mimes.length}, (_, i) => mimes[i]).find(m => m.type === n) || null;
            return mimes;
        },
        configurable: true
    });

    // === 4. LANGUAGES ===
    Object.defineProperty(navigator, 'languages', {
        get: () => ['en-US', 'en'],
        configurable: true
    });
    
    Object.defineProperty(navigator, 'language', {
        get: () => 'en-US',
        configurable: true
    });

    // === 5. PLATFORM ===
    Object.defineProperty(navigator, 'platform', {
        get: () => 'Win32',
        configurable: true
    });

    // === 6. HARDWARE CONCURRENCY (Randomized realistic values) ===
    const cores = [4, 6, 8, 12, 16][Math.floor(Math.random() * 5)];
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => cores,
        configurable: true
    });

    // === 7. DEVICE MEMORY ===
    const memory = [4, 8, 16, 32][Math.floor(Math.random() * 4)];
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => memory,
        configurable: true
    });

    // === 8. CHROME OBJECT (Essential for Chrome spoofing) ===
    if (!window.chrome) {
        window.chrome = {};
    }
    
    window.chrome.runtime = {
        connect: () => ({ onMessage: { addListener: () => {} }, postMessage: () => {}, onDisconnect: { addListener: () => {} } }),
        sendMessage: (ext, msg, cb) => { if (cb) setTimeout(() => cb(), 0); },
        onMessage: { addListener: () => {}, removeListener: () => {} },
        onConnect: { addListener: () => {} },
        id: undefined,
    };
    
    window.chrome.loadTimes = function() {
        return {
            requestTime: performance.timing.navigationStart / 1000,
            startLoadTime: performance.timing.navigationStart / 1000 + Math.random() * 0.5,
            commitLoadTime: performance.timing.responseStart / 1000 + Math.random() * 0.2,
            finishDocumentLoadTime: performance.timing.domContentLoadedEventEnd / 1000,
            finishLoadTime: performance.timing.loadEventEnd / 1000,
            firstPaintTime: performance.timing.domContentLoadedEventStart / 1000 + Math.random() * 0.5,
            firstPaintAfterLoadTime: 0,
            navigationType: 'Other',
            wasFetchedViaSpdy: false,
            wasNpnNegotiated: true,
            npnNegotiatedProtocol: 'h2',
            wasAlternateProtocolAvailable: false,
            connectionInfo: 'h2'
        };
    };
    
    window.chrome.csi = function() {
        return {
            startE: performance.timing.navigationStart,
            onloadT: performance.timing.loadEventEnd,
            pageT: Date.now() - performance.timing.navigationStart,
            tran: 15
        };
    };
    
    window.chrome.app = {
        isInstalled: false,
        InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
        RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' },
        getDetails: () => null,
        getIsInstalled: () => false,
        installState: (cb) => { if (cb) cb('not_installed'); return 'not_installed'; },
        runningState: () => 'cannot_run'
    };

    // === 9. PERMISSIONS API ===
    const originalQuery = navigator.permissions.query;
    navigator.permissions.query = function(parameters) {
        if (parameters.name === 'notifications') {
            return Promise.resolve({ state: Notification.permission, onchange: null });
        }
        return originalQuery.call(this, parameters);
    };

    // === 10. WEBGL FINGERPRINT PROTECTION ===
    const getParameterProxyHandler = {
        apply(target, thisArg, args) {
            const param = args[0];
            // WebGL Vendor
            if (param === 37445) return 'Intel Inc.';
            // WebGL Renderer
            if (param === 37446) return 'Intel Iris OpenGL Engine';
            // WebGL Version
            if (param === 7937) return 'WebKit WebGL';
            // WebGL Shading Language Version
            if (param === 35724) return 'WebGL GLSL ES 1.0 (OpenGL ES GLSL ES 1.0 Chromium)';
            return Reflect.apply(target, thisArg, args);
        }
    };
    
    try {
        WebGLRenderingContext.prototype.getParameter = new Proxy(
            WebGLRenderingContext.prototype.getParameter,
            getParameterProxyHandler
        );
        WebGL2RenderingContext.prototype.getParameter = new Proxy(
            WebGL2RenderingContext.prototype.getParameter,
            getParameterProxyHandler
        );
    } catch(e) {}

    // === 11. CANVAS FINGERPRINT NOISE ===
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {
        const context = this.getContext('2d');
        if (context && this.width <= 200 && this.height <= 200) {
            // Likely fingerprinting canvas - add subtle noise
            try {
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    // Very subtle noise that's imperceptible but changes fingerprint
                    imageData.data[i] = imageData.data[i] ^ ((Math.random() * 2) | 0);
                }
                context.putImageData(imageData, 0, 0);
            } catch(e) {}
        }
        return originalToDataURL.apply(this, arguments);
    };
    
    // Also protect getImageData
    const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    CanvasRenderingContext2D.prototype.getImageData = function(sx, sy, sw, sh) {
        const imageData = originalGetImageData.call(this, sx, sy, sw, sh);
        if (sw <= 200 && sh <= 200) {
            for (let i = 0; i < imageData.data.length; i += 100) {
                imageData.data[i] = imageData.data[i] ^ ((Math.random() * 2) | 0);
            }
        }
        return imageData;
    };

    // === 12. WEB AUDIO FINGERPRINT NOISE ===
    try {
        const originalGetChannelData = AudioBuffer.prototype.getChannelData;
        AudioBuffer.prototype.getChannelData = function(channel) {
            const array = originalGetChannelData.call(this, channel);
            // Add imperceptible noise
            for (let i = 0; i < array.length; i += 100) {
                array[i] = array[i] + (Math.random() * 0.0001 - 0.00005);
            }
            return array;
        };
        
        const originalCreateAnalyser = AudioContext.prototype.createAnalyser;
        AudioContext.prototype.createAnalyser = function() {
            const analyser = originalCreateAnalyser.call(this);
            const originalGetFloatFrequencyData = analyser.getFloatFrequencyData;
            analyser.getFloatFrequencyData = function(array) {
                originalGetFloatFrequencyData.call(this, array);
                for (let i = 0; i < array.length; i += 10) {
                    array[i] = array[i] + (Math.random() * 0.01);
                }
            };
            return analyser;
        };
    } catch(e) {}

    // === 13. NETWORK CONNECTION INFO ===
    Object.defineProperty(navigator, 'connection', {
        get: () => ({
            effectiveType: '4g',
            rtt: 50 + Math.floor(Math.random() * 50),
            downlink: 10 + Math.random() * 5,
            saveData: false,
            type: 'wifi',
            onchange: null,
            addEventListener: () => {},
            removeEventListener: () => {}
        }),
        configurable: true
    });

    // === 14. BATTERY API ===
    if ('getBattery' in navigator) {
        navigator.getBattery = () => Promise.resolve({
            charging: true,
            chargingTime: 0,
            dischargingTime: Infinity,
            level: 0.95 + Math.random() * 0.05,
            onchargingchange: null,
            onchargingtimechange: null,
            ondischargingtimechange: null,
            onlevelchange: null,
            addEventListener: () => {},
            removeEventListener: () => {}
        });
    }

    // === 15. SCREEN PROPERTIES ===
    const screenHeight = screen.height || 1080;
    const screenWidth = screen.width || 1920;
    
    Object.defineProperty(screen, 'availHeight', {
        get: () => screenHeight - 40,  // Taskbar
        configurable: true
    });
    
    Object.defineProperty(screen, 'availWidth', {
        get: () => screenWidth,
        configurable: true
    });
    
    Object.defineProperty(screen, 'colorDepth', {
        get: () => 24,
        configurable: true
    });
    
    Object.defineProperty(screen, 'pixelDepth', {
        get: () => 24,
        configurable: true
    });

    // === 16. HISTORY LENGTH (Make it look like real browsing) ===
    Object.defineProperty(history, 'length', {
        get: () => 2 + Math.floor(Math.random() * 5),
        configurable: true
    });

    // === 17. TOUCH SUPPORT (for desktop - should be false) ===
    Object.defineProperty(navigator, 'maxTouchPoints', {
        get: () => 0,
        configurable: true
    });

    // === 18. DO NOT TRACK ===
    Object.defineProperty(navigator, 'doNotTrack', {
        get: () => '1',
        configurable: true
    });

    // === 19. VENDOR ===
    Object.defineProperty(navigator, 'vendor', {
        get: () => 'Google Inc.',
        configurable: true
    });

    // === 20. PRODUCT SUB (Chrome specific) ===
    Object.defineProperty(navigator, 'productSub', {
        get: () => '20030107',
        configurable: true
    });

    console.log('[Stealth] Anti-detection patches applied successfully');
})();
"""


# ============================================================
# HUMAN-LIKE BEHAVIOR FUNCTIONS
# ============================================================

def random_delay(min_ms: float = 500, max_ms: float = 2000) -> None:
    """Sleep for a random human-like duration."""
    delay = random.uniform(min_ms, max_ms) / 1000.0
    time.sleep(delay)


def bezier_curve_points(start: Tuple[float, float], end: Tuple[float, float], 
                        num_points: int = 20) -> List[Tuple[float, float]]:
    """
    Generate points along a Bezier curve for natural mouse movement.
    Creates a curved path between two points that mimics human hand movement.
    """
    # Random control points for natural curve
    ctrl1 = (
        start[0] + (end[0] - start[0]) * 0.3 + random.uniform(-50, 50),
        start[1] + (end[1] - start[1]) * 0.3 + random.uniform(-50, 50)
    )
    ctrl2 = (
        start[0] + (end[0] - start[0]) * 0.7 + random.uniform(-50, 50),
        start[1] + (end[1] - start[1]) * 0.7 + random.uniform(-50, 50)
    )
    
    points = []
    for i in range(num_points + 1):
        t = i / num_points
        # Cubic Bezier formula
        x = (1-t)**3 * start[0] + 3*(1-t)**2*t * ctrl1[0] + 3*(1-t)*t**2 * ctrl2[0] + t**3 * end[0]
        y = (1-t)**3 * start[1] + 3*(1-t)**2*t * ctrl1[1] + 3*(1-t)*t**2 * ctrl2[1] + t**3 * end[1]
        points.append((int(x), int(y)))
    
    return points


def human_like_mouse_move(page, target_x: int, target_y: int) -> None:
    """
    Move mouse to target position with human-like Bezier curve trajectory.
    """
    try:
        # Get current position (estimate from center if unknown)
        current_x = random.randint(100, 300)
        current_y = random.randint(100, 300)
        
        # Generate Bezier path
        points = bezier_curve_points(
            (current_x, current_y),
            (target_x, target_y),
            num_points=random.randint(15, 30)
        )
        
        # Move through points with varying speed
        for i, (x, y) in enumerate(points):
            # Slow start, fast middle, slow end (ease-in-out)
            progress = i / len(points)
            if progress < 0.2 or progress > 0.8:
                delay = random.uniform(10, 30)
            else:
                delay = random.uniform(5, 15)
            
            page.mouse.move(x, y)
            time.sleep(delay / 1000)
            
    except Exception:
        # Fallback to simple move
        page.mouse.move(target_x, target_y, steps=random.randint(5, 15))


def human_like_scroll(page, direction: str = 'down', amount: int = None) -> None:
    """
    Perform human-like scrolling with variable speed and occasional pauses.
    """
    if amount is None:
        amount = random.randint(200, 500)
    
    if direction == 'up':
        amount = -amount
    
    try:
        # Scroll in chunks to simulate human behavior
        chunks = random.randint(3, 7)
        chunk_size = amount // chunks
        
        for i in range(chunks):
            # Variable scroll amount
            scroll_amount = chunk_size + random.randint(-20, 20)
            page.mouse.wheel(0, scroll_amount)
            
            # Variable delay between chunks
            time.sleep(random.uniform(0.05, 0.15))
            
        # Occasional longer pause after scrolling
        if random.random() < 0.3:
            time.sleep(random.uniform(0.3, 0.8))
            
    except Exception:
        # Fallback
        page.evaluate(f'window.scrollBy(0, {amount})')


def human_like_typing(page, selector: str, text: str) -> None:
    """
    Type text with human-like speed variations and occasional typos.
    """
    try:
        element = page.locator(selector)
        element.click()
        random_delay(100, 300)
        
        for char in text:
            # Variable typing speed (faster for common letters)
            if char in 'etaoinshrdlu':
                delay = random.uniform(50, 120)
            else:
                delay = random.uniform(80, 200)
            
            # Occasional slight pause (thinking)
            if random.random() < 0.05:
                time.sleep(random.uniform(0.2, 0.5))
            
            page.keyboard.type(char)
            time.sleep(delay / 1000)
            
    except Exception:
        # Fallback to direct fill
        page.fill(selector, text)


def get_random_profile(device: str = 'desktop') -> dict:
    """
    Generate a random but consistent browser profile.
    """
    if device.lower() == 'mobile':
        user_agent = random.choice(MOBILE_USER_AGENTS)
        viewport = random.choice(MOBILE_VIEWPORTS)
        has_touch = True
    else:
        user_agent = random.choice(DESKTOP_USER_AGENTS)
        viewport = random.choice(DESKTOP_VIEWPORTS)
        has_touch = False
    
    return {
        'user_agent': user_agent,
        'viewport': viewport,
        'has_touch': has_touch,
        'color_scheme': 'light',
        'reduced_motion': 'no-preference',
        'locale': 'en-US',
        'timezone_id': 'America/New_York',
    }


def should_block_request(url: str) -> bool:
    """
    Check if a request should be blocked based on domain.
    """
    url_lower = url.lower()
    for domain in BLOCKED_DOMAINS:
        if domain in url_lower:
            return True
    return False


def setup_request_interception(context) -> None:
    """
    Set up request interception to block tracking domains.
    """
    def handle_route(route):
        url = route.request.url
        
        # Block known trackers
        if should_block_request(url):
            route.abort()
            return
        
        # Block heavy resources in fast mode (optional)
        resource_type = route.request.resource_type
        if resource_type in ['font', 'media']:
            route.abort()
            return
        
        route.fallback()
    
    try:
        context.route('**/*', handle_route)
    except Exception:
        pass


def apply_stealth_to_context(context) -> None:
    """
    Apply all stealth measures to a browser context.
    """
    try:
        # Apply comprehensive init script
        context.add_init_script(STEALTH_INIT_SCRIPT)
    except Exception:
        pass
