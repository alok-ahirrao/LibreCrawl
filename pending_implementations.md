# 📋 Crawler Implementation Plan (Full Stack Integrated)

This document outlines **all** missing technical SEO checks. Each item contains instructions for **both** the Python Backend (Crawler) and Next.js Frontend (Dashboard).
**Usage:** Feed one complete block to an AI assistant to implement the feature end-to-end.

---

## 🧱 Block 1: Critical Crawling & Indexing Foundation ✅ COMPLETED

### 1. X-Robots-Tag Header Support ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `crawler.py` extracts `X-Robots-Tag` header (lines 907, 1027). `issue_detector.py` checks for noindex/nofollow and includes source in details (lines 420-458).
*   **🖥️ Frontend:** Issue details already show "Source: HTTP Header (X-Robots-Tag)" vs "Source: HTML Meta Tag" in the issue description.

### 2. Infinite Crawl Trap Detection ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `link_manager.py` implements trap detection with URL signature patterns (lines 20-21, 69-92, 298-301).
*   **🖥️ Frontend:** `CrawlTrapsTable.tsx` displays skipped patterns with pattern, count, and example URL.

### 3. Orphan URL Detection ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `link_manager.py` tracks `source_pages` for inlink counting (lines 17, 63-67, 262-265).
*   **🖥️ Frontend:** `PageTable.tsx` calculates inlinks, shows "Orphan" badge, has "Show Orphan Pages Only" filter in dropdown.

### 4. Advanced Robots.txt Parsing ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `crawler.py` has `_validate_robots_txt()` for syntax validation. Data persisted via `crawl_db.py`.
*   **🖥️ Frontend:** `RobotsTxtValidator.tsx` shows raw content with line numbers, error highlighting, and validation report panel.

---

## 🚦 Block 2: HTTP Status, Sitemaps & Redirects

### 1. Advanced Sitemap Validation
*   **Missing Feature:** Validating URLs *inside* sitemaps (Non-200, Noindex, Non-Canonical).
*   **🐍 Backend Implementation:**
    *   **Files:** `sitemap_parser.py`, `issue_detector.py`
    *   **Logic:** Cross-reference crawled URL status with its presence in Sitemap. Flag "Dirty Sitemap" issues.
*   **🖥️ Frontend Implementation:**
    *   **New Component:** `SitemapHealth.tsx`
    *   **Logic:** Visual breakdown: "Total URLs in Sitemap" vs "Valid (200)" vs "Errors (404/Nointo)". Pie chart preferred.

### 2. Soft 404 Detection
*   **Missing Feature:** detecting pages that return `200 OK` but are actually errors ("Page Not Found" text).
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Check if `status==200` AND (Title/H1 matches "Not Found|Error 404" OR Content < 500 bytes).
*   **🖥️ Frontend Implementation:**
    *   **Update:** `PageTable.tsx`
    *   **Logic:** Add a specialized "Soft 404" badge (Distinct from "404"). tooltip explaining "Returns 200 OK but appears broken".

### 3. Redirect Insights (Chains & Loops)
*   **Missing Feature:** Redirect chains >1 hop or loops are not explicitly reported as issues.
*   **🐍 Backend Implementation:**
    *   **Files:** `crawler.py`, `issue_detector.py`
    *   **Logic:** Track redirect history list. Report issue if `len(history) > 1`.
*   **🖥️ Frontend Implementation:**
    *   **New Visualizer:** `RedirectChainView.tsx` (inside Issue Details).
    *   **Logic:** Render a stepper visualization: `Url A (301) --> Url B (301) --> Url C (200)`.

---

## 🔗 Block 3: Internal Link Hygiene

### 1. Internal Links to Redirects
*   **Missing Feature:** Linking internally to a URL that 301 redirects (wasting crawl budget).
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Iterate all link edges. If `Target.status` is 3xx, flag the `Source` page.
*   **🖥️ Frontend Implementation:**
    *   **Table Update:** In `PageDetails`, add a tab "Outlinks to Redirects".
    *   **Logic:** List exactly which links on the page are triggering redirects so the user can fix the `href`.

### 2. Broken Link Source Reporting
*   **Missing Feature:** Reporting *which page* contains the broken link (not just that the link is broken).
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Group 4xx/5xx errors by their `incoming_links` source list.
*   **🖥️ Frontend Implementation:**
    *   **New Drawer Feature:** "Broken Link Inspector".
    *   **Logic:** When clicking a 404 issue, show a table: "Found on these pages: [List of Source URLs]".

---

## 🛡️ Block 4: Security & Protocols

### 1. Mixed Content Detection
*   **Missing Feature:** HTTPS pages loading HTTP assets.
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Scan `images`, `scripts`, `links` for `http://` prefix when page is `https://`.
*   **🖥️ Frontend Implementation:**
    *   **Update:** `IssueDetails`
    *   **Logic:** Highlight the specific insecure asset URL (e.g., `http://unsafe-image.jpg`) in the issue description.

### 2. Security Headers Check
*   **Missing Feature:** HSTS, X-Frame-Options, CSP checks.
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Check `response_headers` dictionary for missing security keys.
*   **🖥️ Frontend Implementation:**
    *   **New Widget:** `SecurityScorecard.tsx`
    *   **Logic:** A checklist view: "SSL: ✅, HSTS: ❌, CSP: ⚠️".

---

## ⚡ Block 5: DOM, JS & Performance

### 1. DOM Complexity & Size
*   **Missing Feature:** Counting HTML nodes (>1500) & nesting depth.
*   **🐍 Backend Implementation:**
    *   **Files:** `seo_extractor.py`
    *   **Logic:** Use BeautifulSoup to count tags and max depth.
*   **🖥️ Frontend Implementation:**
    *   **New Chart:** `DOMComplexityChart.tsx`
    *   **Logic:** Bar chart showing "Node Count" vs "Recommended Limit (1500)".

### 2. JS Rendering Differences
*   **Feature:** Detecting content hidden behind JavaScipt.
*   **🐍 Backend Implementation:**
    *   **Files:** `crawler.py`
    *   **Logic:** Store both `raw_html_hash` and `rendered_html_hash`. If different, check for missing content.
*   **🖥️ Frontend Implementation:**
    *   **New Badge:** "Requires JS"
    *   **Logic:** Display this badge on pages where significant content was only found after rendering.

---

## 🌍 Block 6: International & Structured Data

### 1. Hreflang Validation
*   **Missing Feature:** Validating ISO codes and reciprocal returns.
*   **🐍 Backend Implementation:**
    *   **Files:** `issue_detector.py`
    *   **Logic:** Regex check `lang` codes. Check if Target URL points back to Source URL.
*   **🖥️ Frontend Implementation:**
    *   **New Component:** `HreflangMatrix.tsx`
    *   **Logic:** Database table view: `Page | Lang | Linked URL | Reciprocal Status (✅/❌)`.

### 2. AI Readiness (Schema)
*   **Missing Feature:** Validating Schema types (FAQ, LocalBusiness).
*   **🐍 Backend Implementation:**
    *   **Files:** `seo_extractor.py`, `issue_detector.py`
    *   **Logic:** Validate JSON-LD structure. Check for "FAQPage" presence.
*   **🖥️ Frontend Implementation:**
    *   **New Preview:** "Rich Result Preview"
    *   **Logic:** Render a mock "Google Search Result" card using the extracted Schema data (e.g., showing the FAQ dropdowns).
