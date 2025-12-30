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

### 1. Advanced Sitemap Validation ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` has `detect_sitemap_issues()` method that cross-references crawled URL status with sitemap. Flags "Dirty Sitemap" issues (Non-200, Noindex, Non-Canonical, Redirects).
*   **🖥️ Frontend:** `SitemapHealth.tsx` displays pie chart breakdown: Valid (200) vs Errors (4xx/5xx) vs Noindex vs Non-Canonical vs Redirects.

### 2. Soft 404 Detection ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` checks if `status==200` AND Title/H1 contains error patterns (404, not found, etc). Creates "Soft 404" issues.
*   **🖥️ Frontend:** `PageTable.tsx` shows purple "Soft 404" badge with Ghost icon and tooltip. Filter option added.

### 3. Redirect Insights (Chains & Loops) ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `crawler.py` tracks `redirect_chain` from `response.history`. `issue_detector.py` flags loops (error), long chains >3 hops (warning), and 2-3 hop chains (info).
*   **🖥️ Frontend:** `RedirectChainView.tsx` displays stepper visualization: `URL (301) → URL (301) → URL (200)`.

---

## 🔗 Block 3: Internal Link Hygiene

### 1. Internal Links to Redirects ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` has `detect_links_to_redirects()` method that cross-references all links with target URL status codes. Flags pages with internal links pointing to redirecting URLs.
*   **🖥️ Frontend:** Issues appear in Issues tab as "Links: Internal Links to Redirects" with examples of problematic URLs.

### 2. Broken Link Source Reporting ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` has `detect_broken_link_sources()` that cross-references broken URLs (4xx/5xx) with their incoming links. Creates "Broken Link Sources" issues with source page list.
*   **🖥️ Frontend:** Issues appear in Issues tab showing "Broken Link Sources: 404 error linked from X pages" with source URLs in details.

---

## 🛡️ Block 4: Security & Protocols ✅ COMPLETED

### 1. Mixed Content Detection ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` checks `images` for `http://` src on `https://` pages. Reports "Security: Mixed Content" with asset list.
*   **🖥️ Frontend:** `IssueDetails` shows insecure assets. `SecurityScorecard.tsx` reflects mixed content status.

### 2. Security Headers Check ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` checks for missing `Strict-Transport-Security`, `Content-Security-Policy`, and `X-Frame-Options` headers.
*   **🖥️ Frontend:** `SecurityScorecard.tsx` provides a visual scorecard (0-100) and checklist for SSL, HSTS, CSP, and X-Frame-Options status.

---

## ⚡ Block 5: DOM, JS & Performance ✅ COMPLETED

### 1. DOM Complexity & Size ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `seo_extractor.py` counts HTML nodes and calculates nesting depth using BeautifulSoup (lines 60-83). Data persisted via `crawl_db.py`.
*   **🖥️ Frontend:** `DOMComplexityChart.tsx` visualizes DOM Size and Depth distribution with performance thresholds.

### 2. JS Rendering Differences ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `crawler.py` fetches raw HTML alongside JS rendering, computes hashes, and flags `requires_js` if significant content differs (lines 1024-1065).
*   **🖥️ Frontend:** `url-analysis-sheet.tsx` displays "Requires JS" badge and warning when dynamic content is detected.

---

## 🌍 Block 6: International & Structured Data ✅ COMPLETED

### 1. Hreflang Validation ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` has `detect_hreflang_issues()` that validates ISO 639-1 codes, checks reciprocal links, detects missing self-references, and flags hreflang pointing to non-200 pages (lines 1656-1810).
*   **🖥️ Frontend:** `HreflangMatrix.tsx` displays a table with source page, language code, target URL, reciprocal status (✅/❌), and HTTP status. Added as "International" tab in `crawler-results.tsx`.

### 2. AI Readiness (Schema) ✅
*   **Status:** ✅ IMPLEMENTED
*   **🐍 Backend:** `issue_detector.py` enhanced `_check_structured_data_issues()` to detect AI-ready schema types (FAQPage, LocalBusiness, Article, Product, etc.), extract FAQ items for preview, and validate schema structure (lines 675-825).
*   **🖥️ Frontend:** `RichResultPreview.tsx` renders a mock Google Search result with FAQ accordions, rating stars, and schema type badges. Integrated into `url-analysis-sheet.tsx` under "Structured Data & AI Readiness" section.
