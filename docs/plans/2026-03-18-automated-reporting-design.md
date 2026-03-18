# Automated Reporting Engine Design
**Date:** 2026-03-18
**Topic:** Brand/Lifecycle Automated Reporting (Executive Morning Briefings)

## 1. Overview and Architecture
To provide executives with instant operational visibility across the entire multi-brand ecosystem, D-ROCK will introduce an **Automated Reporting Engine**.
This engine operates entirely headless via a Railway cron job (e.g., executing immediately after the 5:00 AM ingestion synchronization completes). Rather than forcing stakeholders to log into the web dashboard, the engine automatically extracts the data, generates the visualizations directly in memory using Plotly's `Kaleido` image export capabilities, and sends a native HTML email directly to their inbox.

## 2. Report Structure & Scope
The report dynamically queries the last 30 days of `ops_telemarketing_data`.

**Target Entities:**
The loop iterates through 8 explicitly defined clients:
1. Reliato
2. Limitless
3. Simplicity
4. LeoVegas
5. Offside
6. Powerplay
7. Magico Games / Interspin
8. Rhino

**Cohort Segmentation:**
Each client is strictly bifurcated into two lifecycles using the existing `extracted_lifecycle` schemas:
- **WB**: Win-Back
- **RND**: Register No Deposit

## 3. The Visual Layout
The final HTML email is structurally composed of **8 massive visually-isolated blocks** (one per client).
Within each client block, the engine calculates the dynamic sum of `(WB Volume + RND Volume)` over the past 30 days and cross-references it against the `contractual_volumes` SQL table to inject a colored text alert:
- 🟢 `SLA OK: [Client] hit [X] New Data vs SLA [Y]`
- 🔴 `SLA MISSED: [Client] hit [X] New Data vs SLA [Y]`

Beneath the SLA alert, the client block is perfectly subdivided into the **WB** logic and the **RND** logic. Both receive their own independent 3-Chart Stack.

### The 3-Chart Stack
1. **Volume (New Data) Tracker**
   - **Visual:** A 30-day bar/area chart.
   - **Overlays:** A dashed line showing the 7-Day Rolling Average; a solid line denoting the 30-Day Rolling Average.
   - **Data Table:** A succinct summary table placed immediately underneath declaring the exact 7-Day and 30-Day averages, complete with a visual ⬆️/⬇️ trend indicator.
2. **Logins Tracker**
   - **Visual:** A 30-day line chart mapping daily user engagement.
   - **Overlays:** 7-Day and 30-Day horizontal baseline averages.
   - **Data Table:** Averaged daily Logins with Up/Down trend calculations.
3. **Conversions Tracker**
   - **Visual:** A 30-day line chart mapping daily player conversions.
   - **Overlays:** 7-Day and 30-Day horizontal baseline averages.
   - **Data Table:** Averaged daily Conversions with Up/Down trend calculations.

## 4. Administrative Controls (RBAC)
A new configuration sub-module will be added to the `⚙️ Admin` workspace.
This UI allows administrators to securely add, edit, or purge the target distribution list of email addresses that the Railway background cron job uses to dispatch the Morning Briefing.

## 5. Technology Stack
- **Database:** PostgreSQL (`ops_telemarketing_data`, `contractual_volumes`)
- **Charting Engine:** Plotly (`plotly.graph_objects`)
- **Image Renderer:** `kaleido` (Headless PNG generation)
- **Delivery Protocol:** Native MIME/Base64 Python `smtplib` or standard transactional API integration depending on chosen credentials.
