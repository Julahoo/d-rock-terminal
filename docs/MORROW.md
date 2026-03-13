# MORROW.md — Next Session Starting Point
**Date:** 2026-03-13 (01:00 CET)

---

## 🔴 Where We Left Off

1. **Operations Backfill (Jan 2025 → present) is RUNNING in background.** Check terminal or Railway logs for progress. Currently at ~Jan 5, 2025 — estimated several hours to complete. Once done, generate H1 2025 + H2 2025 benchmarks in Admin → Data Maintenance → Benchmark Snapshots.

2. **Player Journey Tracking (Phase 20) is APPROVED — ready to implement.** Design doc: `docs/plans/2026-03-13-player-journey-tracking-design.md`. Start by:
   - Creating `ops_contact_events` table (see design doc SQL)
   - Adding contact-level API pull to `src/iwinback_worker.py` (4 new endpoints: `contact_campaign_association`, `contact_logins`, `contact_registers`, `contact_deposits`)
   - Building the 2 new Operations Command tabs: 🔄 Conversion Funnel + ⏱️ Time-to-Convert
   - File to edit: `app.py` (Operations Command section, after Pitch vs. List Scorecard tab)

3. **Unpushed code on `dev`.** 2 commits ahead of `origin/dev`: SPEC v3.1 + Phase 20 design doc. Push when ready, then merge to `master` for Railway deploy.

---

## ✅ What Was Completed Today
- Scorecard color-coding, Cost Ledger fixes, SLA card layout
- Full production deployment (code + DB sync of 30,435 rows)
- iWinBack API credentials configured on both Railway services (web + cron)
- Fixed: stale `UNIQUE(campaign_name)` constraint, `IWINBACK_BOXES` comma issue
- SPEC.md v3.1, DEVLOG updated, Phase 20 design approved
