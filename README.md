# D-Rock Financial Terminal - Enterprise Reporting Engine

## Overview
This is a production-grade internal analytics application built for Directors and Floor Managers. The system operates on a highly resilient 4-Tier architecture that ingests raw telemarketing and financial CSV data into a centralized PostgreSQL database, then processes it into high-level business intelligence.

**Important Note:** The application runs automatically in the cloud on Railway, utilizing a fully managed PostgreSQL database endpoint. All operational and financial data is securely backed up and stored persistently.

---

## The 4-Tier Architecture

### 1. Ingestion Control (Finance & Ops)
Dropzones for bulk-uploading raw betting extractions and agency outcomes. When uploaded here, files are instantly parsed, validated against the Universal Brand Translator, and permanently fused into the PostgreSQL backend.

### 2. Operations Command
The primary control panel for managing real-time telemarketing data. Tracks daily delivery volumes, contact ratios, and true cost-per-acquisition metrics against historical baselines and active SLAs.

### 3. CRM Intelligence & Financials
Deep analytical views parsing cumulative lifetime value (LTV), cross-brand cannibalization, 80/20 Pareto curves, and dynamic VIP tiering.

### 4. Admin Management
A configuration workspace enabling Directors to define volume SLAs, manually sync missing Brand mappings to the backend, and audit data hygiene across active clients.

---

## Instructions for Directors & Managers

### How to Log In
1. Navigate to the cloud application URL hosted on Railway.
2. If the platform requests a login, select the `superadmin` profile and use the secure password provided during handover.
3. Upon successful login, the system will instantly hydrate all 4 architectural tiers.

### How to Ingest Daily Operations & Monthly Financials
1. **Navigate** via the main sidebar to either `📞 Operations` or `🏦 Financial`.
2. Locate the designated **Ingestion** tab within those views (e.g., `🗄️ Operations Ingestion` or `📥 Financial Ingestion`).
3. **Drag-and-Drop** your raw exported `.csv` or `.xlsx` files straight into the uploader zone.
4. **Click "Process"**. The system will scan, validate, and permanently sink the rows into PostgreSQL.
5. You can immediately return to the main dashboard views; all charts, benchmark deltas, and SLA trackers will be instantly re-calculated.

---
*For system recovery or maintenance issues, please check the Railway metrics dashboard. In the case of severe failure, notify the deployment engineer.*
