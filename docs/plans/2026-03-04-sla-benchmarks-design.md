# System Design: SLA Fulfillment & Granular Benchmarks

**Date:** 2026-03-04
**Topic:** SLA Tracking, Operations Benchmarks, and Asymmetric Scaling

## 1. Overview
This document outlines the validated architectural design for tracking Client SLAs and historical operational benchmarks within the CRMTracker Operations Dashboard. The goal is to enforce monthly volume minimums natively while comparing daily ingested operations data against granular, campaign-type-specific historical performance targets (like Conv% and True CAC).

## 2. Architecture: Dual-Layer Targets
Instead of a single, static SLA configuration matrix, the logic is decoupled into two specialized databases to support multi-dimensional limits without repeating data.

- **`contractual_volume_slas`**: Tracks strictly top-level volume minimums per client/brand/lifecycle. Instead of daily hardcoded limits, the system stores **Monthly Limits** (e.g., 5,000 calls). When charting 30-Day or 7-Day trends, the backend natively divides this limit by the calendar interval.
- **`granular_efficiency_benchmarks`**: Stores specific historical targets per granular campaign signature (e.g., `BAH-CH-ALL-RND-LI`). Includes targets for `Conv%`, `LI%`, and `True CAC`.

## 3. Configuration UI & Workflow
To remove dependency on manual Excel adjustments, benchmarks will be fully configurable via Administration screens.
- When an Administrator selects a Brand in the **System Settings -> Client Profiles**, an **"SLA & Benchmarks"** drawer is rendered.
- Admins input the Monthly Volume constraints and append granular campaign targets directly into the UI.
- On save, Streamlit commits these targets to the Dual-Layer PostgreSQL tables.

## 4. Daily Ingestion Engine
Operations data uploaded by users is processed as daily snapshots rather than monthly static files.
- `ingestion.py` processes files as static points in time, writing to `ops_telemarketing_snapshots`.
- The timestamp on ingestion allows the dashboard to compute the exact delta between the ingested day's True CAC and the benchmark target CAC configured in the Client Profile for that specific campaign.

## 5. Memory & Scalability (Asymmetric Computation)
To prevent Out-Of-Memory (OOM) crashes as the operations dataset scales infinitely over daily ingestions, operations are offset to the PostgreSQL engine:
- **Streaming Ingestion**: `ingestion.py` loads user Excel files via generator buffers, circumventing total-file memory loading.
- **SQL-Level Aggregation**: Trend groupings (Calls by Day, Conversions by Brand) are written strictly as `pd.read_sql` aggregation queries. Data is joined to the `granular_efficiency_benchmarks` table at the database level.
- **Micro-Payload Delivery**: Only the final pre-calculated, indexed chart coordinates (~15 rows of final metrics) cross the network into Streamlit's memory structure to generate the interactive Plotly graphs, preserving server RAM permanently.
