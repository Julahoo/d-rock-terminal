# Player-Level Journey Tracking

Pull contact-level events from iWinBack API to build conversion funnel analytics and time-to-convert analysis. Link with monthly financial reports via `account_number` for True ROI.

## API Endpoints Used

| Endpoint | Purpose | Key Fields |
|---|---|---|
| `GET /api/contact_campaign_association/client_reporting` | Link contact → campaign | `contact_id`, `account_number`, `campaign_id`, `brand_id`, `status` |
| `GET /api/contact_logins` | Login events | `contact_id`, `login_at`, `type`, `domain` |
| `GET /api/contact_registers` | Registration events | `contact_id`, `account_number`, timestamps |
| `GET /api/contact_deposits` | FTD/deposit events (leading indicator) | `contact_id`, `account_number`, timestamps |

## Data Model

Single table `ops_contact_events`:

```sql
CREATE TABLE ops_contact_events (
    id SERIAL PRIMARY KEY,
    box_id VARCHAR(10) NOT NULL,
    contact_id INTEGER NOT NULL,
    account_number VARCHAR(100),
    campaign_id INTEGER,
    brand_id INTEGER,
    event_type VARCHAR(20) NOT NULL,  -- 'login', 'register', 'deposit'
    event_at TIMESTAMP NOT NULL,
    extra_data JSONB,                 -- domain, ip, type, etc.
    ingested_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_events_campaign ON ops_contact_events(campaign_id);
CREATE INDEX idx_events_account ON ops_contact_events(account_number);
CREATE INDEX idx_events_type_date ON ops_contact_events(event_type, event_at);
```

## Cron Sync (Daily, alongside campaign_summary_v3)

For each box:
1. Pull `contact_campaign_association/client_reporting?since_at={yesterday}` → new contact↔campaign links
2. Pull `contact_logins?login_at={yesterday}` → login events
3. Pull `contact_registers?created_at=>{yesterday}` → registration events
4. Pull `contact_deposits?created_at=>{yesterday}` → deposit events
5. Upsert into `ops_contact_events` with dedup on `(box_id, contact_id, event_type, event_at)`

## Backfill Strategy

- **Range:** 2025-01-01 → today (~14 months)
- **Method:** Chunk by month, iterate backwards from latest
- **Rate limit:** 10,000 req/box — process in daily batches with sleep between

## UI: Two New Tabs in Operations Command

### Tab 1: 🔄 Conversion Funnel

Filterable by brand, campaign, date range. Visual funnel:
- 📞 Calls (from `ops_telemarketing_data`)
- 🔑 Logins (event_type='login', deduplicated by contact)
- 📝 Registrations (event_type='register')
- 💰 FTDs (event_type='deposit')
- 💵 GGR (linked via `account_number` to financial data)

Per-campaign metrics: Call→Login %, Login→Reg %, Reg→FTD %, **True ROI** (GGR ÷ campaign cost).

### Tab 2: ⏱️ Time-to-Convert

Per campaign/brand:
- Median hours: Call → Login, Login → Register, Register → FTD
- Distribution histogram (24h buckets)
- Identifies fast-converting vs. slow-burn campaigns

## Financial Linkage

```
iWinBack contact_id → account_number → monthly financial reports (per-player GGR)
```

- Financials are monthly aggregates, not per-deposit
- Deposit events serve as **leading indicators** before monthly report arrives
- Enables: Revenue per acquired player, Campaign ROI, Player quality scoring

## Verification

- Query each endpoint on one box to validate response shape
- Seed with 1 month of data, verify funnel counts match manual dashboard checks
- Compare FTD counts with known conversion numbers from campaign_summary_v3
