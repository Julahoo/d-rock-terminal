# Campaign Component Extraction — Full Convention Alignment

Extract all 8 campaign naming convention components during ops ingestion so sidebar filters map 1:1 to the naming standard: `Brand-Country-Language-Product-Segment-Lifecycle-Sublifecycle-Engagement`.

## Current State → Target

| Component | Currently | After |
|-----------|-----------|-------|
| Brand | ✅ `ops_brand` | No change |
| Country | ✅ `country` | No change |
| Language | ❌ | ✅ `extracted_language` (smart defaults from country) |
| Product | ⚠️ computed at render | ✅ `extracted_product` (stored in DB) |
| Segment | ✅ `extracted_segment` | No change |
| Lifecycle | ✅ `extracted_lifecycle` | No change |
| Sublifecycle | ❌ | ✅ `extracted_sublifecycle` |
| Engagement | ✅ `extracted_engagement` | No change |

## Changes

### 1. `src/ingestion.py` — Add 3 new extractions (~line 865)

```python
# Product: SPO, CAS, LIVE, ALL
extracted_product = next((t for t in tokens if t in ['SPO', 'CAS', 'LIVE', 'ALL']), "UNKNOWN")

# Sublifecycle: J1, J2, J3, BULK, NA
extracted_sublifecycle = next((t for t in tokens if t in ['J1', 'J2', 'J3', 'BULK']), "UNKNOWN")

# Language: smart defaults from country, else UNKNOWN
LANG_DEFAULTS = {
    'TR': 'TR', 'ES': 'ES', 'CL': 'ES', 'EC': 'ES', 'MX': 'ES',
    'AR': 'ES', 'CO': 'ES', 'PE': 'ES', 'BR': 'PT', 'GB': 'EN',
    'UK': 'EN', 'IE': 'EN', 'CA': 'EN', 'NZ': 'EN', 'JP': 'JA',
    'DE': 'DE', 'AT': 'DE', 'CH': 'DE', 'SE': 'SV', 'NO': 'NO',
    'DK': 'DA', 'FI': 'FI', 'IT': 'IT', 'FR': 'FR'
}
extracted_language = LANG_DEFAULTS.get(country, "UNKNOWN")
```

Add to `records_to_insert` dict and DB column lists.

### 2. `src/database.py` — Add 3 columns to both tables

```sql
ALTER TABLE ops_telemarketing_data ADD COLUMN extracted_product TEXT DEFAULT 'UNKNOWN';
ALTER TABLE ops_telemarketing_data ADD COLUMN extracted_language TEXT DEFAULT 'UNKNOWN';
ALTER TABLE ops_telemarketing_data ADD COLUMN extracted_sublifecycle TEXT DEFAULT 'UNKNOWN';
-- Same for ops_telemarketing_snapshots
```

Since DB purge + re-ingestion is planned, columns populate from scratch.

### 3. `app.py` — Update sidebar form

- Remove render-time `__extracted_category` hack
- Add 3 new dropdowns: Language, Product (replaces Category), Sublifecycle
- Full order: Client → Brand → Country → Language → Product → Segment → Lifecycle → Sublifecycle → Engagement
- Display maps: Product codes → friendly names (SPO→Sportsbook, CAS→Casino, LIVE→Live)

### Missing Fields

All 3 new fields default to `"UNKNOWN"` when not found in campaign name. UNKNOWN values are excluded from dropdown options but included when filter is "All".

## Timing

Implement **after** the DB purge. The re-ingestion will populate all new columns automatically.
