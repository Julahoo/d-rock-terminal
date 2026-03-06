import os

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace(
    'if "📊 Executive Summary" in tab_map:\n        with tab_map["📊 Executive Summary"]:',
    'if "🏦 Financial Deep-Dive" in tab_map:\n        with tab_map["🏦 Financial Deep-Dive"]:'
)

# Slice out VIP Health
idx_vip_start = text.find('                # ── Cross-Brand VIP Health ─────────────────────────────')
idx_vip_end = text.find('                # ── Cross-Brand Cash Flow & Promo ─────────────────────────────')

if idx_vip_start != -1 and idx_vip_end != -1:
    vip_block = text[idx_vip_start:idx_vip_end]
    text = text[:idx_vip_start] + text[idx_vip_end:]
else:
    print("Cannot find VIP block")
    exit(1)

# Slice out Cannibalization
idx_can_start = text.find('                # ── Cross-Brand Cannibalization ──────────────────────────────')
idx_can_end = text.find('            else:\n                st.warning("No financial data available for Executive Summary.")')

if idx_can_start != -1 and idx_can_end != -1:
    can_block = text[idx_can_start:idx_can_end]
    text = text[:idx_can_start] + text[idx_can_end:]
else:
    print("Cannot find CAN block")
    exit(1)

# Inject them both
idx_crm_end = text.find('            st.markdown("#### > VIP & RISK LEADERBOARDS_")')

if idx_crm_end != -1:
    injected_blocks = vip_block + '\n' + can_block + '\n\n'
    text = text[:idx_crm_end] + injected_blocks + text[idx_crm_end:]
else:
    print("Cannot find CRM end")
    exit(1)

with open('app.py', 'w', encoding='utf-8') as f:
    f.write(text)

print("Migration successful.")
