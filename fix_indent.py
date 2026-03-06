import re

with open('app.py', 'r', encoding='utf-8') as f:
    text = f.read()

start_idx = text.find('                # ── Cross-Brand VIP Health')
end_idx = text.find('            st.markdown("#### > VIP & RISK LEADERBOARDS_")')

if start_idx != -1 and end_idx != -1:
    block = text[start_idx:end_idx]
    block = re.sub(r'(?m)^ {16}', '            ', block)
    text = text[:start_idx] + block + text[end_idx:]
    with open('app.py', 'w', encoding='utf-8') as f:
        f.write(text)
    print("Indentation fixed.")
else:
    print("Block not found.")
