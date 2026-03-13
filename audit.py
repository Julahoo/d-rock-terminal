import sys, re

with open('app.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

print('--- END TO END AUDIT REPORT: app.py ---')
for i, line in enumerate(lines):
    if 'pd.read_sql' in line:
        cached = False
        for j in range(max(0, i-5), i):
            if '@st.cache' in lines[j]:
                cached = True
                break
        if not cached:
            print(f'[{i+1}] UNCACHED SQL: {line.strip()}')

    if '.apply(' in line and ('lambda' in line):
        print(f'[{i+1}] HEAVY PANDAS APPLY (Lambda): {line.strip()}')
        
    if '.groupby' in line or '.merge' in line or '.concat' in line:
        print(f'[{i+1}] HEAVY PANDAS OP: {line.strip()}')
