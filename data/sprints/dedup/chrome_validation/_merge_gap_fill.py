"""Merge gap_fill_evidence.jsonl into yellow_verdicts.jsonl by idx."""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path

verdicts_path = Path('data/sprints/dedup/chrome_validation/yellow_verdicts.jsonl')
evidence_path = Path('data/sprints/dedup/chrome_validation/gap_fill_evidence.jsonl')

# Load evidence map
evidence_map = {}
with open(evidence_path, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            rec = json.loads(line)
            # Keep the last occurrence
            evidence_map[rec['idx']] = rec

# Load verdicts
verdicts = []
with open(verdicts_path, 'r', encoding='utf-8') as f:
    for line in f:
        if line.strip():
            verdicts.append(json.loads(line))

# Apply gap-fill: update chrome_evidence for matching idx
updated_count = 0
for v in verdicts:
    idx = v.get('idx')
    if idx in evidence_map:
        ev = evidence_map[idx]
        # Preserve original evidence if any, and append URL + new text
        v['chrome_evidence'] = f"{ev.get('evidence_text', '')} [URL: {ev.get('evidence_url', '')}]"
        updated_count += 1

# Write back (dedupe by idx — keep last occurrence since batch-logs may duplicate)
by_idx = {}
for v in verdicts:
    by_idx[v.get('idx')] = v

final = [by_idx[idx] for idx in sorted(by_idx.keys())]

# Backup
import shutil
shutil.copy(verdicts_path, str(verdicts_path) + '.bak')

# Write
with open(verdicts_path, 'w', encoding='utf-8') as f:
    for v in final:
        f.write(json.dumps(v, ensure_ascii=False) + '\n')

print(f'Updated {updated_count} verdicts with gap-fill evidence.')
print(f'Total verdicts in final file: {len(final)}')

# Verify how many now have URL-grounded evidence
import re
has_url = re.compile(r'(?:https?://\S+|\.com|\.fr|\.it|\.at|\.pt|\.es|\.de|\.co|\.net|\.org)')
with_url = sum(1 for v in final if has_url.search(v.get('chrome_evidence', '') or ''))
without_url = len(final) - with_url
print(f'  With URL citation: {with_url}')
print(f'  Without URL citation: {without_url}')

# List the without_url ones
if without_url > 0:
    print('\nMissing URL:')
    for v in final:
        if not has_url.search(v.get('chrome_evidence', '') or ''):
            print(f"  #{v['idx']:>2} {v['name']}: {(v.get('chrome_evidence') or '')[:80]}")
