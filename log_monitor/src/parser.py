import re, pandas as pd

def parse_logs(filepath):
    pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
        r' \w+ \[(\w+\.\d+\.\d+\w)\] \(tid=(\d+)\) (.+)'
    )
    records = []
    with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                timestamp, error_code, tid, message = match.groups()
                severity = 'ERROR' if error_code.endswith('E') else 'INFO' if error_code.endswith('C') else 'WARNING'
                service = 'UNKNOWN'
                if 'EMISSION' in message:
                    service = 'EMISSION'
                elif 'RECEPTION' in message:
                    service = 'RECEPTION'
                elif 'PacsOutRouter' in message:
                    service = 'PACS_ROUTER'
                elif 'changementAgence' in message.lower():
                    service = 'CHANGEMENT_AGENCE'
                records.append({
                    'timestamp': pd.to_datetime(timestamp),
                    'error_code': error_code,
                    'thread_id': tid,
                    'severity': severity,
                    'service': service,
                    'message': message[:200]
                })
    return pd.DataFrame(records)