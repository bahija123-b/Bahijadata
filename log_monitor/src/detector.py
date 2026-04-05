import pandas as pd

def detect_anomalies(df):
    alerts = []
    for code, count in df['error_code'].value_counts().items():
        if count > 10:
            alerts.append({
                'type': 'HIGH_FREQUENCY',
                'severity': 'WARNING',
                'detail': f"Code {code} apparu {count} fois"
            })
    checks = [
        ('ControlCompte', 'CRITICAL_SERVICE_FAILURE', 'CRITICAL'),
        ('ErrorHandler failed', 'ERROR_HANDLER_FAILURE', 'CRITICAL'),
        ('does not exist in the pipeline', 'MISSING_VARIABLE', 'ERROR'),
        ('Delivery Count has exceeded', 'MESSAGE_REJECTED', 'CRITICAL'),
    ]
    for kw, label, sev in checks:
        n = len(df[df['message'].str.contains(kw, na=False)])
        if n > 0:
            alerts.append({'type': label, 'severity': sev, 'detail': f"{label} detecte {n} fois"})
    return pd.DataFrame(alerts)