from datetime import datetime
import os

def print_alerts(alerts_df):
    print("\n" + "="*60)
    print("  RAPPORT D ALERTES - LOG MONITOR")
    print("="*60)
    print(f"  Genere le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Total alertes : {len(alerts_df)}\n")
    for _, row in alerts_df.iterrows():
        niveau = "[CRITIQUE]" if row['severity'] == 'CRITICAL' else "[ERREUR]" if row['severity'] == 'ERROR' else "[WARNING]"
        print(f"{niveau} {row['type']}")
        print(f"   --> {row['detail']}\n")

def save_alerts(alerts_df, path='output/alerts_report.txt'):
    os.makedirs('output', exist_ok=True)
    with open(path, 'w') as f:
        f.write(f"RAPPORT D ALERTES - {datetime.now()}\n")
        f.write("="*60 + "\n\n")
        for _, row in alerts_df.iterrows():
            f.write(f"[{row['severity']}] {row['type']}\n")
            f.write(f"  {row['detail']}\n\n")
    print(f"Alertes sauvegardees dans {path}")