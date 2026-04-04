import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from src.parser import parse_logs
from src.detector import detect_anomalies
from src.alert import print_alerts, save_alerts
import glob
logs = glob.glob("data/*")
if not logs:
    print("Aucun fichier dans data/")
    exit()
LOG_FILE = logs[0]
print("Parsing de " + LOG_FILE)
df = parse_logs(LOG_FILE)
print(str(len(df)) + " lignes parsees")
alerts = detect_anomalies(df)
print_alerts(alerts)
save_alerts(alerts)
os.makedirs("output", exist_ok=True)
df.to_csv("output/errors_report.csv", index=False)
print("Rapport CSV sauvegarde")
