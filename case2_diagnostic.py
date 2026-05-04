import os
import pandas as pd
from datetime import datetime

INPUT_CSV = "output/features_with_scores.csv"
ALERTS_LOG = "output/alerts.log"
DIAGNOSTIC_CSV = "output/incident_diagnostic.csv"

THRESHOLD = 0.3

def normalize(series):
    s = pd.to_numeric(series, errors="coerce").fillna(0)
    if s.nunique() <= 1:
        return pd.Series([0.0] * len(s), index=s.index)
    return ((s - s.min()) / (s.max() - s.min())).clip(0, 1)

def classify_cause(row):
    nb_0020e = row.get("nb_0020E", 0)
    nb_0042e = row.get("nb_0042E", 0)
    ratio_network = row.get("ratio_network", 0)
    ratio_business = row.get("ratio_business", 0)
    score = row.get("anomaly_score_01", 0)

    if nb_0020e > 0 and ratio_network >= ratio_business:
        return pd.Series(["Réseau", "Tester JMS / UM / endpoints réseau"])
    elif nb_0042e > 0 and nb_0020e == 0:
        return pd.Series(["Applicatif", "Vérifier triggers / traitements métier"])
    elif score >= THRESHOLD:
        return pd.Series(["Autre", "Corréler avec logs système et monitoring"])
    else:
        return pd.Series(["Normal", "Aucune alerte"])

def main():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Fichier introuvable : {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    df.columns = df.columns.str.strip()

    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")

    required_cols = ["nb_0020E", "nb_0042E", "nb_retries", "ratio_network", "ratio_business", "ratio_system"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0

    score_parts = (
        normalize(df["nb_0020E"]) * 0.30 +
        normalize(df["nb_0042E"]) * 0.20 +
        normalize(df["nb_retries"]) * 0.20 +
        normalize(df["ratio_network"]) * 0.15 +
        normalize(df["ratio_business"]) * 0.10 +
        normalize(df["ratio_system"]) * 0.05
    )

    df["anomaly_score_01"] = score_parts.clip(0, 1)

    print("Colonnes disponibles :", df.columns.tolist())
    print("Résumé des scores :")
    print(df["anomaly_score_01"].describe())

    threshold_used = THRESHOLD
    if df["anomaly_score_01"].max() < THRESHOLD:
        threshold_used = df["anomaly_score_01"].quantile(0.90)
        print(f"Aucun score au-dessus de {THRESHOLD}. Seuil auto utilisé : {threshold_used:.3f}")

    df["is_anomaly"] = df["anomaly_score_01"] >= threshold_used
    anomalies = df[df["is_anomaly"] == True].copy()

    os.makedirs("output", exist_ok=True)

    if anomalies.empty:
        anomalies.to_csv(DIAGNOSTIC_CSV, index=False)
        print("Aucune anomalie détectée.")
        return

    diag = anomalies.copy()
    diag[["cause_probable", "action_recommandee"]] = diag.apply(classify_cause, axis=1)
    diag.to_csv(DIAGNOSTIC_CSV, index=False)

    with open(ALERTS_LOG, "w", encoding="utf-8") as f:
        f.write(f"ALERT GENERATED AT {datetime.now().isoformat()}\n")
        for _, row in diag.sort_values("anomaly_score_01", ascending=False).head(10).iterrows():
            f.write(
                f"{row.get('window_start', '')} | "
                f"score={row.get('anomaly_score_01', 0):.3f} | "
                f"cause={row.get('cause_probable', '')} | "
                f"action={row.get('action_recommandee', '')}\n"
            )

    print("Diagnostic généré :", DIAGNOSTIC_CSV)
    print("Alerte générée :", ALERTS_LOG)

if __name__ == "__main__":
    main()