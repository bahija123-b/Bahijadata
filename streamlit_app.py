import os
import tempfile

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.ensemble import IsolationForest

from feature_engineering import extract_features


st.set_page_config(
    page_title="Détection Anomalies Logs",
    layout="wide",
    initial_sidebar_state="expanded"
)

CRASH_TIME = pd.to_datetime("2026-03-24 14:10:43")
DEFAULT_LOG_PATH = "data/server.log.20260324.1"

FEATURE_CANDIDATES = [
    "nb_logs",
    "nb_tids",
    "mean_time_between_errors",
    "ratio_network",
    "ratio_business",
    "ratio_system",
    "nb_retries",
    "nb_0020E",
    "nb_0042E",
    "nb_0058E",
    "nb_0101E",
]


def normalize_anomaly_score(raw_scores):
    s = pd.Series(raw_scores, dtype="float64")
    if s.nunique(dropna=True) <= 1:
        return pd.Series([0.0] * len(s), index=s.index)
    normalized = (s.max() - s) / (s.max() - s.min())
    return normalized.clip(0, 1)


def classify_incident(row):
    nb_0020e = row.get("nb_0020E", 0)
    nb_0042e = row.get("nb_0042E", 0)
    ratio_network = row.get("ratio_network", 0)
    ratio_business = row.get("ratio_business", 0)

    if nb_0020e > 0 and ratio_network >= ratio_business:
        return "Réseau"
    if nb_0042e > 0 and nb_0020e == 0:
        return "Applicatif"
    if ratio_network > ratio_business:
        return "Réseau probable"
    if ratio_business > ratio_network:
        return "Applicatif probable"
    return "Indéterminé"


def train_and_score(features_df, contamination):
    available_cols = [c for c in FEATURE_CANDIDATES if c in features_df.columns]
    if not available_cols:
        raise ValueError("Aucune colonne de features exploitable pour Isolation Forest.")

    X = features_df[available_cols].fillna(0)

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X)

    scored_df = features_df.copy()
    scored_df["raw_anomaly_score"] = model.decision_function(X)
    scored_df["anomaly_score_01"] = normalize_anomaly_score(scored_df["raw_anomaly_score"])
    scored_df["is_anomaly"] = model.predict(X) == -1
    scored_df["incident_type"] = scored_df.apply(classify_incident, axis=1)

    if "window_start" in scored_df.columns:
        scored_df["window_start"] = pd.to_datetime(scored_df["window_start"], errors="coerce")

    return scored_df, available_cols


def run_pipeline(log_path, contamination):
    features_df = extract_features(log_path)
    if features_df is None or features_df.empty:
        raise ValueError("Aucune feature extraite depuis le fichier fourni.")
    return train_and_score(features_df, contamination)


def save_uploaded_file(uploaded_file):
    suffix = os.path.splitext(uploaded_file.name)[1] or ".log"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(uploaded_file.getvalue())
        return tmp.name


def build_summary(scored_df):
    anomalies = scored_df[scored_df["is_anomaly"]].copy()
    early_detection = pd.DataFrame()

    if "window_start" in scored_df.columns:
        early_detection = anomalies[anomalies["window_start"] < CRASH_TIME].copy()

    first_anomaly = anomalies["window_start"].min() if not anomalies.empty and "window_start" in anomalies.columns else None
    first_early = early_detection["window_start"].min() if not early_detection.empty else None

    minutes_before = None
    if first_early is not None and pd.notna(first_early):
        minutes_before = round((CRASH_TIME - first_early).total_seconds() / 60, 2)

    dominant_type = "Indéterminé"
    if not anomalies.empty and "incident_type" in anomalies.columns:
        dominant_type = anomalies["incident_type"].mode().iloc[0]

    return {
        "nb_windows": len(scored_df),
        "nb_anomalies": int(scored_df["is_anomaly"].sum()),
        "max_score_01": float(scored_df["anomaly_score_01"].max()),
        "first_anomaly": first_anomaly,
        "first_early": first_early,
        "minutes_before": minutes_before,
        "dominant_type": dominant_type,
        "early_detection_count": len(early_detection),
        "anomalies": anomalies,
        "early_detection": early_detection,
    }


st.title("🔍 Détection d'anomalies logs - Isolation Forest")
st.markdown("**POC Stage Al Barid Bank** — Détection précoce avant la cascade du 24 mars 14:10:43")

with st.sidebar:
    st.header("⚙️ Configuration")
    uploaded_file = st.file_uploader(
        "📁 Fichier de logs",
        type=["log", "txt", "csv"],
        help="Charge un fichier log applicatif ou système pour lancer l’analyse."
    )
    contamination = st.slider(
        "Seuil d’anomalie (contamination)",
        min_value=0.05,
        max_value=0.20,
        value=0.10,
        step=0.01
    )
    use_demo_file = st.button("🚀 Tester sur logs 24 mars")

log_path = None
source_label = None
temp_path = None

if uploaded_file is not None:
    temp_path = save_uploaded_file(uploaded_file)
    log_path = temp_path
    source_label = f"Fichier chargé : {uploaded_file.name}"
elif use_demo_file:
    if os.path.exists(DEFAULT_LOG_PATH):
        log_path = DEFAULT_LOG_PATH
        source_label = f"Fichier de démonstration : {DEFAULT_LOG_PATH}"
    else:
        st.error("❌ Fichier de démonstration introuvable : data/server.log.20260324.1")

if log_path is None:
    st.info("👈 Charge un fichier de logs ou lance le test sur les logs du 24 mars.")
else:
    st.caption(source_label)

    try:
        with st.spinner("Analyse en cours : extraction des features et scoring ML..."):
            scored_df, used_features = run_pipeline(log_path, contamination)
            summary = build_summary(scored_df)

        st.success("✅ Analyse terminée avec succès.")

        st.subheader("📌 Objectifs encadrant")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fenêtres analysées", f"{summary['nb_windows']}")
        c2.metric("Anomalies détectées", summary["nb_anomalies"])
        c3.metric("Score max (0-1)", f"{summary['max_score_01']:.3f}")
        c4.metric("Type dominant", summary["dominant_type"])

        st.subheader("⏱️ Validation temporelle")
        v1, v2, v3 = st.columns(3)

        first_anomaly_text = (
            str(summary["first_anomaly"])
            if summary["first_anomaly"] is not None and pd.notna(summary["first_anomaly"])
            else "Aucune"
        )
        v1.metric("Première anomalie", first_anomaly_text)

        if summary["first_early"] is not None and pd.notna(summary["first_early"]):
            v2.metric("Avant 14:10:43", str(summary["first_early"]))
            v3.metric("Avance", f"{summary['minutes_before']} min")
            st.success(
                f"🎉 Détection précoce confirmée : première anomalie à {summary['first_early']} "
                f"({summary['minutes_before']} minutes avant 14:10:43)."
            )
        else:
            v2.metric("Avant 14:10:43", "Non détecté")
            v3.metric("Avance", "0 min")
            st.warning("Pas de détection précoce avant 14:10:43 sur cette exécution.")

        st.subheader("🧠 Features utilisées")
        st.write(", ".join(used_features))

        st.subheader("📈 Analyse temporelle")
        x_vals = scored_df["window_start"] if "window_start" in scored_df.columns else scored_df.index

        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "Score d'anomalie (0-1)",
                "Erreurs 0020E",
                "TIDs actifs",
                "Ratio réseau (%)",
            ),
        )

        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=scored_df["anomaly_score_01"],
                mode="lines+markers",
                name="Score 0-1",
                line=dict(color="orange", width=3),
            ),
            row=1,
            col=1,
        )

        if "window_start" in scored_df.columns:
            fig.add_vline(
                x=CRASH_TIME,
                line_dash="dash",
                line_color="red",
                row=1,
                col=1,
            )

        if "nb_0020E" in scored_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=scored_df["nb_0020E"],
                    mode="lines+markers",
                    name="0020E",
                    line=dict(color="red", width=2),
                ),
                row=1,
                col=2,
            )

        if "nb_tids" in scored_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=scored_df["nb_tids"],
                    mode="lines+markers",
                    name="TIDs",
                    line=dict(color="blue", width=2),
                ),
                row=2,
                col=1,
            )

        if "ratio_network" in scored_df.columns:
            fig.add_trace(
                go.Scatter(
                    x=x_vals,
                    y=scored_df["ratio_network"] * 100,
                    mode="lines+markers",
                    name="Réseau %",
                    line=dict(color="green", width=2),
                ),
                row=2,
                col=2,
            )

        fig.update_layout(
            height=700,
            showlegend=False,
            title_text="Analyse des anomalies par fenêtres de 5 minutes"
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("🚨 Fenêtres les plus suspectes")
        anomalies = summary["anomalies"].sort_values(by="anomaly_score_01", ascending=False)

        cols_to_show = [
            c for c in [
                "window_start",
                "anomaly_score_01",
                "incident_type",
                "nb_0020E",
                "nb_0042E",
                "nb_tids",
                "ratio_network",
                "ratio_business",
            ]
            if c in anomalies.columns
        ]

        if not anomalies.empty and cols_to_show:
            st.dataframe(anomalies[cols_to_show].head(10), use_container_width=True)
        else:
            st.info("Aucune anomalie détectée sur ce jeu de données.")

        st.subheader("🧾 Lecture métier")
        st.markdown(
            """
- **Réseau** : présence de `0020E` et/ou dominance du ratio réseau.
- **Applicatif** : pic de `0042E` sans `0020E`, ou dominance métier.
- **But du POC** : détecter une déviation avant la cascade du 24 mars, pas seulement après l’explosion des erreurs.
            """
        )

    except Exception as e:
        st.error(f"❌ Erreur pendant l’analyse : {e}")

    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass