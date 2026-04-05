"""
POC Snapshot 01 — Moteur de détection IA des coupures réseau JMS
=================================================================
Ce module consomme les événements Kafka et applique deux niveaux de détection :

  Niveau 1 — Détection immédiate (règles)
    → ISS.0134.0020E = coupure réseau confirmée. Alerte instantanée.

  Niveau 2 — Détection de cascade (fenêtre glissante)
    → Si N erreurs business (0042E) apparaissent dans T secondes
      après un 0020E, l'IA confirme l'impact sur les virements.

  Niveau 3 — Détection de boucle infinie
    → Si des 0101E (retry) sont reçus toutes les 30s pendant > 3 min,
      l'IA lève une alerte "service non récupéré — intervention requise".

Pour chaque alerte, le module déclenche un test de connectivité (telnet socket)
et génère un rapport structuré.
"""

import json
import time
import socket
import threading
from collections import deque, defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer

KAFKA_TOPIC = "webmethods.is.logs"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP = "ai-network-detector"

# ── Configuration des seuils de détection ─────────────────────────────────────
WINDOW_SECONDS = 60          # Fenêtre glissante pour la détection de burst
BURST_THRESHOLD = 10         # Nb d'erreurs business dans WINDOW_SECONDS → alerte burst
RETRY_LOOP_COUNT = 6         # Nb de 0101E en < 3 min → boucle infinie confirmée
RETRY_LOOP_WINDOW = 180      # Fenêtre pour la détection de boucle (en secondes)

# ── Endpoints JMS à tester (telnet) ───────────────────────────────────────────
# Extraits du contexte IS : GSIMTConnectionAlias = UM (Universal Messaging)
JMS_ENDPOINTS = {
    "GSIMTConnectionAlias":       ("172.22.15.83", 9000),   # Port UM par défaut
    "GSIMTConnectionAliasDirect": ("172.22.15.83", 9000),
    "IS_Admin":                   ("172.22.15.83", 5555),
}


# ── Utilitaires ───────────────────────────────────────────────────────────────

def telnet_check(host: str, port: int, timeout: float = 3.0) -> dict:
    """
    Teste la connectivité TCP vers un endpoint (équivalent telnet).
    Retourne un dict avec le résultat et la latence.
    """
    start = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency_ms = (time.monotonic() - start) * 1000
            return {"reachable": True, "latency_ms": round(latency_ms, 1), "error": None}
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        return {"reachable": False, "latency_ms": None, "error": str(e)}


def check_all_endpoints(alias: str | None = None) -> dict:
    """
    Lance les tests de connectivité (optionnellement filtré par alias JMS).
    Retourne un résumé des résultats.
    """
    targets = {}
    if alias and alias in JMS_ENDPOINTS:
        targets[alias] = JMS_ENDPOINTS[alias]
    else:
        targets = JMS_ENDPOINTS

    results = {}
    for name, (host, port) in targets.items():
        result = telnet_check(host, port)
        results[name] = {"host": host, "port": port, **result}
        status = "✅ OK" if result["reachable"] else "❌ UNREACHABLE"
        latency = f"{result['latency_ms']}ms" if result["latency_ms"] else "N/A"
        print(f"  [TELNET] {name} ({host}:{port}) → {status} | latence: {latency}")

    return results


def format_alert(alert_type: str, details: dict, connectivity: dict) -> str:
    """Formate une alerte lisible pour les équipes."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "=" * 70,
        f"🚨 ALERTE [{alert_type}] — {ts}",
        "=" * 70,
    ]
    for k, v in details.items():
        lines.append(f"  {k:<25}: {v}")
    lines.append("")
    lines.append("  Résultats connectivité :")
    for name, r in connectivity.items():
        status = "✅" if r["reachable"] else "❌"
        latency = f"{r['latency_ms']}ms" if r["latency_ms"] else "N/A"
        lines.append(f"    {status} {name} ({r['host']}:{r['port']}) — {latency}")
    lines.append("=" * 70)
    return "\n".join(lines)


# ── Moteur de détection ───────────────────────────────────────────────────────

class NetworkOutageDetector:
    """
    Machine à états pour la détection de coupures réseau JMS.
    
    États :
      NORMAL    → aucune anomalie
      SUSPECTED → 0020E détecté, test telnet en cours
      OUTAGE    → coupure confirmée, virements impactés
      RECOVERY  → IS re-ping réussi (0020E + telnet OK)
    """

    def __init__(self):
        self.state = "NORMAL"
        self.outage_start: datetime | None = None
        self.alerts_sent: list[dict] = []

        # Fenêtres glissantes par code d'erreur
        self.windows: dict[str, deque] = defaultdict(deque)

        # Compteur de boucle 0101E
        self.retry_timestamps: deque = deque()

        # Lock pour thread safety
        self._lock = threading.Lock()

    def _clean_window(self, code: str, now: float):
        """Purge les événements hors de la fenêtre glissante."""
        cutoff = now - WINDOW_SECONDS
        while self.windows[code] and self.windows[code][0] < cutoff:
            self.windows[code].popleft()

    def process_event(self, event: dict):
        """Point d'entrée principal — analyse un événement Kafka."""
        code = event.get("error_code", "")
        alias = event.get("jms_alias")
        now = time.time()
        log_ts_str = event.get("timestamp", "")

        with self._lock:

            # ── Détection Niveau 1 : Coupure réseau directe ────────────────
            if code == "ISS.0134.0020E":
                self.windows["0020E"].append(now)
                print(f"\n⚡ [{log_ts_str}] ISS.0134.0020E détecté sur {alias}")

                if self.state == "NORMAL":
                    self.state = "SUSPECTED"
                    self.outage_start = datetime.now()
                    print(f"  → État: NORMAL → SUSPECTED")
                    print(f"  → Lancement du test de connectivité...")

                    # Test telnet asynchrone
                    threading.Thread(
                        target=self._trigger_level1_alert,
                        args=(event, log_ts_str),
                        daemon=True
                    ).start()

                elif self.state == "OUTAGE":
                    # Re-ping échoué → la coupure continue
                    duration_min = (datetime.now() - self.outage_start).seconds // 60
                    print(f"  → Coupure toujours active (durée: {duration_min} min) — IS re-retry échoué")

            # ── Détection Niveau 2 : Impact business (burst de 0042E) ──────
            elif code == "ISS.0134.0042E":
                self.windows["0042E"].append(now)
                self._clean_window("0042E", now)
                burst_count = len(self.windows["0042E"])

                if burst_count >= BURST_THRESHOLD and self.state in ("SUSPECTED", "OUTAGE"):
                    if self.state == "SUSPECTED":
                        self.state = "OUTAGE"
                        print(f"\n🔴 COUPURE CONFIRMÉE — {burst_count} virements impactés en {WINDOW_SECONDS}s")
                        threading.Thread(
                            target=self._trigger_level2_alert,
                            args=(event, burst_count, log_ts_str),
                            daemon=True
                        ).start()

            # ── Détection Niveau 3 : Boucle de retry infinie (0101E) ───────
            elif code == "ISS.0134.0101E":
                self.retry_timestamps.append(now)
                # Purge hors fenêtre
                cutoff = now - RETRY_LOOP_WINDOW
                while self.retry_timestamps and self.retry_timestamps[0] < cutoff:
                    self.retry_timestamps.popleft()

                if len(self.retry_timestamps) >= RETRY_LOOP_COUNT:
                    if self.state == "OUTAGE":
                        print(
                            f"\n♻️  [{log_ts_str}] Boucle retry détectée "
                            f"({len(self.retry_timestamps)} × 0101E en {RETRY_LOOP_WINDOW}s) — "
                            f"IS ne récupère pas seul"
                        )
                        self.retry_timestamps.clear()  # Reset pour éviter spam

            # ── Détection Niveau 1 : Trigger IS éteint (0058E) ─────────────
            elif code == "ISS.0134.0058E":
                print(f"  ⚠️  [{log_ts_str}] Trigger JMS arrêté — {event.get('service', 'N/A')}")

    def _trigger_level1_alert(self, event: dict, log_ts: str):
        """Alerte immédiate : ping JMS échoué + test connectivité."""
        alias = event.get("jms_alias")
        connectivity = check_all_endpoints(alias)

        # Si le test confirme la coupure réseau
        all_down = all(not r["reachable"] for r in connectivity.values())
        if all_down:
            self.state = "OUTAGE"

        details = {
            "Code erreur":      "ISS.0134.0020E",
            "Alias JMS ciblé":  alias or "N/A",
            "Timestamp log":    log_ts,
            "Heure détection":  datetime.now().strftime("%H:%M:%S"),
            "État endpoints":   "TOUS INJOIGNABLES" if all_down else "PARTIELLEMENT UP",
            "Action requise":   "Vérifier réseau / UM server / VPN GSIM",
            "Impact potentiel": "Virements instantanés interrompus",
        }
        alert_text = format_alert("COUPURE_RESEAU_JMS", details, connectivity)
        print(alert_text)
        self.alerts_sent.append({"type": "LEVEL_1", "ts": log_ts, "details": details})

    def _trigger_level2_alert(self, event: dict, burst_count: int, log_ts: str):
        """Alerte confirmée : impact business mesuré sur les virements."""
        connectivity = check_all_endpoints()
        duration_min = (datetime.now() - self.outage_start).seconds // 60 if self.outage_start else "?"

        details = {
            "Type":               "Impact virements confirmé",
            "Erreurs 0042E":      f"{burst_count} en {WINDOW_SECONDS}s",
            "Durée coupure":      f"{duration_min} minutes",
            "Timestamp début":    self.outage_start.strftime("%H:%M:%S") if self.outage_start else "?",
            "Services impactés":  "ABB_VIREMENTS_INSTANTANE / PacsOutRouter",
            "Trigger défaillant": event.get("service", "N/A"),
            "Action requise":     "Escalade ops + relance manuelle triggers",
        }
        alert_text = format_alert("IMPACT_VIREMENTS_CRITIQUES", details, connectivity)
        print(alert_text)
        self.alerts_sent.append({"type": "LEVEL_2", "ts": log_ts, "details": details})

    def get_summary(self) -> dict:
        """Retourne un résumé de l'état de détection."""
        return {
            "state": self.state,
            "outage_start": self.outage_start.isoformat() if self.outage_start else None,
            "alerts_count": len(self.alerts_sent),
            "alerts": self.alerts_sent,
        }


# ── Consumer principal ────────────────────────────────────────────────────────

def run_consumer():
    """
    Lance le consumer Kafka et alimente le moteur de détection IA.
    Tourne en continu jusqu'à interruption clavier.
    """
    print("=" * 70)
    print("  POC Snapshot 01 — Détecteur IA Coupures Réseau JMS")
    print("=" * 70)
    print(f"  Topic   : {KAFKA_TOPIC}")
    print(f"  Broker  : {KAFKA_BOOTSTRAP}")
    print(f"  Seuils  : burst={BURST_THRESHOLD}/min | retry_loop={RETRY_LOOP_COUNT}/3min")
    print("=" * 70)
    print()

    detector = NetworkOutageDetector()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=10000,  # 10s sans message → timeout
    )

    print("[CONSUMER] En attente d'événements depuis Kafka...\n")
    events_processed = 0

    try:
        for msg in consumer:
            event = msg.value
            events_processed += 1
            detector.process_event(event)

    except KeyboardInterrupt:
        print("\n[CONSUMER] Arrêt demandé.")
    finally:
        consumer.close()
        summary = detector.get_summary()
        print("\n── Résumé de la session ───────────────────────────────────────")
        print(f"  Événements traités : {events_processed}")
        print(f"  État final         : {summary['state']}")
        print(f"  Alertes levées     : {summary['alerts_count']}")
        for a in summary["alerts"]:
            print(f"    → [{a['type']}] {a['ts']}")
        print("───────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    run_consumer()
