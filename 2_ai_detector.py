import json
import time
import socket
import threading
from collections import deque, defaultdict
from datetime import datetime

from kafka import KafkaConsumer

KAFKA_TOPIC = "webmethods.is.logs"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_GROUP = "ai-network-detector"

WINDOW_SECONDS = 60
BURST_THRESHOLD = 10
RETRY_LOOP_COUNT = 6
RETRY_LOOP_WINDOW = 180

JMS_ENDPOINTS = {
    "GSIMTConnectionAlias": ("172.22.15.83", 9000),
    "GSIMTConnectionAliasDirect": ("172.22.15.83", 9000),
    "IS_Admin": ("172.22.15.83", 5555),
}


def telnet_check(host: str, port: int, timeout: float = 3.0) -> dict:
    start = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency_ms = (time.monotonic() - start) * 1000
            return {"reachable": True, "latency_ms": round(latency_ms, 1), "error": None}
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        return {"reachable": False, "latency_ms": None, "error": str(e)}


def check_all_endpoints(alias: str | None = None) -> dict:
    targets = {alias: JMS_ENDPOINTS[alias]} if alias and alias in JMS_ENDPOINTS else JMS_ENDPOINTS
    results = {}
    for name, (host, port) in targets.items():
        result = telnet_check(host, port)
        results[name] = {"host": host, "port": port, **result}
    return results


def format_alert(alert_type: str, details: dict, connectivity: dict) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = ["=" * 70, f"ALERTE [{alert_type}] — {ts}", "=" * 70]
    for k, v in details.items():
        lines.append(f"{k:<25}: {v}")
    lines.append("")
    lines.append("Résultats connectivité :")
    for name, r in connectivity.items():
        status = "OK" if r["reachable"] else "UNREACHABLE"
        latency = f"{r['latency_ms']}ms" if r["latency_ms"] is not None else "N/A"
        lines.append(f"  {status} {name} ({r['host']}:{r['port']}) — {latency}")
    lines.append("=" * 70)
    return "\n".join(lines)


class NetworkOutageDetector:
    def __init__(self):
        self.state = "NORMAL"
        self.outage_start = None
        self.alerts_sent = []
        self.windows = defaultdict(deque)
        self.retry_timestamps = deque()
        self._lock = threading.Lock()

    def _clean_window(self, code: str, now: float):
        cutoff = now - WINDOW_SECONDS
        while self.windows[code] and self.windows[code][0] < cutoff:
            self.windows[code].popleft()

    def process_event(self, event: dict):
        code = event.get("error_code", "")
        alias = event.get("jms_alias")
        now = time.time()
        log_ts_str = event.get("timestamp", "")

        with self._lock:
            if code == "ISS.0134.0020E":
                self.windows["0020E"].append(now)
                if self.state == "NORMAL":
                    self.state = "SUSPECTED"
                    self.outage_start = datetime.now()
                    threading.Thread(
                        target=self._trigger_level1_alert,
                        args=(event, log_ts_str),
                        daemon=True,
                    ).start()

            elif code == "ISS.0134.0042E":
                self.windows["0042E"].append(now)
                self._clean_window("0042E", now)
                burst_count = len(self.windows["0042E"])
                if burst_count >= BURST_THRESHOLD and self.state in ("SUSPECTED", "OUTAGE"):
                    if self.state == "SUSPECTED":
                        self.state = "OUTAGE"
                        threading.Thread(
                            target=self._trigger_level2_alert,
                            args=(event, burst_count, log_ts_str),
                            daemon=True,
                        ).start()

            elif code == "ISS.0134.0101E":
                self.retry_timestamps.append(now)
                cutoff = now - RETRY_LOOP_WINDOW
                while self.retry_timestamps and self.retry_timestamps[0] < cutoff:
                    self.retry_timestamps.popleft()
                if len(self.retry_timestamps) >= RETRY_LOOP_COUNT and self.state == "OUTAGE":
                    self.retry_timestamps.clear()

            elif code == "ISS.0134.0058E":
                pass

    def _trigger_level1_alert(self, event: dict, log_ts: str):
        alias = event.get("jms_alias")
        connectivity = check_all_endpoints(alias)
        all_down = all(not r["reachable"] for r in connectivity.values())
        if all_down:
            self.state = "OUTAGE"
        details = {
            "Code erreur": "ISS.0134.0020E",
            "Alias JMS ciblé": alias or "N/A",
            "Timestamp log": log_ts,
            "Heure détection": datetime.now().strftime("%H:%M:%S"),
            "État endpoints": "TOUS INJOIGNABLES" if all_down else "PARTIELLEMENT UP",
            "Action requise": "Vérifier réseau / UM server / VPN GSIM",
            "Impact potentiel": "Virements instantanés interrompus",
        }
        alert_text = format_alert("COUPURE_RESEAU_JMS", details, connectivity)
        print(alert_text)
        self.alerts_sent.append(
            {"type": "LEVEL_1", "ts": log_ts, "details": details, "connectivity": connectivity}
        )

    def _trigger_level2_alert(self, event: dict, burst_count: int, log_ts: str):
        connectivity = check_all_endpoints()
        duration_min = int((datetime.now() - self.outage_start).total_seconds() // 60) if self.outage_start else 0
        details = {
            "Type": "Impact virements confirmé",
            "Erreurs 0042E": f"{burst_count} en {WINDOW_SECONDS}s",
            "Durée coupure": f"{duration_min} minutes",
            "Timestamp début": self.outage_start.strftime("%H:%M:%S") if self.outage_start else "?",
            "Services impactés": "ABB_VIREMENTS_INSTANTANE / PacsOutRouter",
            "Trigger défaillant": event.get("service", "N/A"),
            "Action requise": "Escalade ops + relance manuelle triggers",
        }
        alert_text = format_alert("IMPACT_VIREMENTS_CRITIQUES", details, connectivity)
        print(alert_text)
        self.alerts_sent.append(
            {"type": "LEVEL_2", "ts": log_ts, "details": details, "connectivity": connectivity}
        )

    def get_summary(self) -> dict:
        return {
            "state": self.state,
            "outage_start": self.outage_start.isoformat() if self.outage_start else None,
            "alerts_count": len(self.alerts_sent),
            "alerts": self.alerts_sent,
        }


def run_consumer():
    print("=" * 70)
    print("POC Snapshot 01 — Détecteur IA Coupures Réseau JMS")
    print("=" * 70)
    print(f"Topic   : {KAFKA_TOPIC}")
    print(f"Broker  : {KAFKA_BOOTSTRAP}")
    print(f"Seuils  : burst={BURST_THRESHOLD}/min | retry_loop={RETRY_LOOP_COUNT}/3min")
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
        consumer_timeout_ms=10000,
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
        print(f"Événements traités : {events_processed}")
        print(f"État final         : {summary['state']}")
        print(f"Alertes levées      : {summary['alerts_count']}")
        for a in summary["alerts"]:
            print(f"→ [{a['type']}] {a['ts']}")
        print("───────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    run_consumer()