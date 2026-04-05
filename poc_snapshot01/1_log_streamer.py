"""
POC Snapshot 01 — Simulateur de serveur IS (Virtual IS Log Streamer)
======================================================================
Ce script lit le fichier server.log et le rejoue ligne par ligne vers Kafka,
simulant un vrai serveur webMethods Integration Server en temps réel.

Corrections v2 :
    Gestion des lignes multilignes (continuations avec tabulation)
    Retry automatique si Kafka n'est pas encore prêt
    Affichage complet de tous les types d'événements
    Statistiques en temps réel par catégorie
    Compatibilité Python 3.8+

Modes disponibles :
    mode realtime  : rejoue avec les vrais intervalles de temps
    mode fast      : accéléré x speed (défaut x 50)
    mode burst     : uniquement la fenêtre de coupure (14:10→15:10)
"""

import re
import time
import json
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_TOPIC     = "webmethods.is.logs"
KAFKA_BOOTSTRAP = "localhost:9092"

LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) WEST "
    r"\[(?P<error_code>[A-Z0-9.]+)\] "
    r"\(tid=(?P<tid>\d+)\) "
    r"(?P<message>.+)"
)

NETWORK_CODES = {
    "ISS.0134.0020E": "JMS_PING_FAILURE",
    "ISS.0134.0016E": "JMS_CONNECTION_LOST",
    "ISS.0134.0058E": "JMS_TRIGGER_SHUTDOWN",
    "ISS.0134.0101E": "JMS_TRIGGER_RESTART_FAILED",
    "ISS.0134.0998E": "JMS_LISTENER_STOP",
}

BUSINESS_CODES = {
    "ISS.0134.0042E": "JMS_TRIGGER_FAILED",    # Trigger JMS échoue → virement KO
    "ISS.0134.0046E": "JMS_TRIGGER_RECOVERED",  # Trigger récupéré → bonne nouvelle
    "ISS.0134.0004E": "JMS_MESSAGE_REJECTED",   # Message rejeté
    "ISS.0134.0056E": "JMS_TRIGGER_SUSPENDED",  # Trigger suspendu
    "BPM.0102.0002E": "BPM_PROCESS_FAILURE",    # Processus BPM bloqué
    "BPM.0102.0376E": "BPM_STEP_FAILURE",       # Step BPM échoué
    "ISC.0076.0005E": "SERVICE_INVOKE_ERROR",    # Invocation service KO
    "ISP.0090.0003C": "SERVICE_CRITICAL",        # Erreur critique service (virement)
    "ART.0114.1007E": "ADAPTER_INVOKE_ERROR",    # Erreur adapter
}

ICONS = {
    "CRITICAL": "🔴",
    "ERROR":    "🟠",
    "WARNING":  "🟡",
    "INFO":     "🟢",
}


def classify_event(code):
    if code in NETWORK_CODES:
        # 0020E est le seul signal CRITIQUE réseau — c'est la cause racine
        if code == "ISS.0134.0020E":
            return "NETWORK", "CRITICAL"
        return "NETWORK", "ERROR"
    if code in BUSINESS_CODES:
        # Impact direct sur les virements ou processus critiques = ERROR
        if code in ("ISS.0134.0042E", "BPM.0102.0002E", "BPM.0102.0376E",
                    "ISP.0090.0003C"):
            return "BUSINESS", "ERROR"
        # Récupération = INFO (signal positif)
        if code == "ISS.0134.0046E":
            return "BUSINESS", "INFO"
        return "BUSINESS", "WARNING"
    # Codes non répertoriés : déduire depuis le suffixe IS standard
    if code.endswith("C"):
        return "SYSTEM", "CRITICAL"
    if code.endswith("E"):
        return "SYSTEM", "ERROR"
    return "SYSTEM", "INFO"


def parse_log_line(raw_line, continuation_buffer):
    """
    Parse une ligne IS en gerant les continuations multilignes.
    Retourne (event_complet_ou_None, nouveau_buffer).
    """
    line = raw_line.rstrip("\r\n")

    # Ligne de continuation (tab ou espace en debut)
    if line.startswith("\t") or (line.startswith("  ") and line.strip()):
        if continuation_buffer:
            continuation_buffer["message"] += " | " + line.strip()
        return None, continuation_buffer

    # Ligne vide
    if not line.strip():
        return None, continuation_buffer

    m = LOG_PATTERN.match(line)
    if not m:
        return None, continuation_buffer

    code    = m.group("error_code")
    message = m.group("message").strip()

    alias_match   = re.search(r'"(GSIMTConnection[^"]*)"', message)
    jms_alias     = alias_match.group(1) if alias_match else None

    trigger_match = re.search(r'JMS [Tt]rigger ([^\s:]+)', message)
    service       = trigger_match.group(1) if trigger_match else None

    event_type, severity = classify_event(code)

    new_event = {
        "timestamp":   m.group("timestamp"),
        "error_code":  code,
        "tid":         m.group("tid"),
        "message":     message[:400],
        "event_type":  event_type,
        "severity":    severity,
        "jms_alias":   jms_alias,
        "service":     service,
        "streamed_at": datetime.now().isoformat(),
    }

    previous_event      = continuation_buffer
    return previous_event, new_event


def create_producer(max_retries=10, retry_delay=3):
    """Cree un KafkaProducer avec retry automatique au démarrage."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(
                    v, ensure_ascii=False
                ).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                request_timeout_ms=10000,
            )
            return producer
        except NoBrokersAvailable:
            print(
                "  [STREAMER] Kafka pas encore pret "
                "({}/{}) - retry dans {}s...".format(attempt, max_retries, retry_delay)
            )
            time.sleep(retry_delay)
    raise RuntimeError(
        "Impossible de se connecter a Kafka apres {} tentatives.\n"
        "Verifier que Docker est démarre : docker compose up -d".format(max_retries)
    )


def print_event(event, lines_sent):
    """Affiche l'événement dans la console selon sa sévérité."""
    icon     = ICONS.get(event["severity"], "⚪")
    ts       = event["timestamp"][11:]
    code     = event["error_code"]
    target   = event["jms_alias"] or event["service"] or ""
    category = event["event_type"]

    # NETWORK et CRITICAL : toujours afficher
    if event["event_type"] == "NETWORK" or event["severity"] == "CRITICAL":
        print("  {} [{}] {:<22} {:<10} {}".format(icon, ts, code, category, target))

    # BUSINESS ERROR : afficher 1 sur 10
    elif event["event_type"] == "BUSINESS" and event["severity"] == "ERROR":
        if lines_sent % 10 == 0:
            print("  {} [{}] {:<22} {:<10} {}".format(icon, ts, code, category, target))

    # Compteur general tous les 50
    elif lines_sent % 50 == 0:
        print("  -- [{}] ... {} événements envoyés".format(ts, lines_sent))


def print_stats(stats, lines_sent, lines_skipped):
    print("-" * 60)
    print("  Événements envoyés  : {}".format(lines_sent))
    print("  Lignes ignorees     : {}".format(lines_skipped))
    print("  Repartition :")
    for category, count in sorted(stats.items(), key=lambda x: -x[1]):
        bar = chr(9608) * min(count // 10, 30)
        print("    {:<12} {:>4}  {}".format(category, count, bar))
    print("-" * 60)


def stream_logs(log_path, mode="fast", speed=50.0):
    print("=" * 60)
    print("  POC Snapshot 01 -- Virtual IS Log Streamer")
    print("=" * 60)
    print("  Fichier : {}".format(log_path))
    print("  Topic   : {}".format(KAFKA_TOPIC))
    print("  Mode    : {}  |  Acceleration : x{}".format(mode, speed))
    print("=" * 60)
    print("\n[STREAMER] Connexion a Kafka ({})...".format(KAFKA_BOOTSTRAP))

    producer = create_producer()
    print("[STREAMER] Connecte OK\n")

    lines_sent          = 0
    lines_skipped       = 0
    prev_log_ts         = None
    stats               = {}
    continuation_buffer = None
    burst_start         = "14:10"
    burst_end           = "15:10"

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()

    all_lines.append("")  # sentinelle pour flush du dernier buffer

    for raw_line in all_lines:
        previous_event, continuation_buffer = parse_log_line(
            raw_line, continuation_buffer
        )

        event = previous_event
        if event is None:
            continue

        # Filtrage mode burst
        if mode == "burst":
            hm = event["timestamp"][11:16]
            if hm < burst_start or hm > burst_end:
                lines_skipped += 1
                continue

        # Délai inter-messages
        log_ts = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        if prev_log_ts is not None and mode in ("realtime", "fast", "burst"):
            delta_s = (log_ts - prev_log_ts).total_seconds()
            if delta_s > 0:
                factor  = 1.0 if mode == "realtime" else speed
                sleep_s = min(delta_s / factor, 2.0)
                time.sleep(sleep_s)
        prev_log_ts = log_ts

        # Envoi Kafka
        key = event["jms_alias"] or event["error_code"]
        producer.send(KAFKA_TOPIC, key=key, value=event)
        lines_sent += 1

        cat = event["event_type"]
        stats[cat] = stats.get(cat, 0) + 1

        print_event(event, lines_sent)

    producer.flush()
    print("\n[STREAMER] Stream termine")
    print_stats(stats, lines_sent, lines_skipped)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Virtual IS Log Streamer -> Kafka")
    parser.add_argument(
        "--log",
        default="data/server.log.20260324.1",
        help="Chemin vers le fichier de log IS",
    )
    parser.add_argument(
        "--mode",
        choices=["realtime", "fast", "burst"],
        default="fast",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=50.0,
        help="Facteur d acceleration",
    )
    args = parser.parse_args()
    stream_logs(args.log, args.mode, args.speed)