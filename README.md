# POC Big Data — Snapshot 01
## Détection IA des coupures réseau JMS / Virements Instantanés
**Al Barid Bank — Intégration Système | Mars 2026**

---

## Problématique résolue

Le 24 mars 2026 à 14h10:43, une micro-coupure réseau sur les providers JMS
(`GSIMTConnectionAlias`, `GSIMTConnectionAliasDirect`) a causé :
- 693 échecs de triggers sur les virements instantanés
- 231 tentatives de restart inutiles toutes les 30s
- 47 minutes d'impact opérationnel sans alerte automatique

**Ce POC détecte cet incident en < 30 secondes.**

---

## Architecture Snapshot 01

```
┌────────────────────────────────────────────────────────────────┐
│               Serveur IS Virtuel (Simulation)                  │
│  server.log.20260324.1  →  1_log_streamer.py  →  Kafka Topic  │
│                             (replay contrôlé)                  │
└────────────────────────────────────────────────────────────────┘
                               ↓ Kafka
┌────────────────────────────────────────────────────────────────┐
│              Moteur de Détection IA  (2_ai_detector.py)        │
│                                                                │
│  Niveau 1 : ISS.0134.0020E → Alerte immédiate + Telnet        │
│  Niveau 2 : Burst 0042E > 10/min → Confirmation impact        │
│  Niveau 3 : Boucle 0101E > 6/3min → Intervention requise      │
└────────────────────────────────────────────────────────────────┘
                               ↓
              Alertes console / email / SMS (à brancher)
```

---

## Lancement pas à pas

### Prérequis
- Docker Desktop installé
- Python 3.10+

### Étape 1 — Démarrer Kafka

```bash
docker-compose up -d
# Attendre ~15s que Kafka soit prêt
```

### Étape 2 — Installer les dépendances Python

```bash
pip install -r requirements.txt
```

### Étape 3 — Lancer le détecteur IA (Terminal 1)

```bash
python 2_ai_detector.py
```

### Étape 4 — Lancer le simulateur IS (Terminal 2)

```bash
# Mode rapide (× 50) — recommandé pour démo
python 1_log_streamer.py --log data/server.log.20260324.1 --mode fast --speed 50

# Mode burst — rejoue uniquement la fenêtre de coupure (14:10 → 15:10) × 10
python 1_log_streamer.py --log data/server.log.20260324.1 --mode burst --speed 10

# Mode temps réel — fidèle à la production (long)
python 1_log_streamer.py --log data/server.log.20260324.1 --mode realtime
```

---

## Ce que vous verrez

```
[CONSUMER] En attente d'événements depuis Kafka...

⚡ [2026-03-24 14:10:43] ISS.0134.0020E détecté sur GSIMTConnectionAlias
  → État: NORMAL → SUSPECTED
  → Lancement du test de connectivité...
  [TELNET] GSIMTConnectionAlias (172.22.15.83:9000) → ❌ UNREACHABLE | latence: N/A
  [TELNET] GSIMTConnectionAliasDirect (172.22.15.83:9000) → ❌ UNREACHABLE | latence: N/A
  [TELNET] IS_Admin (172.22.15.83:5555) → ✅ OK | latence: 4.2ms

======================================================================
🚨 ALERTE [COUPURE_RESEAU_JMS] — 2026-03-24 14:10:44
======================================================================
  Code erreur             : ISS.0134.0020E
  Alias JMS ciblé         : GSIMTConnectionAlias
  État endpoints          : TOUS INJOIGNABLES
  Action requise          : Vérifier réseau / UM server / VPN GSIM
  Impact potentiel        : Virements instantanés interrompus
======================================================================

🔴 COUPURE CONFIRMÉE — 12 virements impactés en 60s
```

---

## Codes d'erreur IS surveillés

| Code | Signification | Niveau |
|------|--------------|--------|
| ISS.0134.0020E | Ping JMS échoué (root cause) | CRITIQUE |
| ISS.0134.0058E | Trigger JMS arrêté | ERREUR |
| ISS.0134.0101E | Restart trigger échoué (boucle) | ERREUR |
| ISS.0134.0042E | Trigger JMS échoue (virements) | IMPACT MÉTIER |
| BPM.0102.0002E | Processus BPM bloqué | IMPACT MÉTIER |

---

## Prochaines étapes (Snapshot 02)

- [ ] Brancher un vrai alerting email/SMS (SMTP / Twilio)
- [ ] Modèle ML (Isolation Forest) pour détecter les anomalies non-prévues
- [ ] Dashboard Streamlit temps réel consommant Kafka
- [ ] Test avec les vraies IPs du serveur webMethods (172.22.15.83:5555)
