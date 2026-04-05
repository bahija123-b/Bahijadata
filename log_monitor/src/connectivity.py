import socket
import datetime
import pandas as pd

def check_service(host, port, timeout=5):
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        status = "EN LIGNE"
        reachable = True
    except:
        status = "HORS LIGNE"
        reachable = False
    result = {
        'host': host,
        'port': port,
        'status': status,
        'reachable': reachable,
        'checked_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    print(f"[{result['checked_at']}] {host}:{port} --> {status}")
    return result

def check_all(services):
    results = [check_service(s['host'], s['port']) for s in services]
    df = pd.DataFrame(results)
    df.to_csv('output/connectivity_report.csv', index=False)
    print("Rapport connectivite sauvegarde dans output/connectivity_report.csv")
    return df

if __name__ == '__main__':
    services = [
        {'host': '172.18.41.162', 'port': 5555},   # webMethods IS port par defaut
        {'host': '172.18.41.162', 'port': 9999},   # JMS/MQ
        {'host': '172.18.41.162', 'port': 8080},   # HTTP
    ]
    check_all(services)