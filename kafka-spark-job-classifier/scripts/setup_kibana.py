"""Crée la data view Kibana « job-offers* » (champ temporel @timestamp).

Usage : python scripts/setup_kibana.py   (Kibana doit être démarré : http://localhost:5601)
"""
import os
import sys

import requests

KIBANA = os.getenv("KIBANA_URL", "http://localhost:5601")

resp = requests.post(
    f"{KIBANA}/api/data_views/data_view",
    headers={"kbn-xsrf": "true", "Content-Type": "application/json"},
    json={"data_view": {"title": "job-offers*", "name": "Offres d'emploi",
                        "timeFieldName": "@timestamp"}},
    timeout=15,
)
if resp.ok:
    print("Data view « Offres d'emploi » créée.")
elif resp.status_code == 400 and "Duplicate" in resp.text:
    print("La data view existe déjà.")
else:
    print(f"Erreur {resp.status_code} : {resp.text}")
    sys.exit(1)
