"""
Orchestrateur de déploiement continu, avec élection de leader codée à la main.

Principe :
- Ce programme tourne en plusieurs réplicas (un par nœud du cluster hybride).
- Les réplicas s'accordent via un objet Kubernetes Lease (coordination.k8s.io/v1)
  pour désigner un leader unique — exactement le même mécanisme que celui
  utilisé nativement par kube-controller-manager / kube-scheduler.
- Seul le pod leader exécute la boucle de réconciliation : il interroge
  GitHub Container Registry (GHCR) à intervalle régulier pour détecter une
  nouvelle image publiée par le pipeline CI, et met à jour le Deployment de
  l'application de démo en conséquence.
- Si le pod leader tombe (nœud en panne), son bail (Lease) expire et un
  autre réplica prend automatiquement le relais, sans intervention manuelle.
"""

import os
import socket
import time
import threading
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("orchestrator")

# --- Configuration (variables d'environnement injectées par le Deployment) ---
NAMESPACE = os.environ.get("NAMESPACE", "default")
LEASE_NAME = os.environ.get("LEASE_NAME", "cicd-orchestrator-leader")
POD_NAME = os.environ.get("POD_NAME", socket.gethostname())
LEASE_DURATION = int(os.environ.get("LEASE_DURATION_SECONDS", "15"))
RENEW_INTERVAL = max(LEASE_DURATION / 3, 1)

GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "")
IMAGE_REPO = os.environ["IMAGE_REPO"]  # ex: mon-user/mon-repo/demo-api
DEMO_DEPLOYMENT = os.environ.get("DEMO_DEPLOYMENT", "demo-api")
DEMO_CONTAINER = os.environ.get("DEMO_CONTAINER", "demo-api")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "20"))

# --- État partagé entre les threads (élection + réconciliation) ---
is_leader = False
last_deployed_digest = None
last_error = None

# --- Client Kubernetes : utilise le ServiceAccount monté dans le pod ---
config.load_incluster_config()
coord_api = client.CoordinationV1Api()
apps_api = client.AppsV1Api()


def try_acquire_or_renew_lease():
    """
    Un cycle de l'algorithme d'élection :
      1. Si le Lease n'existe pas encore -> on le crée et on devient leader.
      2. Si on est déjà le holder -> on renouvelle notre bail (heartbeat).
      3. Si le holder actuel n'a pas renouvelé à temps (bail expiré) -> on
         tente de prendre sa place.
      4. Sinon -> un leader actif existe déjà, on reste en veille.
    La resourceVersion de l'objet Lease sert de verrou optimiste : si deux
    pods tentent la bascule en même temps, un seul des deux appels
    replace_namespaced_lease réussira, l'autre échouera proprement (409 Conflict).
    """
    global is_leader
    now = datetime.now(timezone.utc)

    try:
        lease = coord_api.read_namespaced_lease(LEASE_NAME, NAMESPACE)
    except ApiException as e:
        if e.status != 404:
            raise
        # Aucun lease n'existe encore : premier démarrage du cluster.
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=LEASE_NAME, namespace=NAMESPACE),
            spec=client.V1LeaseSpec(
                holder_identity=POD_NAME,
                lease_duration_seconds=LEASE_DURATION,
                acquire_time=now,
                renew_time=now,
            ),
        )
        try:
            coord_api.create_namespaced_lease(NAMESPACE, body)
            is_leader = True
            log.info("Lease créé : je deviens LEADER (%s)", POD_NAME)
        except ApiException:
            log.info("Un autre pod a créé le lease en premier, je reste en veille")
            is_leader = False
        return

    holder = lease.spec.holder_identity
    renew_time = lease.spec.renew_time
    expired = (now - renew_time).total_seconds() > LEASE_DURATION

    if holder == POD_NAME:
        # Je suis déjà leader : renouvellement du bail (heartbeat).
        lease.spec.renew_time = now
        try:
            coord_api.replace_namespaced_lease(LEASE_NAME, NAMESPACE, lease)
            is_leader = True
        except ApiException:
            log.warning("Échec du renouvellement du lease (conflit) : je redeviens candidat")
            is_leader = False
        return

    if expired:
        # Le leader actuel semble en panne (bail expiré) : on tente la bascule.
        lease.spec.holder_identity = POD_NAME
        lease.spec.acquire_time = now
        lease.spec.renew_time = now
        try:
            coord_api.replace_namespaced_lease(LEASE_NAME, NAMESPACE, lease)
            is_leader = True
            log.info("Lease expiré (ancien holder : %s) : je prends le relais, LEADER = %s", holder, POD_NAME)
        except ApiException:
            log.info("Un autre pod a repris le lease en premier, je reste en veille")
            is_leader = False
        return

    # Un leader actif et à jour existe déjà, et ce n'est pas moi.
    if is_leader:
        log.info("Je perds le rôle de leader (holder actuel : %s)", holder)
    is_leader = False


def election_loop():
    """Boucle de fond exécutée par TOUS les réplicas, en continu."""
    while True:
        try:
            try_acquire_or_renew_lease()
        except Exception:
            log.exception("Erreur pendant le cycle d'élection")
        time.sleep(RENEW_INTERVAL)


def get_ghcr_token():
    """Jeton anonyme en lecture seule pour interroger un dépôt GHCR public."""
    url = f"https://ghcr.io/token?service=ghcr.io&scope=repository:{IMAGE_REPO}:pull"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["token"]


def get_latest_digest():
    """Interroge GHCR pour obtenir le digest actuel de l'image taguée 'latest'."""
    token = get_ghcr_token()
    url = f"https://ghcr.io/v2/{IMAGE_REPO}/manifests/latest"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.headers["Docker-Content-Digest"]


def deploy_new_image(digest):
    """
    Met à jour le Deployment de l'application de démo avec la nouvelle image,
    référencée par son digest (immuable) plutôt que par le tag 'latest' :
    c'est ce qui garantit que Kubernetes déclenche bien un nouveau rollout
    (un tag identique ne changerait pas la spec du pod et ne redéploierait rien).
    """
    image_ref = f"ghcr.io/{IMAGE_REPO}@{digest}"
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [{"name": DEMO_CONTAINER, "image": image_ref}]
                }
            }
        }
    }
    apps_api.patch_namespaced_deployment(DEMO_DEPLOYMENT, NAMESPACE, patch)
    log.info("Déploiement mis à jour avec l'image %s", image_ref)


def reconciliation_loop():
    """
    Boucle exécutée UNIQUEMENT quand ce pod est leader : c'est elle qui
    matérialise le pilotage du pipeline CD (GitHub -> cluster).
    """
    global last_deployed_digest, last_error
    while True:
        if is_leader:
            try:
                digest = get_latest_digest()
                if digest != last_deployed_digest:
                    log.info("Nouvelle image détectée sur GHCR (%s)", digest)
                    deploy_new_image(digest)
                    last_deployed_digest = digest
                else:
                    log.info("Aucun changement détecté (digest actuel : %s)", digest)
                last_error = None
            except Exception as e:
                last_error = str(e)
                log.exception("Erreur pendant la réconciliation")
        time.sleep(POLL_INTERVAL)


# --- Serveur HTTP minimal, pour l'observation pendant la démo ---
app = Flask(__name__)


@app.route("/status")
def status():
    return jsonify(
        pod=POD_NAME,
        is_leader=is_leader,
        last_deployed_digest=last_deployed_digest,
        last_error=last_error,
    )


@app.route("/healthz")
def healthz():
    return jsonify(status="ok")


if __name__ == "__main__":
    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=reconciliation_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)
