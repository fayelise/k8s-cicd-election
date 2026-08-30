# CI/CD auto-résilient sur cluster Kubernetes hybride

Système de déploiement continu où l'orchestrateur lui-même est réparti sur
les 3 nœuds hybrides du cluster, avec élection de leader codée à la main
(objet `Lease` Kubernetes). Seul le leader pilote le pipeline (surveillance
de GitHub Container Registry + déploiement) ; en cas de panne du leader, un
autre réplica prend automatiquement le relais.

## Architecture

```
GitHub (source de vérité)
  └─ push sur main → GitHub Actions (build + tests + push image GHCR)
                                │
                        (polling toutes les 20s)
                                ▼
     Cluster Kubernetes hybride (opium / opium1 / opium2)
        cicd-orchestrator (3 réplicas, 1 par nœud, élection via Lease)
                                │  (seul le leader agit)
                                ▼
                     Deployment "demo-api" (l'application)
```

## Mise en place, étape par étape

### 1. Créer le dépôt GitHub

Crée un dépôt (ex. `k8s-cicd-election`), et pousse-y l'intégralité de ce
dossier (`demo-api/`, `orchestrator/`, `k8s/`, `.github/`).

### 2. Autoriser GitHub Actions à publier sur GHCR

Dans le dépôt : **Settings → Actions → General → Workflow permissions**,
sélectionne **"Read and write permissions"**. Sans ça, le job `packages: write`
du pipeline échouera à l'étape de publication de l'image.

### 3. Remplacer les placeholders

Dans les fichiers suivants, remplace `REMPLACER_USER_GITHUB` et
`REMPLACER_REPO` par ton nom d'utilisateur GitHub et le nom du dépôt :

- `k8s/orchestrator-deployment.yaml`
- `k8s/demo-api-deployment.yaml`

### 4. Premier build (déclenche les pipelines)

```bash
git add .
git commit -m "Initial commit"
git push origin main
```

Les deux workflows (`ci-demo-api.yml` et `ci-orchestrator.yml`) se déclenchent
automatiquement et publient les deux images sur GHCR. Vérifie dans l'onglet
**Actions** du dépôt que les deux passent au vert.

### 5. Rendre les images publiques

Par défaut, un paquet GHCR est privé. Comme l'orchestrateur interroge GHCR de
façon anonyme (pas d'identifiants stockés dans le cluster), les deux paquets
doivent être publics : va dans **ton profil GitHub → Packages**, ouvre
`demo-api` puis `orchestrator`, et dans **Package settings → Change visibility**,
sélectionne **Public**.

### 6. Déployer sur le cluster (depuis opium)

```bash
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/demo-api-deployment.yaml
kubectl apply -f k8s/orchestrator-deployment.yaml
```

Vérifie que tout tourne, avec bien 1 pod orchestrateur par nœud :

```bash
kubectl get pods -o wide
```

### 7. Vérifier l'élection

```bash
kubectl get lease cicd-orchestrator-leader -o yaml
```

Le champ `holderIdentity` indique quel pod est actuellement leader. Confirme
via l'API du pod correspondant :

```bash
kubectl port-forward pod/<nom-du-pod-leader> 8080:8080
curl localhost:8080/status
```

