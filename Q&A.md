# Questions et réponses: soutenance Head of Data et MLOps (groupe X)

Ce document rassemble les argumentaires techniques, financiers et organisationnels mobilisés dans le support interactif [index.html](index.html), la synthèse exécutive [note-architecture-cio-cdo.md](note-architecture-cio-cdo.md) et la documentation générale [README.md](README.md). Les spécifications détaillées des 11 invariants d'architecture (`AD-1` à `AD-11`), du cahier d'exigences produit et des 22 stories d'implémentation sont réservées à la démonstration commentée en séance.

## 1. Pourquoi écarter le tout-Fabric et Spark au profit d'une architecture micro-services sur Azure hors Fabric?

Le mandat prévoit d'accompagner la modernisation analytique amorcée côté Commerce autour de l'écosystème Microsoft. En comité de direction, l'enjeu consiste à préserver l'engagement des sponsors métiers tout en démontrant chiffres à l'appui pourquoi unifier toute l'ingénierie et le MLOps sous Microsoft Fabric et Spark créerait une plateforme rigide et surdimensionnée.

### L'épreuve des volumes et de la vélocité réels

Le groupe X opère environ 15 500 événements et accueille près de 650 000 participants par an auprès d'environ 5 200 entreprises clientes sur près de 80 sites européens. Même en consolidant cinq années complètes d'historique (`Lxxxx`, `Dynamics 365`, `Cegid`, `Esker`, `Skello`), le volume structuré utile reste inférieur à **50 Go**. Côté vélocité, les processus métiers tournent au rythme quotidien (scoring matinal d'options, marges, bilans carbone) avec un seul flux au quart d'heure sur les disponibilités de salles.

Déployer un moteur distribué Spark pour traiter moins de 50 Go revient à payer des temps d'allumage de cluster, une complexité de débogage PySpark superflue et un modèle de facturation aux Capacity Units exposé au bridage diurne (*throttling*) des rapports métiers dès qu'un traitement Python déborde. Sur le plan financier, dans un groupe dont le taux d'EBITDA France s'est contracté d'environ 14 % à 5,5 % entre 2022 et 2025, engager une capacité réservée `Fabric F64` (~5 240 € HT/mois) doublée d'`Azure API Management Standard v2` (~650 € HT/mois) immobiliserait près de 70 000 € HT par an uniquement en infrastructure. Même l'argument des licences `Power BI Pro` économisées au-delà de 200 lecteurs tombe: les quelque 150 conseillers commerciaux B2B consomment les recommandations directement dans `Dynamics 365` et les cuisines reçoivent leurs alertes J-5 dans `Teams` sans ouvrir d'outil BI.

### Pourquoi Azure hors Fabric plutôt qu'AWS Paris

À périmètre identique (`PostgreSQL` managé 2 vCPU / 8 Go RAM + conteneurs Docker permanents et éphémères pour `Airflow`, `FastAPI`, `MLflow`, `Streamlit` et `dlt` + stockage et journaux), AWS Paris (`eu-west-3`, ~330 € HT/mois sur instances ARM Graviton) et **Azure hors Fabric** (`France Central`, **~340 à 405 € HT/mois** avec `PostgreSQL Flexible Server` et `Azure Container Apps`) affichent moins de 75 € d'écart mensuel.

Puisque l'annuaire `Microsoft Entra ID`, `Dynamics 365 Sales` et `Power BI` résident déjà sur Azure, déployer sur Azure hors Fabric évite de construire et maintenir un tunnel VPN inter-cloud, une fédération IAM croisée et une passerelle de données Power BI. Toute la plateforme restant packagée en conteneurs Docker standards sous `Terraform`, la portabilité vers AWS demeure totale sans enfermement propriétaire, et l'authentification OAuth2 native sur l'ingress `Container Apps` économise les 650 € mensuels d'`Azure API Management`.

### La trajectoire diplomatique: démarrer en option 3 cantonnée, arbitrer l'option 1 à six mois

L'équipe BI commerciale exploite `Power BI` de longue date et a engagé un premier chantier pilote sur cet environnement avec un cabinet partenaire. Exiger l'arrêt immédiat de leur espace braquerait un allié métier précieux dès la prise de poste.

La posture retenue sépare donc la vitrine BI du réacteur d'ingénierie et de MLOps. Dès le jour 1, 100 % de l'ingestion, du stockage (`PostgreSQL`), des transformations (`dbt Core`), de l'orchestration (`Airflow`), du lignage (`OpenLineage`) et des micro-services MLOps (`MLflow`, `FastAPI`) sont construits hors de Fabric sur Azure (~340 à 405 € HT/mois). En vague 1 (option 3), l'espace BI commercial existant est maintenu sur sa micro-capacité plafonnée (<= 300 € HT/mois, soit un budget global de **~650 à 700 € HT/mois**) comme simple couche sémantique en lecture seule sur les vues Gold de `PostgreSQL`, avec interdiction d'y héberger du Spark ou des pipelines Data Factory. En semaine 19, un jalon FinOps factuel avec le DAF évalue si cette micro-capacité apporte une valeur mesurable face à une connexion directe de licences `Power BI Pro` sur un réplica de lecture `PostgreSQL` (option 1 pure), bascule réalisable en une demi-journée sans réécrire une seule règle métier.

## 2. Qu'est-ce que le PMS interne Lxxxx et comment l'exploiter sans risque RGPD ni surcharge?

Le sigle PMS désigne un *Property Management System*, c'est-à-dire le progiciel opérationnel utilisé dans chaque établissement pour gérer les plannings de salles, l'attribution des chambres, les arrivées et les prestations d'accueil.

### Pourquoi le groupe a développé son propre PMS

Les progiciels hôteliers du marché (Opera, Mews) facturent chaque prestation à l'acte (nuitée, boisson, location horaire de salle), alors que le modèle économique du groupe repose sur un forfait journalier global tout-compris par participant. Pour modéliser cette économie forfaitaire sans développements externes prohibitifs, la Direction digitale a conçu en interne **`Lxxxx`**, maintenu par une équipe dédiée d'une dizaine de développeurs au siège.

Pour le pôle data, cette pleine propriété intellectuelle supprime toute redevance d'extraction, tout bridage d'API éditeur et facilite la mise en place de contrats de données directement avec l'équipe produit interne.

### Protection RGPD dès la mémoire vive et extraction non intrusive

`Lxxxx` traite chaque année près de 650 000 participants en sous-traitance B2B (article 28 du RGPD), incluant des régimes alimentaires médicaux ou confessionnels et des besoins d'accessibilité relevant de l'article 9. Le conteneur d'ingestion Python `dlt` hache en mémoire vive tout identifiant direct en `SHA-256` avec un sel stocké dans `Azure Key Vault` (époque de sel figée par `stay_id`, zéro table de correspondance) avant même l'écriture dans le schéma Bronze de `PostgreSQL`, et agrège les contraintes de service à la seule maille du séjour.

Sur le plan opérationnel, l'audit de deux jours mené en semaine 1 avec le référent technique de `Lxxxx` tranchera entre lecture CDC et requêtes incrémentales toutes les 15 minutes sur un réplica en lecture seule afin de garantir zéro impact sur la production des établissements.

## 3. Pourquoi associer Apache Airflow, dbt Core et le standard OpenLineage?

Pour une équipe qui maîtrise déjà **Apache Airflow** en production, introduire un orchestrateur inconnu dès le premier jour créerait un risque opérationnel inutile. À l'échelle de quelques dizaines de DAGs quotidiens et d'un flux aux 15 minutes, Airflow conteneurisé couvre sans effort l'ordonnancement en fuseau dynamique `Europe/Paris` et l'envoi d'alertes webhooks `Teams`.

Airflow étant toutefois pensé autour des tâches plutôt que des tables, l'architecture lui adjoint deux briques complémentaires. Toutes les transformations relationnelles dans `PostgreSQL` sont écrites et testées dans **`dbt Core`**, qui versionne sous Git les contrats SQL entre les trois domaines Gold (`gold_commercial`, `gold_operations`, `gold_finance_rse`) et bloque la publication si un test d'unicité, de fraîcheur ou d'écart comptable (> 0,5 % face à `Cegid`) échoue. En parallèle, le connecteur natif **`OpenLineage`** est activé dans Airflow et dbt afin d'émettre automatiquement le graphe complet de **data lineage jusqu'à la colonne**, de la table brute jusqu'au `Certificat scope 3` et aux variables ML. En phase de cadrage, un arbitrage financier simple comparera le coût complet d'une interface open source auto-hébergée (`OpenMetadata` ou `Marquez`, et l'opportunité future de `Dagster`) à une solution catalogue SaaS sur devis.

## 4. Quelle approche Data Science, MLOps et micro-services défendre en production?

L'architecture reflète une discipline d'ingénierie MLOps pragmatique centrée sur la maintenabilité et la preuve de valeur hors échantillon.

### Pandas, scikit-learn et protocole "baseline avant modélisation"

Sur moins de 50 Go d'historique et ~15 500 événements annuels, **`Pandas`** et **`scikit-learn`** sur un conteneur éphémère de quelques gigaoctets de mémoire vive s'exécutent en quelques secondes, avec une parité totale entre le poste local, les tests `pytest` en CI/CD et la production. Sur le scoring de survie des options (POC 1) comme sur la prévision d'attrition en cuisine (POC 2), aucun algorithme complexe (type gradient boosting) n'est imposé d'emblée: l'équipe met d'abord en production dans **`MLflow`** une **baseline statistique explicable** (médiane historique segmentée bornée par le plancher contractuel), puis ne promeut un modèle supervisé calibré que s'il démontre en validation temporelle un gain net hors échantillon.

### FastAPI sans course artificielle aux millisecondes sur les devis CRM

Lorsqu'un conseiller commercial dispose d'un objectif de deux heures pour envoyer un devis B2B face aux courtiers en ligne (Naboo, Kactus), gagner quelques centaines de millisecondes par du cache distribué complexe n'apporte aucune valeur métier et crée des risques de collision entre profils. Un micro-service Python **`FastAPI`** interrogeant directement les tables indexées de `PostgreSQL` répond naturellement en une à deux secondes, expose un contrat OpenAPI propre au composant `Dynamics 365 Sales` et intègre un coupe-circuit repliant sans blocage vers la saisie manuelle en cas de coupure réseau.

### Une mini-application Streamlit + FastAPI pour peser les biodéchets en cuisine (POC 2)

Sur les 8 sites pilotes, développer un front-end mobile sur mesure ralentirait la preuve de valeur. Une petite interface Python **`Streamlit`** conteneurisée, protégée par le SSO `Entra ID` et appelant un endpoint idempotent `POST /v1/ops/biowaste` sur **`FastAPI`**, se livre en 48 heures sans dépendance externe.

Si un établissement présente une plonge en sous-sol dépourvue de Wi-Fi et de 4G (`Streamlit` nécessitant une session WebSocket active), la parade architecturale est déjà prête: en POC sur 8 sites, la saisie s'effectue dans l'office couvert en Wi-Fi; et si la généralisation aux 80 établissements révèle quelques sous-sols hors réseau, toute la logique métier et d'idempotence résidant déjà dans `FastAPI`, un développeur React interne pourra brancher un simple écran web progressif doté de cache local navigateur sur le même endpoint sans toucher au socle data.
