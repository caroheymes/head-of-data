---
title: "Note d'architecture CIO/CDO: plateforme data et MLOps découplée sur Azure (hors monolithe Fabric)"
created: "2026-11-16"
updated: "2026-11-17"
---

# Note d'architecture CIO/CDO: plateforme data et MLOps découplée sur Azure

La présente note expose l'architecture cible retenue pour livrer la vague 1 en 19 semaines avec deux recrutements au pôle central du groupe X. Un seul point d'EBITDA regagné en France (~1,85 M€) couvre plus de quinze fois le coût annuel d'infrastructure de ce dispositif. Elle synthétise les onze invariants d'architecture (`AD-1` à `AD-11`, détaillés en séance), s'appuie sur la topologie [architecture.drawio](architecture.drawio) ([aperçu PNG](architecture.png), [export SVG](architecture.svg)) et complète le support interactif [index.html](index.html) ainsi que l'argumentaire [Q&A.md](Q&A.md).

## 1. Pourquoi refuser le surdimensionnement tout-Fabric tout en protégeant l'acquis BI commercial

Cinq ans d'historique structuré au sein du groupe X (~15 500 événements et ~650 000 participants par an, ~5 200 comptes B2B, près de 80 sites) représentent moins de 50 Go de données utiles, avec des cycles de décision quotidiens ou au quart d'heure. Déployer un moteur distribué Spark et souscrire une capacité `Microsoft Fabric F64` (~5 240 € HT/mois en tarif réservé, plus ~650 € HT/mois d'`Azure API Management`) imposerait une complexité opérationnelle disproportionnée, un verrouillage éditeur fort et un risque connu de bridage diurne des rapports métiers par les traitements Python.

À l'inverse, éteindre brutalement l'espace `Power BI` / `Fabric` initié côté Commerce par l'équipe BI commerciale avec un cabinet partenaire casserait une dynamique métier utile. L'architecture résout cette équation par un découplage franc sur **Azure hors Fabric** (`France Central`, conteneurs Docker portables sous `Terraform`). Tout le réacteur d'ingénierie, de qualité, de data lineage et de MLOps s'exécute dans des briques ouvertes, testables sous Git et isolées sur **`Azure Database for PostgreSQL Flexible Server`** et **`Azure Container Apps`**. La couche `Power BI` existante est sanctuarisée comme vitrine sémantique en lecture seule sur les tables `gold_*` certifiées (option 3 sur micro-capacité plafonnée <= 300 € HT/mois, zéro Spark, zéro Data Factory). À la semaine 19, un jalon factuel avec le DAF arbitrera entre le maintien de cette fine couche BI et la connexion directe de `Power BI Pro` sur un réplica `PostgreSQL` (option 1 pure).

## 2. Trois partis pris d'ingénierie et de MLOps adaptés à l'échelle du groupe

Le premier parti pris unifie les données analytiques et opérationnelles légères dans une seule base **`PostgreSQL`** découpée en trois domaines Gold (`gold_commercial`, `gold_operations`, `gold_finance_rse`) reliés uniquement par des vues contrats SQL en lecture seule gérées sous **`dbt Core`** (`AD-1`, `AD-3`). Dès le premier mois, le domaine finance réconcilie chaque jour les ~5 200 `Comptes B2B unifiés` à +/- 0,5 % avec `Cegid` (filiale Formation incluse), tandis que les opérations distinguent proprement le séjour séminaire au forfait, l'`Affaire Événementielle` sur devis (pôle Traiteur inclus) et le `Contrat In-Situ`.

Le deuxième parti pris privilégie des micro-services Python simples et une démarche Data Science rigoureuse (`AD-5`, `AD-6`, `AD-10`). Les recommandations de sites de repli dans `Dynamics 365 Sales` transitent par un micro-service **`FastAPI`** protégé nativement par OAuth2 `Entra ID` sur l'ingress `Container Apps`. En cuisine (POC 2 sur 8 sites pilotes), un formulaire Python léger **`Streamlit`** branché sur `FastAPI` permet de tester la pesée des biodéchets en 48 heures sans mobiliser de développeur front-end. Côté prédiction (survie d'options et attrition *wash*), tout sujet démarre par une **baseline statistique explicable** (`Pandas`) enregistrée dans **`MLflow`**, avant de promouvoir un modèle supervisé (`scikit-learn`) uniquement si le gain hors échantillon est prouvé.

Le troisième parti pris valorise la pleine propriété du PMS interne `Lxxxx` (`AD-2`, `AD-11`). Un audit de deux jours en semaine 1 avec l'équipe `Lxxxx` fixe le mode d'extraction incrémentale aux 15 minutes via des conteneurs Python `dlt` orchestrés par **`Apache Airflow`**, sans aucune écriture retour sur les systèmes transactionnels maîtres.

## 3. Neutralisation des quatre risques réglementaires et budgétaires du mandat

### Protection RGPD (articles 9 et 28) dès le sas d'ingestion

Avant toute écriture dans `PostgreSQL`, le conteneur `dlt` remplace en mémoire vive les identifiants des ~650 000 `Participants` `Lxxxx` par un hachage salé `SHA-256` (sel tournant dans `Azure Key Vault`, zéro table de correspondance) et agrège les régimes alimentaires ou besoins PMR à la maille du séjour (`AD-2`). Des tests automatisés bloquent tout motif d'email ou texte libre résiduel et purgent sous 30 jours tout fichier brut temporaire.

### Étanchéité NIS2 (article 21) et cohérence sémantique BI

Les groupes `Microsoft Entra ID` pilotent un filtrage symétrique entre les politiques natives **Row-Level Security de `PostgreSQL`** et les rôles RLS des modèles `Power BI` versionnés sous Git en TMDL (`AD-4`, `AD-8`), avec chiffrement `TLS 1.3`/`AES-256` et conservation de 365 jours des traces dans `Azure Log Analytics`. Les établissements et campus *In-Situ* restent strictement cloisonnés par `site_id`, et les conseillers commerciaux n'accèdent jamais aux marges sites.

### Lignage à la colonne OpenLineage et auditabilité CSRD

Chaque exécution `Airflow` et `dbt Core` émet automatiquement son graphe de lignage jusqu'à la colonne au standard ouvert **`OpenLineage`** (`AD-9`), l'interface de consultation étant arbitrée sur devis entre une brique open source (`OpenMetadata` ou `Marquez`) et un catalogue SaaS. Les packs de facteurs carbone (*ADEME*, AIE, référentiel SaaS bilan carbone) sont historisés en mode *append-only*. Tout séjour incomplet bascule en quarantaine avec alerte webhook `Teams` immédiate, garantissant des `Certificats scope 3` intégralement rejouables devant l'organisme tiers indépendant (OTI) et les grands comptes.

### Sobriété FinOps et suppression du risque de bridage BI

L'absence de Spark et d'`APIM` divise la facture récurrente par près de huit (`AD-7`): le socle `PostgreSQL` + `Container Apps` (`Airflow`, `FastAPI`, `MLflow`, `Streamlit`) revient entre **340 et 405 € HT/mois**, soit **~650 à 700 € HT/mois** en incluant la micro-capacité BI conservée en vague 1 (contre plus de 5 800 € HT/mois pour un couple `Fabric F64` + `APIM`). Les calculs Python tournant sur des conteneurs éphémères physiquement séparés de la BI et bornés avant 7 h 15 `Europe/Paris`, tout bridage diurne de `Power BI` devient structurellement impossible.

## 4. Trajectoire d'extinction progressive de l'entrepôt historique

La bascule s'opère par domaine sans coupure brutale. Des semaines 1 à 6 (POC 1), l'ingestion dans `PostgreSQL` des flux commerciaux (`Dynamics 365 Sales`, `Lxxxx`) et du `Compte B2B unifié` (`Cegid`) gèle toute évolution commerciale sur l'ancien entrepôt. Des semaines 7 à 13 (POC 2), l'équipe BI rebranche les rapports d'exploitation et de marge sur les vues `gold_operations` alimentées par `Esker` et `Skello`. Des semaines 14 à 19 (POC 3), les flux énergétiques des 7 pays complètent `gold_finance_rse`, ouvrant l'arbitrage FinOps S19 puis la reprise finale des historiques au second semestre 2027 pour éteindre définitivement les serveurs hérités.
