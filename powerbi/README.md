# Tableau de bord Power BI — Commerce Sentinel

Ce dossier accompagne la dernière étape du pipeline. Il contient le thème, les requêtes Power Query, les mesures DAX, la structure du modèle et une maquette SVG. Les CSV sont générés par `PowerBiExport.java` dans `reports/powerbi/data/` **après** les jobs MapReduce. Power BI Desktop n'est pas requis pour générer les CSV, mais il est requis pour créer et enregistrer le rapport `.pbix`.

## 1. Obtenir les données

```powershell
mvn clean package
.\scripts\run-mapreduce.ps1
Get-ChildItem reports/powerbi/data
```

Le pipeline produit les fichiers suivants :

| Table Power BI | CSV | Usage |
|---|---|---|
| Transactions | transactions.csv | Fait détaillé : temps, client, produit, catégorie, quantité, montant, paiement, ville |
| Customers | customers.csv | Dimension client : segment, ville |
| Products | products.csv | Dimension produit : catégorie, prix de référence |
| Categories | categories.csv | Dimension catégorie |
| ClientStats | client_stats.csv | Agrégats MapReduce par client |
| CategoryStats | category_stats.csv | Agrégats MapReduce par catégorie, moyenne, écart-type |
| PeriodStats | period_stats.csv | Agrégats MapReduce par période |
| TransactionAnomalies | transaction_anomalies.csv | Signalements de montant, seuil et contexte |
| CustomerAnomalies | customer_anomalies.csv | Clients à forte activité |
| PeriodAnomalies | period_anomalies.csv | Variations de volume |
| DataQuality | data_quality.csv | Comptages de contrôle et date d'export |

L'export des transactions lit HDFS en continu; il ne recalcule aucun signalement. Les six tables d'agrégats ou d'anomalies proviennent exclusivement des jobs MapReduce. `data_quality.csv` compare le nombre de lignes exportées au nombre agrégé par `ClientStatsJob`. Une différence mérite enquête : ligne invalide, fichier absent ou archivage en double.

## 2. Importer dans Power BI Desktop

1. Ouvrir un rapport vide et créer le paramètre texte `pDataFolder`, valeur absolue du dossier `reports\powerbi\data` sur la machine.
2. Dans **Transformer les données > Nouvelle source > Requête vide > Éditeur avancé**, coller `queries/LoadCsv.pq` et nommer la fonction `LoadCsv`.
3. Créer une requête vide pour chaque autre fichier `.pq` et lui donner exactement le nom du fichier sans extension : `Transactions`, `Customers`, etc.
4. **Fermer et appliquer**. Les types sont fixés dans les requêtes : dates, heures, entiers, montants.
5. Dans **Affichage > Thèmes > Parcourir les thèmes**, importer `theme.json`.
6. Créer les mesures de `measures.dax` une par une. La table `Date` est une **nouvelle table**, et ses colonnes sont des **nouvelles colonnes**; marquer ensuite `Date[Date]` comme table de dates.
7. Créer les relations ci-dessous, toutes en filtrage **simple** depuis la dimension vers le fait. Enregistrer sous `Commerce-Sentinel.pbix`.

Les CSV peuvent aussi être importés directement via **Obtenir des données > Texte/CSV**, mais les requêtes fournies garantissent les bons types à chaque actualisation. Après un nouveau lot MapReduce et un nouvel export, utiliser **Actualiser** dans Power BI.

## 3. Modèle de données

```text
Date[Date] ───────────────→ Transactions[transaction_date]
         └───────────────→ TransactionAnomalies[transaction_date]

Customers[customer_id] ──→ Transactions[customer_id]
                       ├─→ ClientStats[customer_id]
                       ├─→ CustomerAnomalies[customer_id]
                       └─→ TransactionAnomalies[customer_id]

Products[product_id] ────→ Transactions[product_id]
                       └─→ TransactionAnomalies[product_id]

Categories[category] ────→ Transactions[category]
                       ├─→ CategoryStats[category]
                       └─→ TransactionAnomalies[category]
```

Ne pas créer de relation automatique entre `Transactions` et `TransactionAnomalies` sur `transaction_id` : un redémarrage du Producer peut réutiliser un identifiant et l'archivage HDFS peut contenir des doublons. `PeriodStats` et `PeriodAnomalies` restent autonomes parce que la clé `period` peut représenter une heure, un jour ou un mois selon la configuration du job. `DataQuality` reste autonome. Désactiver les relations automatiques proposées qui contredisent ce schéma.

## 4. Design visuel

Voir [design-preview.svg](design-preview.svg) pour l'apparence cible. Format conseillé : **16:9, 1440 × 810**. Fond `#0B1220`, surfaces `#152238`, cyan `#00C9D8` pour le volume et le CA, orange `#FFB454` pour l'attention, rose `#EF476F` pour les anomalies. Typographie Segoe UI. Utiliser les mêmes marges (24 px), cartes de même hauteur, 6 colonnes de grille et peu de lignes de grille. Afficher l'unité monétaire choisie par le projet; ne pas supposer EUR ou MAD si les transactions ne la définissent pas.

Slicers communs, synchronisés entre pages : plage de dates, catégorie, segment client, ville, mode de paiement. Ajouter un bouton de réinitialisation des filtres et des info-bulles expliquant les seuils. Les tables de détail doivent permettre la recherche, le tri et l'export. Utiliser une couleur d'alerte uniquement pour les valeurs signalées; les normales restent neutres.

### Page 1 — Vue exécutive

- En-tête : titre, période analysée, statut du dernier export.
- Cartes : transactions, chiffre d'affaires, panier moyen, clients actifs, anomalies de montant, taux d'anomalie.
- Courbe : CA et volume par jour; barres : top catégories par CA; barres : top villes.
- Bandeau : dernière variation de période et nombre de clients atypiques.

### Page 2 — Activité et saisonnalité

- Courbe quotidienne : transactions, CA, variation jour précédent, cumul du CA.
- Matrice thermique : jour de semaine × heure, valeur = nombre de transactions.
- Histogramme : répartition par mode de paiement; petit multiple par ville.
- Slicers temporels; afficher explicitement les jours sans activité si la table Date les inclut.

### Page 3 — Catégories et produits

- Barres : CA, volume et panier moyen par catégorie.
- Nuage de points : quantité vendue × CA par produit, taille = nombre de transactions.
- Tableau : catégorie, moyenne MapReduce, écart-type, seuil `mean + 3σ`, anomalies et taux d'anomalie.
- Top 10 produits par CA et comparaison prix moyen / prix de référence (la référence est indicative).

### Page 4 — Clients et géographie

- Cartes : clients actifs, CA par client, clients atypiques, CA des clients atypiques.
- Barres : top 20 clients par CA et par nombre de transactions.
- Comparaison Premium/Standard et villes; carte géographique uniquement si le géocodage des villes est fiable.
- Tableau de détail client : segment, ville, count, total, average, motif d'alerte.

### Page 5 — Anomalies de transactions

- Cartes : nombre, taux, montant cumulé, surplus au-dessus du seuil.
- Courbe : anomalies par date; barres : anomalies par catégorie et par ville.
- Nuage de points : montant × seuil, couleur par catégorie.
- Tableau triable : transaction, date, client, produit, ville, montant, moyenne, écart-type, seuil, motif.
- Mise en forme conditionnelle : rose si montant supérieur au seuil; info-bulle avec `amount - threshold`.

### Page 6 — Variations et qualité des données

- Colonnes : volume par période; lignes de référence à ± seuil configuré.
- Tableau : période précédente, période actuelle, volumes et variation en %.
- Cartes de contrôle : lignes brutes, lignes invalides, total agrégé, différence, date d'export.
- Texte explicatif : une variation signale un changement d'activité, pas une fraude.

### Page 7 — Explorateur des transactions

- Table détaillée avec recherche et filtres croisés : date, ID, catégorie, quantité, prix, montant, client, produit, ville, paiement.
- Drill-through vers la fiche client et la catégorie. Conserver cette page pour l'audit et la démonstration orale.

## 5. Mesures et interprétation

Les mesures DAX de `measures.dax` couvrent ventes, volume, panier, clients, produits, anomalies, taux, surplus, écarts standardisés, variation quotidienne et cumul. Formatter les taux/variations comme **pourcentage**, les montants comme **nombre décimal à deux chiffres** (avec une unité monétaire seulement si elle est connue), les comptes comme **entiers**. La moyenne, l'écart-type et le seuil issus de CategoryStats sont calculés sur l'ensemble du lot MapReduce; les filtres Power BI ne recalculent pas ces valeurs. Les visualisations dynamiques par date utilisent les transactions détaillées.

## 6. Limites

Les tables sont actualisées après un traitement batch, pas en temps réel. Un grand volume de transactions rend l'import Power BI lourd : filtrer la période, créer un export par lot ou utiliser un stockage analytique compatible DirectQuery dans une évolution future. Le fichier `.pbix` doit être enregistré une première fois avec Power BI Desktop; ce dépôt fournit des composants réels et importables, sans prétendre contenir un rapport natif déjà validé par Desktop.
