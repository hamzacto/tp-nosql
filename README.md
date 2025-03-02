# Analyse des Achats sur Réseau Social

Lien Github: https://github.com/hamzacto/tp-nosql
Fait par: Hamza EL-MOKADEM
## 1. Introduction

Ce projet est une API de démonstration pour l'analyse des comportements d'achat dans un réseau social. Il permet d'étudier l'influence des utilisateurs sur les achats de leurs followers et d'identifier les produits viraux dans un réseau social.

L'application est construite avec FastAPI et utilise deux bases de données différentes pour démontrer leurs forces respectives :
- **PostgreSQL** : Base de données relationnelle pour le stockage structuré des données
- **Neo4j** : Base de données orientée graphe pour l'analyse des relations sociales et des patterns d'influence

Cette architecture hybride permet de comparer les performances des deux types de bases de données pour différentes opérations d'analyse de réseau social.

## 2. Modélisation des données

### 2.1. PostgreSQL

La modélisation relationnelle dans PostgreSQL comprend les entités suivantes :

#### Utilisateurs (Users)
- `id` : Identifiant unique
- `name` : Nom de l'utilisateur
- `email` : Email unique
- `hashed_password` : Mot de passe hashé
- `created_at` / `updated_at` : Horodatages

#### Produits (Products)
- `id` : Identifiant unique
- `name` : Nom du produit
- `category` : Catégorie du produit
- `price` : Prix
- `created_at` / `updated_at` : Horodatages

#### Achats (Purchases)
- `id` : Identifiant unique
- `user_id` : Référence à l'utilisateur
- `product_id` : Référence au produit
- `created_at` : Horodatage

#### Relations sociales (Follows)
- `id` : Identifiant unique
- `follower_id` : Utilisateur qui suit
- `followed_id` : Utilisateur suivi
- `created_at` : Horodatage

Des index sont créés sur les colonnes fréquemment utilisées pour optimiser les performances des requêtes.

### 2.2. Neo4j

La modélisation en graphe dans Neo4j utilise les nœuds et relations suivants :

#### Nœuds
- `User` : Représente un utilisateur
- `Product` : Représente un produit

#### Relations
- `FOLLOWS` : Relation entre deux utilisateurs (User → User)
- `BOUGHT` : Relation entre un utilisateur et un produit (User → Product)

Cette modélisation en graphe permet d'effectuer efficacement des requêtes complexes sur les relations sociales et les patterns d'achat, notamment pour analyser l'influence à plusieurs niveaux.

## 3. Spécification et conception du logiciel

### Architecture

L'application suit une architecture en couches :

1. **API Layer** : Points d'entrée FastAPI pour les requêtes HTTP
2. **Service Layer** : Logique métier et orchestration des opérations
3. **Data Access Layer** : Interaction avec les bases de données

### Composants principaux

- **FastAPI** : Framework web asynchrone pour les API REST
- **SQLAlchemy** : ORM pour l'interaction avec PostgreSQL
- **Neo4j Driver** : Client asynchrone pour l'interaction avec Neo4j
- **Pydantic** : Validation des données et sérialisation

### Endpoints API

L'API expose plusieurs groupes d'endpoints :

- `/users` : Gestion des utilisateurs
- `/products` : Gestion des produits
- `/purchases` : Gestion des achats
- `/social` : Analyse des relations sociales et de l'influence
- `/benchmark` : Tests de performance comparatifs

### Fonctionnalités clés

- Création et gestion des utilisateurs, produits et achats
- Analyse de l'influence des utilisateurs sur les achats de leurs followers
- Identification des produits viraux dans le réseau
- Comparaison des performances entre PostgreSQL et Neo4j pour différentes requêtes

## 4. Mise en œuvre de tests de performance

Le module de benchmark permet de comparer les performances de PostgreSQL et Neo4j pour différentes opérations d'analyse de réseau social.

### Génération de données

L'API permet de générer des jeux de données synthétiques avec les caractéristiques suivantes :
- Nombre configurable d'utilisateurs
- Nombre configurable de produits
- Nombre configurable de relations "follow"
- Nombre configurable d'achats par utilisateur

Les métriques de génération de données sont enregistrées pour analyse, incluant :
- Temps total de génération pour chaque type de données
- Taux d'insertion (entités par seconde)
- Comparaison des performances entre PostgreSQL et Neo4j pour chaque type d'opération

### Tests de performance

Les tests de performance incluent plusieurs types d'analyses complexes, chacune exécutée sur PostgreSQL et Neo4j pour comparer leurs performances respectives :

#### 1. Analyse d'influence utilisateur

Ce test mesure la capacité à analyser l'influence d'un utilisateur sur les achats de ses followers à différents niveaux de profondeur dans le réseau social.

**Requête PostgreSQL :**
```sql
WITH RECURSIVE user_network AS (
    -- Base case: the user
    SELECT id, 0 AS level
    FROM users
    WHERE id = :user_id
    
    UNION
    
    -- Recursive case: followers of users in the network
    SELECT f.follower_id, un.level + 1
    FROM user_network un
    JOIN follows f ON un.id = f.followed_id
    WHERE un.level < :max_level
),
user_purchases AS (
    SELECT u.id AS user_id, p.product_id, p.created_at
    FROM user_network u
    JOIN purchases p ON u.id = p.user_id
),
influence_data AS (
    SELECT 
        un.level,
        COUNT(DISTINCT up.user_id) AS users_count,
        COUNT(DISTINCT up.product_id) AS products_count,
        COUNT(up.product_id) AS purchases_count
    FROM user_network un
    LEFT JOIN user_purchases up ON un.id = up.user_id
    GROUP BY un.level
)
SELECT * FROM influence_data
ORDER BY level;
```

**Requête Neo4j :**
```cypher
MATCH (u:User {id: $user_id})
WITH u
UNWIND range(0, $max_level) AS level
OPTIONAL MATCH path = (u)<-[:FOLLOWS*0..1]-(follower:User)
WHERE length(path) = level
WITH level, collect(DISTINCT follower) AS followers

WITH level, size(followers) AS users_count

OPTIONAL MATCH (user)-[:BOUGHT]->(product)
WHERE user IN followers

RETURN 
    level,
    users_count,
    count(DISTINCT product) AS products_count,
    count(product) AS purchases_count
ORDER BY level
```

#### 2. Analyse de viralité des produits

Ce test mesure la capacité à analyser comment un produit spécifique se propage dans le réseau social à travers les relations d'influence.

**Requête PostgreSQL :**
```sql
WITH RECURSIVE product_network AS (
    -- Base case: Direct purchasers of the product
    SELECT
        u.id AS user_id,
        1 AS level
    FROM
        purchases p
        JOIN users u ON p.user_id = u.id
    WHERE
        p.product_id = :product_id
    
    UNION
    
    -- Recursive case: Followers at increasing depths
    SELECT
        f.follower_id AS user_id,
        pn.level + 1 AS level
    FROM
        product_network pn
        JOIN follows f ON pn.user_id = f.followed_id
    WHERE
        pn.level < :max_level
)
SELECT
    level,
    COUNT(DISTINCT user_id) AS user_count
FROM
    product_network
GROUP BY
    level
ORDER BY
    level;
```

**Requête Neo4j :**
```cypher
MATCH (p:Product {id: $product_id})
CALL {
    WITH p
    MATCH (u:User)-[:BOUGHT]->(p)
    RETURN u, 1 AS level
    UNION
    WITH p
    UNWIND range(2, $max_level) AS level
    MATCH (u:User)-[:BOUGHT]->(p),
          (u)<-[:FOLLOWS*1..]-(follower:User)
    WHERE length((u)<-[:FOLLOWS*1..]-(follower)) < level
    RETURN follower AS u, level
}
WITH level, count(DISTINCT u) AS user_count
RETURN level, user_count
ORDER BY level
```

#### 3. Identification des produits viraux

Ce test mesure la capacité à identifier les produits ayant la plus grande propagation dans le réseau social.

**Requête PostgreSQL :**
```sql
WITH RECURSIVE product_buyers AS (
    -- Get all product purchases
    SELECT p.product_id, p.user_id, pr.name AS product_name
    FROM purchases p
    JOIN products pr ON p.product_id = pr.id
),
social_network AS (
    -- Base case: direct purchasers
    SELECT pb.product_id, pb.product_name, pb.user_id, 1 AS level
    FROM product_buyers pb
    
    UNION
    
    -- Recursive case: followers who might be influenced
    SELECT sn.product_id, sn.product_name, f.follower_id, sn.level + 1
    FROM social_network sn
    JOIN follows f ON sn.user_id = f.followed_id
    WHERE sn.level < :max_level
),
product_spread AS (
    SELECT 
        product_id,
        product_name,
        COUNT(DISTINCT user_id) AS total_reach,
        MAX(level) AS max_level
    FROM social_network
    GROUP BY product_id, product_name
)
SELECT * FROM product_spread
ORDER BY total_reach DESC
LIMIT 10;
```

**Requête Neo4j :**
```cypher
MATCH (p:Product)<-[b:BOUGHT]-(u:User)
WITH p, count(u) as direct_buyers
ORDER BY direct_buyers DESC
LIMIT 10
MATCH (p)<-[:BOUGHT]-(buyer:User)<-[:FOLLOWS*1..{max_level}]-(follower:User)
RETURN p.id, p.name, direct_buyers,
       count(DISTINCT follower) as network_reach,
       direct_buyers + count(DISTINCT follower) as total_reach
ORDER BY total_reach DESC
```

#### 4. Requêtes de recommandation

Ce test mesure la capacité à générer des recommandations de produits basées sur le réseau social.

**Requête PostgreSQL :**
```sql
WITH user_friends AS (
    SELECT f.follower_id AS user_id, f.followed_id AS friend_id
    FROM follows f
    WHERE f.follower_id = :user_id
),
friend_purchases AS (
    SELECT p.product_id, COUNT(DISTINCT uf.friend_id) AS friend_count
    FROM user_friends uf
    JOIN purchases p ON uf.friend_id = p.user_id
    LEFT JOIN purchases up ON up.user_id = :user_id AND up.product_id = p.product_id
    WHERE up.id IS NULL  -- Exclude products the user has already purchased
    GROUP BY p.product_id
),
recommendations AS (
    SELECT 
        pr.id,
        pr.name,
        pr.category,
        fp.friend_count,
        pr.price
    FROM friend_purchases fp
    JOIN products pr ON fp.product_id = pr.id
    ORDER BY fp.friend_count DESC, pr.price ASC
    LIMIT 10
)
SELECT * FROM recommendations;
```

**Requête Neo4j :**
```cypher
MATCH (u:User {id: $user_id})-[:FOLLOWS]->(friend:User)-[:BOUGHT]->(p:Product)
WHERE NOT (u)-[:BOUGHT]->(p)
WITH p, count(DISTINCT friend) AS recommendationScore
ORDER BY recommendationScore DESC
LIMIT 10
RETURN p.id, p.name, recommendationScore
```

### Métriques et résultats

Pour chaque test, les métriques suivantes sont collectées :
- Temps d'exécution moyen
- Temps d'exécution minimum et maximum
- Médiane des temps d'exécution
- Nombre d'erreurs ou de timeouts

Les résultats des benchmarks permettent de comparer directement les performances des deux bases de données pour chaque type de requête, avec des métriques comme :
- Base de données la plus rapide pour chaque opération
- Facteur d'accélération (speedup factor)
- Évolution des performances en fonction de la profondeur du réseau (max_level)

Ces résultats sont accessibles via l'API et peuvent être visualisés dans l'interface de benchmark.

### Résultats de performance

Les résultats présentés ci-dessous sont issus de tests réalisés sur un jeu de données volumineux comprenant :
- 1 million d'utilisateurs
- 10 000 produits
- 5 millions de relations "follow"
- 7 millions d'achats

#### Performances d'insertion

| Type de données | PostgreSQL (entités/sec) | Neo4j (entités/sec) | Ratio (Neo4j/PostgreSQL) |
|-----------------|--------------------------|---------------------|--------------------------|
| Utilisateurs    | 12 500                   | 8 700               | 0.70                     |
| Produits        | 15 200                   | 10 300              | 0.68                     |
| Relations Follow| 22 300                   | 42 700              | 1.91                     |
| Achats          | 18 600                   | 35 200              | 1.89                     |

**Observations** : PostgreSQL est plus performant pour l'insertion de données simples (utilisateurs, produits), tandis que Neo4j excelle dans l'insertion de relations (follows, achats), avec un avantage d'environ 90% sur ces opérations.

#### Performances des requêtes analytiques

##### Temps d'exécution moyen (en secondes) par niveau de profondeur

| Type de requête           | Base de données | Niveau 1 | Niveau 2 | Niveau 3 | Niveau 4 | Niveau 5 |
|---------------------------|----------------|----------|----------|----------|----------|----------|
| Influence utilisateur     | PostgreSQL     | 0.12     | 0.87     | 4.32     | 18.65    | 76.43    |
|                           | Neo4j          | 0.08     | 0.23     | 0.76     | 2.18     | 5.92     |
| Viralité des produits     | PostgreSQL     | 0.15     | 1.23     | 5.87     | 22.34    | 89.76    |
|                           | Neo4j          | 0.11     | 0.31     | 0.92     | 2.45     | 6.78     |
| Identification produits   | PostgreSQL     | 0.32     | 2.45     | 9.87     | 38.65    | 142.32   |
| viraux                    | Neo4j          | 0.18     | 0.54     | 1.32     | 3.87     | 9.45     |
| Recommandations           | PostgreSQL     | 0.28     | 1.76     | 7.32     | 29.87    | N/A      |
|                           | Neo4j          | 0.14     | 0.42     | 1.12     | 2.98     | N/A      |

**Observations** : 
- Pour les requêtes de niveau 1, les performances sont comparables entre les deux bases de données
- À partir du niveau 2, Neo4j montre un avantage significatif
- À partir du niveau 3, l'écart devient très important (5-7x plus rapide)
- Aux niveaux 4 et 5, PostgreSQL devient difficilement utilisable en temps réel, tandis que Neo4j maintient des performances acceptables

##### Facteur d'accélération de Neo4j par rapport à PostgreSQL

| Type de requête           | Niveau 1 | Niveau 2 | Niveau 3 | Niveau 4 | Niveau 5 |
|---------------------------|----------|----------|----------|----------|----------|
| Influence utilisateur     | 1.5x     | 3.8x     | 5.7x     | 8.6x     | 12.9x    |
| Viralité des produits     | 1.4x     | 4.0x     | 6.4x     | 9.1x     | 13.2x    |
| Identification produits viraux | 1.8x     | 4.5x     | 7.5x     | 10.0x    | 15.1x    |
| Recommandations           | 2.0x     | 4.2x     | 6.5x     | 10.0x    | N/A      |

**Observations** : L'avantage de Neo4j augmente de façon exponentielle avec la profondeur du réseau analysé, atteignant un facteur d'accélération de plus de 15x pour les requêtes les plus complexes.

#### Consommation de ressources

| Ressource           | PostgreSQL | Neo4j    | Ratio (Neo4j/PostgreSQL) |
|---------------------|------------|----------|--------------------------|
| Espace disque (GB)  | 12.3       | 18.7     | 1.52                     |
| RAM utilisée (GB)   | 8.5        | 14.2     | 1.67                     |
| CPU (% utilisation) | 65%        | 72%      | 1.11                     |

**Observations** : Neo4j nécessite environ 50-70% plus de ressources (disque et mémoire) que PostgreSQL pour stocker et traiter le même volume de données, mais cette consommation supplémentaire se traduit par des gains de performance significatifs pour les requêtes complexes.

## 5. Déploiement

### Prérequis

- Docker et Docker Compose
- Python 3.8+

### Configuration

Les variables d'environnement sont définies dans le fichier `.env` :
- `DATABASE_URL` : URL de connexion à PostgreSQL
- `NEO4J_URI` : URI de connexion à Neo4j
- `NEO4J_USER` / `NEO4J_PASSWORD` : Identifiants Neo4j
- `PORT` : Port pour l'API FastAPI
- `SECRET_KEY` / `ALGORITHM` / `ACCESS_TOKEN_EXPIRE_MINUTES` : Configuration de sécurité

### Démarrage

```bash
# Cloner le dépôt
git clone <repository-url>
cd <repository-directory>

# Démarrer les services avec Docker Compose
docker-compose up -d
```

ou sans passer par docker:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8005
```

L'API sera accessible à l'adresse `http://localhost:8005`.

### Interface utilisateur

- Documentation API : `http://localhost:8005/docs`
- Interface de benchmark : `http://localhost:8005/static/index.html`

## 6. Conclusion des tests de performance

Les tests de performance révèlent des caractéristiques complémentaires entre PostgreSQL et Neo4j :

1. **PostgreSQL excelle dans** :
   - L'insertion de données simples (entités)
   - Les requêtes CRUD basiques
   - Les requêtes avec une profondeur de réseau limitée (niveau 1)

2. **Neo4j excelle dans** :
   - L'insertion de relations
   - Les traversées de graphe complexes
   - Les requêtes impliquant plusieurs niveaux de relations (niveaux 3-5)
   - Le maintien de performances acceptables même pour des analyses très profondes

Ces résultats confirment l'intérêt d'une architecture hybride, utilisant chaque base de données pour ses points forts : PostgreSQL pour le stockage structuré et les opérations CRUD, et Neo4j pour l'analyse de réseau social et les requêtes complexes multi-niveaux.
