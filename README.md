# TP : Apache Kafka & Spring Cloud Streams (Real-Time Data Analytics)

Ce dépôt contient l'ensemble des fichiers et instructions pour réaliser les 3 parties pratiques :
1. Installation et test local d'Apache Kafka (sans Docker)
2. Déploiement de Zookeeper et Kafka Broker avec Docker Compose
3. Application Spring Boot / Spring Cloud Streams / Kafka Streams avec tableau de bord Web temps réel

---

## Partie 1 : Kafka en local (sans Docker)

### 1. Télécharger Kafka
- Télécharger Apache Kafka depuis : https://kafka.apache.org/downloads (archive binaire, ex. `kafka_2.13-3.7.0.tgz`).
- Extraire l'archive dans un répertoire (ex. `C:\kafka` sous Windows ou `~/kafka` sous Linux/macOS).

### 2. Démarrer Zookeeper
Sous Windows (PowerShell / CMD depuis le dossier Kafka) :
```powershell
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
Sous Linux / macOS :
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 3. Démarrer Kafka-server
Dans un second terminal :
Sous Windows :
```powershell
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
Sous Linux / macOS :
```bash
bin/kafka-server-start.sh config/server.properties
```

### 4. Tester avec `kafka-console-producer` et `kafka-console-consumer`
Dans un troisième terminal (Producer) :
```powershell
# Windows
.\bin\windows\kafka-console-producer.bat --bootstrap-server localhost:9092 --topic test-topic
```
*(Sous Linux/macOS, remplacer `.\bin\windows\...bat` par `bin/...sh`)*

Dans un quatrième terminal (Consumer) :
```powershell
# Windows
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```

---

## Partie 2 : Avec Docker (`docker-compose.yml`)

Le fichier [`docker-compose.yml`](./docker-compose.yml) définit deux services : `zookeeper` et `kafka-broker` (basés sur les images Confluent).

### 1. Démarrer les conteneurs Docker
```bash
docker compose up -d
```
Vérifier que les deux conteneurs sont actifs :
```bash
docker ps
```

### 2. Tester avec `kafka-console-producer` et `kafka-console-consumer`
Ouvrir un terminal pour le **Producer** dans le conteneur `kafka-broker` :
```bash
docker exec -it kafka-broker kafka-console-producer --bootstrap-server localhost:9092 --topic T1
```

Ouvrir un autre terminal pour le **Consumer** dans le conteneur `kafka-broker` :
```bash
docker exec -it kafka-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic T1 --from-beginning
```

---

## Partie 3 : Kafka & Spring Cloud Streams

### Architecture implémentée
1. **Producer KAFKA via REST Controller** ([`PageEventRestController.java`](./src/main/java/ma/enset/kafkaspringcloudstreams/web/PageEventRestController.java))
   - `GET /publish/{topic}/{name}` : publie un `PageEvent` aléatoire sur le topic spécifié via `StreamBridge`.
   - `POST /publish/{topic}` : publie un `PageEvent` JSON personnalisé.
2. **Consumer KAFKA** ([`PageEventService.java`](./src/main/java/ma/enset/kafkaspringcloudstreams/services/PageEventService.java))
   - Bean `pageEventConsumer` (`Consumer<PageEvent>`) abonné au topic `T1`.
3. **Supplier KAFKA** ([`PageEventService.java`](./src/main/java/ma/enset/kafkaspringcloudstreams/services/PageEventService.java))
   - Bean `pageEventSupplier` (`Supplier<PageEvent>`) qui génère un `PageEvent` (`P1` ou `P2`) toutes les secondes sur le topic `T2`.
4. **Data Analytics Real Time Stream Processing (Kafka Streams)** ([`PageEventService.java`](./src/main/java/ma/enset/kafkaspringcloudstreams/services/PageEventService.java))
   - Bean `kStreamFunction` (`Function<KStream<String, PageEvent>, KStream<String, Long>>`) :
     - Lit depuis `T2`
     - Filtre les visites avec `duration > 100`
     - Agrège et compte les visites par page sur une fenêtre temporelle de 5 secondes (`TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))`)
     - Stocke le résultat dans le State Store `"page-count"` et publie vers `T4`.
5. **Application Web Temps Réel** ([`index.html`](./src/main/resources/static/index.html))
   - Consomme le flux Server-Sent Events (`GET /analytics`) alimenté par `InteractiveQueryService`.
   - Affiche les courbes en temps réel avec SmoothieCharts et les compteurs par page.

### Lancer l'application Spring Boot
1. Assurez-vous que Docker Compose (`zookeeper` et `kafka-broker`) est démarré :
   ```bash
   docker compose up -d
   ```
2. Démarrez l'application Maven / Spring Boot :
   ```bash
   mvn spring-boot:run
   ```
3. Accédez aux endpoints :
   - **Dashboard Web temps réel** : http://localhost:8080/
   - **Publier un événement via REST** : http://localhost:8080/publish/T1/P1
   - **Flux SSE Analytics** : http://localhost:8080/analytics
