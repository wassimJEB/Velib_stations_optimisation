# Velib Station 
Il s'agit d'un pipeline qui rassemble les differentes technologies: kafka, spark, et streamlit pour le Dashboarding 

## Documentation technique 
### Lancer kafka en initialisant zookeeper et kafka server avec les commandes suivantes
./bin/zookeeper-server-start.sh ./config/zookeeper.properties
./bin/kafka-server-start.sh ./config/server.properties
rm -rf /tmp/kafka-logs
### Créer un topic sous le nom velib-station
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic velib-station

### Créer un grp : $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic blabla --consumer-property group.id=mygroup



#consumers ≤ #partitions
la suppression des messages au bout de, par exemple, quatre secondes :
./bin/kafka-configs.sh --zookeeper localhost:2181 --entity-type topics --entity-name velib-stations --alter --add-config retention.ms=4000
## Producer.py 
-Authentification API
Producer récupérer des données toutes les minutes de l’API en JSON et filtrer ces données 
=> python3 Producer.py
## Consumer.py
Réception des données
Affichage des données 

## Streaming.py 
Consumer avec spark 
Manipuler les données dans une dataframe 
=>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 Streaming.py

## Dashboard.py
Afficher et Analyser les données afin d'optimiser dans chaqe stations 
on affiche ces informations : 
		-Nombre de vélo à paris et total 
		-Nombre de vélo disponible à paris et total 
		-Biggest stations
		-Smallest stations
		-graphe sur la localisation des stations de vélo (long, lat )
		-gra^he sur la disponibilité des vélos 
		-graphe sur les stations sans vélos disponible
		-map de distrubitoion des vélos 
	=> afin d'optimiser le,processes les stations vides sont marqué par des marker (cercle bleu) 
lorsque le client rend son vélo dans le cercle bleu il aura un bonus


=>streamlit run Dashboard.py

## optimisation_velib 
Notebook  pour analyser les données 