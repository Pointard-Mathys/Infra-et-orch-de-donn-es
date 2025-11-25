
# CONTAINER="hadoop-namenode"
# INPUT_CSV="weapons.csv"
# MAPPER="mapper.py"
# REDUCER="reducer.py"
# OUTPUT_HDFS="/output_weapons"

# echo "📄 Copie des scripts et du CSV dans le conteneur..."
# docker cp $MAPPER $CONTAINER:/$MAPPER
# docker cp $REDUCER $CONTAINER:/$REDUCER
# docker cp $INPUT_CSV $CONTAINER:/data/$INPUT_CSV

# echo "⚙️ Mise en place des permissions..."
# docker exec $CONTAINER chmod +x /$MAPPER /$REDUCER

# echo "📤 Chargement du CSV dans HDFS..."
# docker exec $CONTAINER hdfs dfs -mkdir -p /data
# docker exec $CONTAINER hdfs dfs -put -f /data/$INPUT_CSV /data/$INPUT_CSV

# echo "🗑️ Suppression d'anciennes sorties si existantes..."
# docker exec $CONTAINER hdfs dfs -rm -r $OUTPUT_HDFS || true

# echo "⚡ Lancement du job MapReduce..."
# docker exec $CONTAINER hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
#   -input /data/$INPUT_CSV \
#   -output $OUTPUT_HDFS \
#   -mapper /$MAPPER \
#   -reducer /$REDUCER

# echo "✅ Résultat du job :"
# docker exec $CONTAINER hdfs dfs -cat $OUTPUT_HDFS/part-00000




#!/bin/bash

CONTAINER="hadoop-namenode"
INPUT_CSV="weapons.csv"
MAPPER="mapper.py"
REDUCER="reducer.py"
OUTPUT_HDFS="/output_weapons"

echo "📄 Création du dossier /data dans le conteneur et copie des fichiers..."
docker exec $CONTAINER mkdir -p /data
docker cp $MAPPER $CONTAINER:/$MAPPER
docker cp $REDUCER $CONTAINER:/$REDUCER
docker cp $INPUT_CSV $CONTAINER:/data/$INPUT_CSV

echo "⚙️ Mise en place des permissions sur les scripts..."
docker exec $CONTAINER chmod +x /$MAPPER /$REDUCER

echo "📤 Chargement du CSV dans HDFS..."
docker exec $CONTAINER hdfs dfs -mkdir -p /data
docker exec $CONTAINER hdfs dfs -put -f /data/$INPUT_CSV /data/$INPUT_CSV

echo "🗑️ Suppression d'anciennes sorties si elles existent..."
docker exec $CONTAINER bash -c "hdfs dfs -test -d $OUTPUT_HDFS && hdfs dfs -rm -r $OUTPUT_HDFS || true"

echo "⚡ Lancement du job MapReduce..."
docker exec $CONTAINER hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
  -input /data/$INPUT_CSV \
  -output $OUTPUT_HDFS \
  -mapper /$MAPPER \
  -reducer /$REDUCER

echo "✅ Résultat du job :"
docker exec $CONTAINER bash -c "hdfs dfs -cat $OUTPUT_HDFS/part-00000 || echo '❌ Aucun fichier produit'"
