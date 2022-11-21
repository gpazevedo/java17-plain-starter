# Kafka to BigQuery

Este artefato consome mensagens do tópico especificado e escreve na tabela bigquery especificada.

## Teste pipeline DataFlow

Para testar a pipeline e garantir que funcione nos ambientes de dev e prod, o ideal é executar no Dataflow. O comando Maven a seguir executa o pipeline com o runner do Dataflow. Basta preencher os parametros de acordo com o ambiente.

```java
mvn compile exec:java \
        -Dexec.mainClass=br.com.gruposbf.burnwood.Kafka2BigQuery \
        -Dexec.args="\
    --kafkaUser= \
    --kafkaPassword= \
    --registryUser= \
    --registryPassword= \
    --kafkaTopic= \
    --consumerGroup= \
    --projectId= \
    --datasetName= \
    --writeMode=Append \
    --tableName= \
    --runner=DataflowRunner \
    --project= \
    --region=us-east1 \
    --tempLocation= \
    --subnetwork= \
    --streaming=true \
    --kafkaHost= \
    --registryHost="
```
