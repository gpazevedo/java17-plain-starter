# Kafka Twin

Gera no repositório de contratos de dados do Burnwood um contrato "bigquery" correspondente ao contrato "kafka" especificado.

## Execução

O comando Maven a seguir executa o kafka twin.

```java
mvn compile exec:java -Dexec.mainClass=br.com.gruposbf.burnwood.KafkaTwin -Dexec.args="kafkaContractFile"
 ```

kafkaContractFile é o endereço do arquivo json com o contrato kafka, por exemplo:

``
src/test/organization/fisia-data-lake/dev/trusted/pedido/fis-order-dev-order-metrics-v2.json
``
