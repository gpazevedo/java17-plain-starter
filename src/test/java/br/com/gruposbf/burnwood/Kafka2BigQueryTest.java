package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.BigQueryTable;
import br.com.gruposbf.burnwood.config.KafkaAccess;
import br.com.gruposbf.burnwood.config.RegistryAccess;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for simple App.
 */
public class Kafka2BigQueryTest
{
    RegistryAccess registryAccess;
    KafkaAccess kafkaAccess;
    BigQueryTable bigQueryTable;
    @BeforeEach
    public void getConfigs() {

        registryAccess = new RegistryAccess(
            System.getenv("registry_host"),
            System.getenv("registry_user"),
            System.getenv("registry_password")
        );

        kafkaAccess = new KafkaAccess(
                System.getenv("kafka_host"),
                System.getenv("kafka_user"),
                System.getenv("kafka_password")
        );

        bigQueryTable = new BigQueryTable(
                System.getenv("project_id"),
                System.getenv("dataset_name"),
                System.getenv("table_name"),
                BigQueryTable.WriteMode.APPEND
        );
    }

    @Test
    public void testGetAvroSchema()
    {
        String topic = "fis.trd.order.dev.order-test";
        KafkaAux kafkaAux = new KafkaAux();
        Schema avroSchema = kafkaAux.getAvroSchema(registryAccess, topic);

        GenericData.Record avroRecord = new GenericData.Record(avroSchema);
        assertTrue( avroSchema != null);

        List<Schema.Field> fields = avroSchema.getFields();
        assertTrue( fields.size() > 0);
    }

    @Test
    public void testKafka2BigQuery() {
        String topic = "fis.order.dev.order-metrics.v2";
        String consumerGroup = "data_platform_dev";

        KafkaAux kafkaAux = new KafkaAux();
        int records = kafkaAux.kafka2BigQuery(topic, kafkaAccess, registryAccess, consumerGroup, bigQueryTable);
    }
}
