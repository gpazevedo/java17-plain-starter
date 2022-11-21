package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.BigQueryTable;
import br.com.gruposbf.burnwood.config.KafkaAccess;
import br.com.gruposbf.burnwood.config.RegistryAccess;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Kafka2BigQuery
{
    static Logger log = Logger.getLogger(Kafka2BigQuery.class.getName());
    public static void main( String[] args )
    {
        log.setLevel(Level.ALL);

        PipelineOptionsFactory.register(Kafka2BigQueryOptions.class);
        Kafka2BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Kafka2BigQueryOptions.class);

        KafkaAccess kafkaAccess = new KafkaAccess(
                options.getKafkaHost(),
                options.getKafkaUser(),
                options.getKafkaPassword()
        );

        RegistryAccess registryAccess = new RegistryAccess(
                options.getRegistryHost(),
                options.getRegistryUser(),
                options.getRegistryPassword()
        );

        String write = options.getWriteMode();

        BigQueryTable.WriteMode writeMode = write.equals("Replace") ? BigQueryTable.WriteMode.REPLACE :
                write.equals("Append") ? BigQueryTable.WriteMode.APPEND : BigQueryTable.WriteMode.EMPTY;

        BigQueryTable bigQueryTable = new BigQueryTable(
                options.getProjectId(),
                options.getDatasetName(),
                options.getTableName(),
                writeMode
        );

        String consumerGroup = options.getConsumerGroup();
        String topic = options.getKafkaTopic();

        KafkaAux kafkaAux = new KafkaAux();
        int records = kafkaAux.kafka2BigQuery(topic, kafkaAccess, registryAccess, consumerGroup, bigQueryTable);
        log.log(Level.INFO, records + " mensagens transfereidas do Topico: " + topic + " para BigQuery: " + bigQueryTable);
    }
}
