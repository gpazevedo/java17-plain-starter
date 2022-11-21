package br.com.gruposbf.burnwood;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface Kafka2BigQueryOptions extends PipelineOptions {
    @Description("Kafka host")
    String getKafkaHost();
    void setKafkaHost(String kafkaHost);

    @Description("Kafka user")
    String getKafkaUser();
    void setKafkaUser(String kafkaUser);

    @Description("Kafka password")
    String getKafkaPassword();
    void setKafkaPassword(String kafkaPassword);

    @Description("Registry host")
    String getRegistryHost();
    void setRegistryHost(String registryHost);

    @Description("Registry user")
    String getRegistryUser();
    void setRegistryUser(String registryUser);

    @Description("Registry password")
    String getRegistryPassword();
    void setRegistryPassword(String registryPassword);

    @Description("Kafka Topic")
    String getKafkaTopic();
    void setKafkaTopic(String kafkaTopic);

    @Description("Kafka Consumer Group")
    String getConsumerGroup();
    void setConsumerGroup(String consumerGroup);

    // Bigquery Options
    @Description("ProjectId")
    String getProjectId();
    void setProjectId(String projectId);

    @Description("DatasetName")
    String getDatasetName();
    void setDatasetName(String datasetName);

    @Description("TableName")
    String getTableName();
    void setTableName(String tableName);

    @Description("Write Mode: Replace/Append/Empty")
    String getWriteMode();
    void setWriteMode(String writeMode);

}
