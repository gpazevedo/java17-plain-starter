package br.com.gruposbf.burnwood;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface KafkaProducerOptions extends PipelineOptions  {
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

    @Description("Number of Messages")
    Integer getMessages();
    void setMessages(Integer messages);

}
