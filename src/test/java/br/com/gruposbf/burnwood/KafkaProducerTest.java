package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.KafkaAccess;
import br.com.gruposbf.burnwood.config.RegistryAccess;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class KafkaProducerTest {
    RegistryAccess registryAccess;
    KafkaAccess kafkaAccess;

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
    }
    @Test
    public void testProducer() {
        String topic = "fis.order.dev.order-metrics.v2";
        KafkaAux kafkaAux = new KafkaAux();
        int toSend = 3;

        int sent = kafkaAux.kafkaProducer(topic, toSend,  kafkaAccess, registryAccess);
        assertEquals(toSend,  sent);
    }


}
