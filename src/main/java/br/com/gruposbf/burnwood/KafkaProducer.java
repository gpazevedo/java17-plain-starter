package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.KafkaAccess;
import br.com.gruposbf.burnwood.config.RegistryAccess;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaProducer {
    static Logger log = Logger.getLogger(KafkaProducer.class.getName());

    public static void main(String[] args) {
        PipelineOptionsFactory.register(KafkaProducerOptions.class);
        KafkaProducerOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaProducerOptions.class);

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

        String topic = options.getKafkaTopic();
        KafkaAux kafkaAux = new KafkaAux();
        int messages = options.getMessages();

        log.log(Level.INFO, "Sending " + messages + " to topic ", topic);

        int sent = kafkaAux.kafkaProducer(topic, messages,  kafkaAccess, registryAccess);

    }
}
