package br.com.gruposbf.burnwood.config;

import lombok.Data;
import lombok.NonNull;

@Data
public class KafkaAccess {
    @NonNull
    private final String bootstrapServers;
    @NonNull
    private final String kafkaUser;
    @NonNull
    private final String kafkaPassword;
}
