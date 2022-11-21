package br.com.gruposbf.burnwood.config;

import lombok.Data;
import lombok.NonNull;

@Data
public class RegistryAccess {
    @NonNull
    private final String schemaRegistryUrl;
    @NonNull
    private final String schemaRegistryUser;
    @NonNull
    private final String schemaRegistryPassword;
}
