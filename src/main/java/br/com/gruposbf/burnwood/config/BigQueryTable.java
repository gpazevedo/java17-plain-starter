package br.com.gruposbf.burnwood.config;

import lombok.Data;
import lombok.NonNull;

@Data
public class BigQueryTable {
    private final String projectId;
    @NonNull
    private final String datasetName;
    @NonNull
    private final String tableName;
    private final WriteMode writeMode;

    public enum WriteMode {
        APPEND,
        REPLACE,
        EMPTY
    }
}
