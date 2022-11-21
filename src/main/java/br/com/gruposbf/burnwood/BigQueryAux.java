package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.BigQueryTable;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.cloud.bigquery.Field;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import static com.google.cloud.bigquery.TableDefinition.Type.TABLE;
import static com.google.cloud.bigquery.TimePartitioning.Type.DAY;

public class BigQueryAux {
    static Logger log = Logger.getLogger(BigQueryAux.class.getName());

    public void writeBigQuery (BigQueryTable bigQueryTable,
                               Schema avroSchema, PCollection<KV<String, GenericRecord>> rows) {
        String projectId = bigQueryTable.getProjectId();
        String datasetName = bigQueryTable.getDatasetName();
        String tableName = bigQueryTable.getTableName();
        BigQueryTable.WriteMode writeMode = bigQueryTable.getWriteMode();

        log.log(Level.INFO, "writeBigQuery: " + projectId +":" + datasetName + ":" + tableName);
        log.log(Level.INFO, avroSchema.toString());

        org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(avroSchema);
        log.log(Level.INFO, "beamSchema: " + beamSchema.toString());

//        BigQueryIO.Write.CreateDisposition createDisposition = tableExists(projectId, datasetName, tableName)
//                ? BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED : BigQueryIO.Write.CreateDisposition.CREATE_NEVER;

        BigQueryIO.Write.CreateDisposition createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
        BigQueryIO.Write.WriteDisposition writeDisposition = writeMode.equals(BigQueryTable.WriteMode.REPLACE) ?
                BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
                : writeMode.equals(BigQueryTable.WriteMode.APPEND) ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                : BigQueryIO.Write.WriteDisposition.WRITE_EMPTY;

        TableReference tableSpec = new TableReference()
                .setProjectId(projectId)
                .setDatasetId(datasetName)
                .setTableId(tableName);

        log.log(Level.INFO, "tableSpec: " + tableSpec);

        rows.apply(Values.<GenericRecord>create())
                .setSchema(beamSchema,
                        TypeDescriptor.of(GenericRecord.class),
                        AvroUtils.getToRowFunction(GenericRecord.class, avroSchema),
                        AvroUtils.getFromRowFunction(GenericRecord.class))
                .apply(BigQueryIO.<GenericRecord>write()
                        .to(tableSpec)
                        .useBeamSchema()
                        .withCreateDisposition(createDisposition)
                        .withWriteDisposition(writeDisposition));

        log.log(Level.INFO,"Fim");
    }

    public boolean tableExists(String datasetName, String tableName) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            Table table = bigquery.getTable(TableId.of(datasetName, tableName));
            if (table != null
                    && table
                    .exists()) { // table will be null if it is not found and setThrowNotFound is not set
                // to `true`
                log.log(Level.INFO, "Table found: " + datasetName + ":" + tableName);
                return true;
            } else {
                log.log(Level.INFO, "Table not found: " + datasetName + ":" + tableName);
            }
        } catch (BigQueryException e) {
            log.log(Level.INFO, "Table not found: " + datasetName + ":" + tableName);
            log.log(Level.SEVERE, "Table not found. \n" + e);
        }
        return false;
    }

    public com.google.cloud.bigquery.Schema getTableSchema(String datasetName, String tableName) {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table table = bigquery.getTable(TableId.of(datasetName, tableName));
        com.google.cloud.bigquery.Schema bqShema = table.getDefinition().getSchema();

        return bqShema;
    }

    private JSONArray getSchemaContractFields(String contract) {
        JSONObject burnwoodContract = new JSONObject(contract);
        JSONObject schema = (JSONObject)burnwoodContract.get("schema_contract");

        JSONArray fields = (JSONArray)schema.get("properties");

        return fields;
    }

    public com.google.cloud.bigquery.Schema getBigQuerySchema (String contract) {

        JSONArray bwFields = getSchemaContractFields(contract);
        ArrayList<com.google.cloud.bigquery.Field> bqFields = new ArrayList<com.google.cloud.bigquery.Field>();

        for (int f = 0; f < bwFields.length(); f++) {
            JSONObject field = (JSONObject)bwFields.get(f);
            String name = (String)field.get("field_name");
            Object type = field.get("type");
            String bqType = type instanceof String ? type.toString() : "STRING";
            String mode = (String)field.get("mode");
            String description = (String)field.get("description");
            //long maxLength = field.get("maxLength") != null ? (long)field.get("maxLength") : null;
            long maxLength = field.optLong("maxLength");
            long scale = field.optLong("scale");
            long precision = field.optLong("precision");
            String defaultValueExpression = field.optString("defaultValueExpression");

            LegacySQLTypeName sqlType = LegacySQLTypeName.valueOf(bqType);

            com.google.cloud.bigquery.Field bqField = Field.newBuilder(name, sqlType)
                    .setMode(Field.Mode.valueOf(mode)).build();

            if (description != null &&  description.length() > 0)
                bqField = bqField.toBuilder().setDescription(description).build();
            if (maxLength > 0)
                bqField = bqField.toBuilder().setMaxLength(maxLength).build();
            if (scale > 0)
                bqField = bqField.toBuilder().setScale(scale).build();
            if (precision > 0)
                bqField = bqField.toBuilder().setPrecision(precision).build();
            if (defaultValueExpression != null &&  defaultValueExpression.length() > 0)
                bqField = bqField.toBuilder().setDefaultValueExpression(defaultValueExpression).build();

            bqFields.add(bqField);
        }
        com.google.cloud.bigquery.Schema bqSchema = com.google.cloud.bigquery.Schema.of(bqFields);

        return bqSchema;
    }

    public static boolean createTable(String datasetName, String tableName, com.google.cloud.bigquery.Schema schema, TimePartitioning partitioning) {
        try {
            TableDefinition.Type tableType = TABLE;

            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            StandardTableDefinition tableDefinition =
                    StandardTableDefinition.newBuilder()
                            .setSchema(schema)
                            .setType(tableType)
                            .setTimePartitioning(partitioning)
                            .setRangePartitioning(null)
                            .build();
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            Table table = bigquery.create(tableInfo);
            log.log(Level.INFO, "Table created successfully");
            return true;
        } catch (BigQueryException e) {
            log.log(Level.WARNING, "Table was not created. \n" + e);
        }
        return false;
    }

    public boolean createTable(String contract) {

        String datasetName = getLayer(contract);
        String tableName = getTableName(contract);


        JSONObject burnwoodContract = new JSONObject(contract);
        JSONObject schema = (JSONObject)burnwoodContract.get("schema_contract");
        TimePartitioning partitioning = TimePartitioning.of(DAY);

        com.google.cloud.bigquery.Schema bqSchema = getBigQuerySchema(contract);
        boolean ok = createTable(datasetName, tableName, bqSchema, partitioning);

        return ok;
    }

    public String getLayer(String contract) {
        JSONObject burnwoodContract = new JSONObject(contract);

        String id = (String) burnwoodContract.get("id");
        String layer = id.split("\\.")[3];
        return layer;
    }

    public String getTableName(String contract) {
        JSONObject burnwoodContract = new JSONObject(contract);

        String name = (String) burnwoodContract.get("id");
        String tableName = name.replace(".", "_");

        return tableName;
    }

    public String readContract(String contractPath) {
        String contract = "";
        try {
            Path filePath = Paths.get(contractPath);
            contract = new String(Files.readAllBytes(filePath));
        } catch (Exception e) {
            log.log(Level.WARNING, "Could not read from: " + contractPath + e);
        }
        return contract;
    }
}
