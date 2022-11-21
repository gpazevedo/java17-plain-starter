package br.com.gruposbf.burnwood;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ContractUtils {
    static Logger log = Logger.getLogger(ContractUtils.class.getName());

    public String convertAvro2BigQuery(String contract) {
        JSONObject avroContract = new JSONObject(contract);
        JSONObject bqContract = updateSchemaContractFields(avroContract);

        return bqContract.toString(4);
    }

    public JSONObject updateSchemaContractFields(JSONObject avroContract) {

        String kafkaName = (String)avroContract.get("name");
        String bigQueryName = getBigQueryName(kafkaName);
        avroContract.put("name", bigQueryName);

        String kafkaId = (String)avroContract.get("id");
        String bigQueryId = kafkaId + "-bq";
        avroContract.put("id", bigQueryId);

        JSONObject avroSchema = (JSONObject)avroContract.get("schema_contract");
        JSONArray fields = (JSONArray)avroSchema.get("properties");

        JSONArray bqFields = new JSONArray();

        for (int i = 0; i < fields.length(); i++) {
            JSONObject avroField = (JSONObject)fields.get(i);
            JSONObject bigQueryField = avroToBQField(avroField);
            bqFields.put(i, bigQueryField);
        }

        JSONObject bqSchema = (JSONObject)avroContract.get("schema_contract");
        bqSchema.put("type", "bigquery");

        JSONObject newSchema = bqSchema.put("properties", bqFields);
        JSONObject updatedContract = avroContract.put("schema_contract", newSchema);

        JSONObject metadata = (JSONObject)updatedContract.get("metadata");
        metadata.put("source_format", "native");
        updatedContract.put("metadata", metadata);

        return updatedContract;
    }

    public JSONObject avroToBQField(JSONObject field) {
        String bqType = "";

        String mode = (String)field.get("mode");
        if(!Arrays.asList("NULLABLE", "REQUIRED", "REPEATED").contains(mode)) {
            mode = "REQUIRED";
        }

        Object type = field.get("type");
        if (type instanceof String) {
            switch (type.toString()) {

                case "boolean":
                    bqType = "BOOLEAN";
                    break;

                case "int":
                    bqType = "INTEGER";
                    break;

                case "long":
                    bqType = "LONG";
                    break;

                case "float":
                case "double":
                    bqType = "FLOAT";
                    break;

                case "bytes":
                    bqType = "BYTES";
                    break;

                case "string":
                    bqType = "STRING";
                    break;

                case "time-micros":
                case "timestamp-micros":
                default:
                    log.log(Level.WARNING, field.get("field_name") + " has type (" + type + ") not allowed!");
            }
        }
        else {
            JSONObject aType;
            if (type instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray)type;
                Object obj = jsonArray.get(0);
                if (obj instanceof String && obj.equals("null")) {
                    aType = (JSONObject)jsonArray.get(1);
                    mode = "NULLABLE";
                }
                else aType = (JSONObject)jsonArray.get(0);
            }
            else {
                aType = (JSONObject)field.get("type");
            }

            String logicalType = (String)aType.opt("logicalType");

            switch (logicalType) {
                case "date":
                    bqType = "DATE";
                    break;

                case "time-millis":
                    bqType = "TIME";
                    break;

                case "timestamp-millis":
                    bqType = "TIMESTAMP";
                    break;

                case "decimal":
                    Integer precision = (Integer)aType.opt("precision");
                    Integer scale = (Integer)aType.opt("scale");

                    if (scale > 9 || precision - scale > 29)
                        bqType = "NUMERIC";
                    else if (precision < 77)
                        bqType = "BIGNUMERIC";
                    break;

                default:
                    log.log(Level.WARNING, field.get("field_name") + " has logicalType (" + logicalType + ") not allowed!");
                    bqType = "";
                    break;
            }
        }
        field.put("mode", mode);

        log.log(Level.INFO, "Type: " + type + " bqType: " + bqType);
        field.put("type", bqType);
        return field;
    }

    public String getBigQueryName(String avroName) {
        return avroName.replace('.', '_');
    }
}
