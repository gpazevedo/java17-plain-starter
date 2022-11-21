package br.com.gruposbf.burnwood;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BigQueryAuxTest {

    @Test
    public void bigQueryProjectId() {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        String projectId = bigquery.getOptions().getProjectId();
        assertEquals("dev-data-platform-291914", projectId);
    }

    @Test
    public void tableExists() {
        String datasetName = "Trusted";
        String tableName = "cnt_digital_trd_canal_movimento";
        BigQueryAux bigQueryAux = new BigQueryAux();
        assertTrue(bigQueryAux.tableExists(datasetName, tableName));
    }

    @Test
    public void tableSchema() {
        String datasetName = "Trusted";
        String tableName = "cnt_digital_trd_canal_movimento";
        BigQueryAux bigQueryAux = new BigQueryAux();
        Schema bqSchema = bigQueryAux.getTableSchema(datasetName, tableName);
        assertNotNull(bqSchema);
    }
    
    @Test
    public void createBQTableTest() {
        String srcFolder = "src/test/organization/fisia-data-lake/dev/trusted/pedido/";
        String contractPath = srcFolder + "fis-order-dev-order-metrics-v2-bq.json";

        BigQueryAux bigQueryAux = new BigQueryAux();
        String contract = bigQueryAux.readContract(contractPath);

        boolean ok = bigQueryAux.createTable(contract);
    }
}