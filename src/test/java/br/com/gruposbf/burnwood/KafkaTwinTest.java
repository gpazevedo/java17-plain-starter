package br.com.gruposbf.burnwood;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test for simple App.
 */
public class KafkaTwinTest
{
    String contract = "{\n" +
            "   \"id\": \"gruposbf.allocation.prd.raw.ztmm-vinculo-ped.v0\",\n" +
            "   \"name\": \"gruposbf.allocation.prd.sap-hana.ztmm-vinculo-ped.v0\",\n" +
            "   \"schema_contract\": {\n" +
            "      \"type\": \"kafka\",\n" +
            "      \"description\": \"Dados SAP Pedidos tabela ZTMM_VINCULO_PED\",\n" +
            "      \"partition\": {\n" +
            "         \"type\": \"\",\n" +
            "         \"config\": {}\n" +
            "      },\n" +
            "      \"properties\": [\n" +
            "         {\n" +
            "            \"field_name\": \"DOCNUM\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"\",\n" +
            "            \"description\": \"Numero da Chave da NFe\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"ITMNUM\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Numero item do documento\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"EBELN\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"\",\n" +
            "            \"description\": \"Numero do documento de compras\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"EBELP\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Numero item do documento de compra\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"MEINS\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"\",\n" +
            "            \"description\": \"Unidade de medida do pedido\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"MENGE\",\n" +
            "            \"type\": {\n" +
            "               \"logicalType\": \"decimal\",\n" +
            "               \"precision\": 38,\n" +
            "               \"scale\": 3,\n" +
            "               \"type\": \"bytes\"\n" +
            "            },\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Quantidade\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"MATNR\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"\",\n" +
            "            \"description\": \"Numero do material\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"KTMNG\",\n" +
            "            \"type\": {\n" +
            "               \"logicalType\": \"decimal\",\n" +
            "               \"precision\": 38,\n" +
            "               \"scale\": 3,\n" +
            "               \"type\": \"bytes\"\n" +
            "            },\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Quantidade prevista\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"EBELN_TRANSF\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"\",\n" +
            "            \"description\": \"Numero do documento de compras\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"EBELP_TRANSF\",\n" +
            "            \"type\": \"string\",\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Numero item do documento de compra\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"LASTCHANGEDATETIME\",\n" +
            "            \"type\": [\n" +
            "               \"null\",\n" +
            "               {\n" +
            "                  \"logicalType\": \"timestamp-millis\",\n" +
            "                  \"type\": \"long\"\n" +
            "               }\n" +
            "            ],\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Data da ultima alteracao\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"DAT_CARGA\",\n" +
            "            \"type\": [\n" +
            "               \"null\",\n" +
            "               {\n" +
            "                  \"logicalType\": \"date\",\n" +
            "                  \"type\": \"int\"\n" +
            "               }\n" +
            "            ],\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Data da extracao\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         },\n" +
            "         {\n" +
            "            \"field_name\": \"REGISTROS\",\n" +
            "            \"type\": \"long\",\n" +
            "            \"mode\": \"REQUIRED\",\n" +
            "            \"description\": \"Quantidade de registros na origem\",\n" +
            "            \"deidentify\": {\n" +
            "               \"transformation\": \"\",\n" +
            "               \"info_types\": []\n" +
            "            },\n" +
            "            \"policyTags\": {\n" +
            "               \"names\": []\n" +
            "            },\n" +
            "            \"great_expectation\": [\n" +
            "               {\n" +
            "                  \"expectation_function\": \"expect_column_values_to_not_be_null\",\n" +
            "                  \"expectation_param\": []\n" +
            "               }\n" +
            "            ]\n" +
            "         }\n" +
            "      ]\n" +
            "   },\n" +
            "   \"metadata\": {\n" +
            "      \"data_owner\": \"Alocacao\",\n" +
            "      \"data_owner_email\": \"prod.alocacao@gruposbf.com.br\",\n" +
            "      \"data_custodian\": \"Alocacao\",\n" +
            "      \"data_custodian_email\": \"prod.alocacao@gruposbf.com.br\",\n" +
            "      \"ingestion_method\": \"batch\",\n" +
            "      \"ingestion_cron\": \"*/20 * * * *\",\n" +
            "      \"upstream_lineage\": [],\n" +
            "      \"source_format\": \"parquet\",\n" +
            "      \"bu\": \"gruposbf\",\n" +
            "      \"bu_code\": \"\",\n" +
            "      \"tags\": []\n" +
            "   }\n" +
            "}";


    @Test
    public void testApp()
    {
        assertTrue( true );
    }
}
