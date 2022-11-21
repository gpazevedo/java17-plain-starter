package br.com.gruposbf.burnwood;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaTwin
{
    static Logger log = Logger.getLogger(KafkaTwin.class.getName());

    public static void main( String[] args )
    {
        String kafkaContract = "";
        String kafkaContractFile = args.length  == 0 ? "" : args[0];
        
        log.info("Kafka Contract file: " + kafkaContractFile);

        try {
            Path filePath = Paths.get(kafkaContractFile);
            kafkaContract = Files.readString(filePath);
        } catch (Exception e) {
            log.log(Level.WARNING, "Could not read from: " + kafkaContractFile + "\n" + e);
        }

        if (kafkaContract.length() > 0) {
            ContractUtils contractUtils = new ContractUtils();
            String bqContract = contractUtils.convertAvro2BigQuery(kafkaContract);
            
            if (bqContract != null) {
                String bqContractFile = kafkaContractFile.substring(0, kafkaContractFile.length() - 5) + "-bq.json";

                try {
                    Path path = Paths.get(bqContractFile);
                    Files.writeString(path, bqContract);
                } catch (Exception e) {
                    //IllegalArgumentException
                    //IOException
                    //SecurityException
                    log.log(Level.WARNING, "Could not write to: " + bqContractFile + "\n" + e);
                }
            }
        }
    }
}
