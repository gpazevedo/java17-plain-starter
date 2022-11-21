package br.com.gruposbf.burnwood;

import br.com.gruposbf.burnwood.config.BigQueryTable;
import br.com.gruposbf.burnwood.config.KafkaAccess;
import br.com.gruposbf.burnwood.config.RegistryAccess;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.time.Instant.now;


public class KafkaAux {
    static Logger log = Logger.getLogger(KafkaAux.class.getName());

    public Map<String, Object> getRegistryConfigs (RegistryAccess registryAccess) {
        String registryAuth = MessageFormat.format("{0}:{1}", registryAccess.getSchemaRegistryUser(), registryAccess.getSchemaRegistryPassword());
        Map<String, Object> schemaRegistryConfigs = new HashMap<>();
        schemaRegistryConfigs.put("basic.auth.credentials.source", "USER_INFO");
        schemaRegistryConfigs.put("basic.auth.user.info", registryAuth);
        schemaRegistryConfigs.put("schema.registry.url", registryAccess.getSchemaRegistryUrl());

        return schemaRegistryConfigs;
    }

    public Map<String, Object> getConsumerConfigs(KafkaAccess kafkaAccess, String consumerGroup, boolean autoCommit) {

        String kafkaJaasConfig = MessageFormat.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";",
                kafkaAccess.getKafkaUser(), kafkaAccess.getKafkaPassword());

        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerConfigs.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        consumerConfigs.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        consumerConfigs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000);

        consumerConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        consumerConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaJaasConfig);
        consumerConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        consumerConfigs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(autoCommit));
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return consumerConfigs;
    }

    public Map<String, Object> getProducerConfigs (KafkaAccess kafkaAccess, RegistryAccess registryAccess) {
        String kafkaJaasConfig = MessageFormat.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";",
                kafkaAccess.getKafkaUser(), kafkaAccess.getKafkaPassword());

        String registryAuth = MessageFormat.format("{0}:{1}", registryAccess.getSchemaRegistryUser(), registryAccess.getSchemaRegistryPassword());

        Map<String, Object> producerConfigs = new HashMap<>();

        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAccess.getBootstrapServers());
        producerConfigs.put(SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "https");
        producerConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        producerConfigs.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaJaasConfig);
        producerConfigs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        producerConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 500);
        producerConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        producerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 12000);

        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        producerConfigs.put("acks", "0"); // 0: fire & forget
        producerConfigs.put("schema.registry.url", registryAccess.getSchemaRegistryUrl());
        producerConfigs.put("basic.auth.credentials.source", "USER_INFO");
        producerConfigs.put("basic.auth.user.info", registryAuth);

        return producerConfigs;
    }

    public SchemaMetadata getTopicSchema(RegistryAccess registryAccess, String topic) {
        log.info("getTopicSchema topic: " + topic);

        Map<String, Object> schemaRegistryConfigs = getRegistryConfigs(registryAccess);
        String registryHost = schemaRegistryConfigs.get("schema.registry.url").toString();

        RestService schemaRegistryRestService = new RestService(registryHost);

        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryRestService, 10, schemaRegistryConfigs);
        try {
            SchemaMetadata latestSchemaMetadata = registryClient.getLatestSchemaMetadata(topic + "-value");
            return latestSchemaMetadata;

        } catch (Exception e) {
            log.log(Level.WARNING, e.getMessage());
        }
        return null;
    }
    
    public Schema getAvroSchema(RegistryAccess registryAccess, String topic) {
        log.info("getAvroSchema topic: " + topic);
        Schema avroSchema = null;

        Map<String, Object> schemaRegistryConfigs = getRegistryConfigs(registryAccess);
        String registryHost = schemaRegistryConfigs.get("schema.registry.url").toString();

        RestService schemaRegistryRestService = new RestService(registryHost);

        SchemaRegistryClient registryClient = new CachedSchemaRegistryClient(schemaRegistryRestService, 10, schemaRegistryConfigs);
        log.log(Level.INFO, "registryClient: " + registryClient);
        try {
            SchemaMetadata latestSchemaMetadata = registryClient.getLatestSchemaMetadata(topic + "-value");
            avroSchema = new Schema.Parser().parse(latestSchemaMetadata.getSchema());

        } catch (Exception e) {
            log.log(Level.WARNING, e.getMessage());
        }
        log.log(Level.INFO, "avroSchema: " + avroSchema);

        return avroSchema;
    }

    public int kafkaProducer(String topic, int mesgs, KafkaAccess kafkaAccess, RegistryAccess registryAccess) {
        log.info("Kafka Producer para " + topic);

        Schema avroSchema = getAvroSchema(registryAccess, topic);
        String stringTopicSchema = getTopicSchema(registryAccess, topic).getSchema();
        JSONObject jsonTopicSchema = new JSONObject(stringTopicSchema);

        Map<String, Object> producerConfigs = getProducerConfigs(kafkaAccess, registryAccess);
        int sent = 0;

        try(KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerConfigs)) {
            GenericData.Record avroRecord = new GenericData.Record(avroSchema);
            List<Schema.Field> fields = avroSchema.getFields();
            JSONArray jsonFields = (JSONArray) jsonTopicSchema.get("fields");

            for (int m = 0; m < mesgs; m++) {

                for (int f = 0; f < fields.size(); f++) {
                    String fieldName = fields.get(f).name();
                    Object fieldSchema = fields.get(f).schema();
                    System.out.println(fieldName + ": " + fieldSchema.getClass().toString());

                    JSONObject jsonField = (JSONObject) jsonFields.get(f);
                    Object type = jsonField.get("type");

                    Object value = randomValue(type);
                    avroRecord.put(fieldName, value);

                }
                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, avroRecord);
                log.log(Level.INFO, "Producer com avroRecord: " + record);

                /*Future<RecordMetadata> abc =*/ producer.send(record, (metadata, e) -> {
                    // executes every time a record is successfully sent or an exception is thrown
                    log.log(Level.INFO, "Sent: " + avroRecord);
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.log(Level.WARNING, "Error while producing: " + e);
                    }
                });
                sent++;
            }
            producer.flush();
            producer.close();
            log.log(Level.INFO, mesgs + " Mensagens geradas para o topico: " + topic);

        } catch (final SerializationException e) {
            e.printStackTrace();
        }
        return sent;
    }

    public Object randomValue(Object type) {
        Random rnd = new Random();
        Object value = null;

        if (type instanceof String) {
            switch (type.toString()) {

                case "boolean":
                    value = rnd.nextBoolean();
                    break;

                case "int":
                    value = rnd.nextInt(2 ^ 31);
                    break;

                case "long":
                    value = rnd.nextLong();
                    break;

                case "float":
                    value = rnd.nextFloat();
                    break;

                case "double":
                    value = rnd.nextDouble();
                    break;

                case "bytes":
                    break;

                case "string":
                    value = UUID.randomUUID().toString();
                    break;

                case "time-micros":
                case "timestamp-micros":
                default:
                    log.log(Level.WARNING,  "Type (" + type + ") not allowed!");
            }
        }
        else {
            JSONObject aType;
            if (type instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray)type;
                Object obj = jsonArray.get(0);
                if (obj instanceof String && obj.equals("null")) {
                    aType = (JSONObject)jsonArray.get(1);
                }
                else aType = (JSONObject)jsonArray.get(0);
            }
            else {
                aType = (JSONObject)type;
            }

            String logicalType = (String)aType.opt("logicalType");

            switch (logicalType) {
                case "date":
                    //bqType = "DATE";
                    break;

                case "time-millis":
                    //bqType = "TIME";
                    break;

                case "timestamp-millis":
                    value = now().toEpochMilli();
                    break;

                case "decimal":
                    Integer precision = (Integer)aType.opt("precision");
                    Integer scale = (Integer)aType.opt("scale");

                    if (scale > 9 || precision - scale > 29)
                        value = rnd.nextFloat();
                        //bqType = "NUMERIC";
                    else if (precision < 77)
                        //bqType = "BIGNUMERIC";
                        value = rnd.nextDouble();
                    break;

                default:
                    log.log(Level.WARNING,  "Type (" + type + ") not allowed!");
                    break;
            }
        }
        return value;
    }

    public long countMessages (String topic, String consumerGroup, KafkaAccess kafkaAccess, RegistryAccess registryAccess) {

        Map<String, Object> consumerConfigs = getConsumerConfigs(kafkaAccess, consumerGroup, false);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerConfigs.put("schema.registry.url", registryAccess.getSchemaRegistryUrl());
        consumerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAccess.getBootstrapServers());

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerConfigs);

        List<TopicPartition> partitions = consumer.partitionsFor(topic)
                .stream().map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());

        consumer.assign(partitions);
        consumer.seekToEnd(Collections.emptySet());
        Map<TopicPartition, Long> endPartitions = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), consumer::position));

        consumer.close();

        return partitions.stream().mapToLong(endPartitions::get).sum();
    }

    public int kafka2BigQuery (String topic, KafkaAccess kafkaAccess, RegistryAccess registryAccess, String consumerGroup, BigQueryTable bigQueryTable) {

        Schema avroSchema = getAvroSchema(registryAccess, topic);
        String[] args = new String[]{
                "--runner=DataflowRunner",
                "--project=" + bigQueryTable.getProjectId(),
                "--region=" + "us-east1",
                "--tempLocation=gs://dev-burnwood/temp/",
                "--subnetwork=https://www.googleapis.com/compute/v1/projects/dc-interconnect/regions/us-east1/subnetworks/data-platform-dev-use-02"
        };

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, GenericRecord>> records = beamConsumer(p, consumerGroup, kafkaAccess, registryAccess, topic);

        BigQueryAux bq = new BigQueryAux();
        bq.writeBigQuery(bigQueryTable, avroSchema, records);

        p.run().waitUntilFinish();
        log.log(Level.INFO, "The End!");

        return 1;
    }

    public PCollection<KV<String, GenericRecord>> beamConsumer(Pipeline p, String consumerGroup, KafkaAccess kafkaAccess, RegistryAccess registryAccess, String topic) {

        Map<String, Object> schemaRegistryConfigs = getRegistryConfigs(registryAccess);
        String registryHost = schemaRegistryConfigs.get("schema.registry.url").toString();

        log.log(Level.INFO, registryHost + " Consumindo de " + topic + " através de " + consumerGroup);

        Map<String, Object> consumerConfigs = getConsumerConfigs(kafkaAccess, consumerGroup, false);

        PCollection<KV<String, GenericRecord>> records = p.apply("Beam Read", KafkaIO.<String, GenericRecord>read()
                .withBootstrapServers(kafkaAccess.getBootstrapServers())
                .withTopic(topic)
                .withConsumerConfigUpdates(consumerConfigs)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ConfluentSchemaRegistryDeserializerProvider.of(
                        registryHost,
                        1000,
                        topic + "-value",
                        null,
                        schemaRegistryConfigs))
                .withoutMetadata());

        return records;
    }
}
