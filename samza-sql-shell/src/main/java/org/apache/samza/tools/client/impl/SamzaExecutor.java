package org.apache.samza.tools.client.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.avro.Schema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.fn.FlattenUdf;
import org.apache.samza.sql.fn.RegexMatchUdf;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProviderFactory;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplication;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.ReflectionUtils;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.eventhub.EventHubSystemFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.tools.avro.AvroSchemaGenRelConverterFactory;
import org.apache.samza.tools.avro.AvroSerDeFactory;
import org.apache.samza.tools.client.interfaces.*;
import org.apache.samza.tools.client.util.RandomAccessQueue;
import org.apache.samza.tools.json.JsonRelConverterFactory;
import org.apache.samza.tools.schemas.ProfileChangeEvent;
import org.jline.utils.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;
import scala.collection.JavaConversions;

import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.*;
import static org.apache.samza.tools.client.util.CliUtil.*;


public class SamzaExecutor implements SqlExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutor.class);
    private static final String SAMZA_SYSTEM_KAFKA = "kafka";
    private static final String SAMZA_SYSTEM_EVENTHUBS = "eventhub";
    private static final String SAMZA_SYSTEM_LOG = "log";
    private static final int RANDOM_ACCESS_QUEUE_CAPACITY = 5000;

    private static class SamzaExecution {
        SamzaSqlApplicationRunner runner;
        SamzaSqlApplication app;

        SamzaExecution(SamzaSqlApplicationRunner runner, SamzaSqlApplication app) {
            this.runner = runner;
            this.app = app;
        }

        ExecutionStatus getExecutionStatus() {
            switch (runner.status(app).getStatusCode()) {
                case New:
                    return ExecutionStatus.New;
                case Running:
                    return ExecutionStatus.Running;
                case SuccessfulFinish:
                    return ExecutionStatus.SuccessfulFinish;
                case UnsuccessfulFinish:
                    return ExecutionStatus.UnsuccessfulFinish;
                default:
                    throw new ExecutionException(String.format("Unsupported execution status %s",
                        runner.status(app).getStatusCode().toString()));
            }
        }
    }
    private static AtomicInteger m_execIdSeq = new AtomicInteger(0);
    private ConcurrentHashMap<Integer, SamzaExecution> m_executors = new ConcurrentHashMap<>();
    private static RandomAccessQueue<OutgoingMessageEnvelope> m_outputData =
        new RandomAccessQueue<>(OutgoingMessageEnvelope.class, RANDOM_ACCESS_QUEUE_CAPACITY);
    private String m_lastestErrorMsg = "";

    // -- implementation of SqlExecutor ------------------------------------------

    @Override
    public void start(ExecutionContext context) {

    }

    @Override
    public void stop(ExecutionContext context) {
        for (int execId : m_executors.keySet()) {
            stopExecution(null, execId);
            removeExecution(null, execId);
        }
        m_outputData.clear();
    }

    @Override
    public List<String> listTables(ExecutionContext context) {
        String address = "localhost:2181";
        ZkUtils zkUtils = new ZkUtils(new ZkClient(address), new ZkConnection(address), false);
        List<String> tables = JavaConversions.seqAsJavaList(zkUtils.getAllTopics())
            .stream()
            .map(x -> SAMZA_SYSTEM_KAFKA + "." + x)
            .collect(Collectors.toList());

        tables.add(SAMZA_SYSTEM_EVENTHUBS + "." + "teststream1");
        tables.add(SAMZA_SYSTEM_EVENTHUBS + "." + "teststream2");

        return tables;
    }

    @Override
    public SqlSchema getTableScema(ExecutionContext context, String tableName) {
        int execId = m_execIdSeq.incrementAndGet();
        Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
        Config samzaSqlConfig = new MapConfig(staticConfigs);
        SqlIOResolver ioResolver = SamzaSqlApplicationConfig.createIOResolver(samzaSqlConfig);
        SqlIOConfig sourceInfo = ioResolver.fetchSourceInfo(tableName);
        RelSchemaProvider schemaProvider =
            initializePlugin("RelSchemaProvider", sourceInfo.getRelSchemaProviderName(), samzaSqlConfig,
                CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN,
                (o, c) -> ((RelSchemaProviderFactory) o).create(sourceInfo.getSystemStream(), c));

        AvroRelSchemaProvider avroSchemaProvider = (AvroRelSchemaProvider) schemaProvider;
        String schema = avroSchemaProvider.getSchema(sourceInfo.getSystemStream());

        return convertAvroToSamzaSqlSchema(schema);
    }

    @Override
    public QueryResult executeQuery(ExecutionContext context, String statement) {
        List<String> sqlStmts = formatSqlStmts(Collections.singletonList(statement));

        int execId = m_execIdSeq.incrementAndGet();
        Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
        staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

        SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
        SamzaSqlApplication app = new SamzaSqlApplication();
        runner.run(app);

        m_executors.put(execId, new SamzaExecution(runner, app));
        LOG.debug("Executing sql. Id ", execId);

        return new QueryResult(execId, generateResultSchema(new MapConfig(staticConfigs)));
    }

    @Override
    public int getRowCount() {
        return m_outputData.getSize();
    }

    @Override
    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) {
        List<String[]> results = new ArrayList<>();
        for (OutgoingMessageEnvelope row : m_outputData.get(startRow, endRow)) {
            results.add(getFormattedRow(context, row));
        }
        return results;
    }

    @Override
    public List<String[]> consumeQueryResult(ExecutionContext context, int startRow, int endRow) {
        List<String[]> results = new ArrayList<>();
        for (OutgoingMessageEnvelope row : m_outputData.consume(startRow, endRow)) {
            results.add(getFormattedRow(context, row));
        }
        return results;
    }

    @Override
    public NonQueryResult executeNonQuery(ExecutionContext context, URI sqlFile) {
        m_lastestErrorMsg = "";

        Log.info("Sql file path: " + sqlFile.getPath());
        List<String> executedStmts = new ArrayList<>();
        try {
            executedStmts = Files.lines(Paths.get(sqlFile.getPath())).collect(Collectors.toList());
        } catch (IOException e) {
            String msg = String.format("Unable to parse the sql file %s", sqlFile.getPath());
            LOG.error(msg, e);
            throw new SamzaException(msg, e);
        }
        Log.info("Sql statements in Sql file: " + executedStmts.toString());

        List<String> submittedStmts = new ArrayList<>();
        List<String> nonSubmittedStmts = new ArrayList<>();
        validateExecutedStmts(executedStmts, submittedStmts, nonSubmittedStmts);
        NonQueryResult result =  executeNonQuery(context, submittedStmts);
        return new NonQueryResult(result.getExecutionId(), result.succeeded(), submittedStmts, nonSubmittedStmts);
    }

    @Override
    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement) {
        statement = formatSqlStmts(statement);

        int execId = m_execIdSeq.incrementAndGet();
        Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
        staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(statement));

        SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
        SamzaSqlApplication app = new SamzaSqlApplication();
        runner.run(app);

        m_executors.put(execId, new SamzaExecution(runner, app));
        LOG.debug("Executing sql. Id ", execId);

        return new NonQueryResult(execId, true);
    }

    @Override
    public boolean stopExecution(ExecutionContext context, int exeId) {
        SamzaExecution exec = m_executors.get(exeId);
        if(exec != null) {
            exec.runner.kill(exec.app);
            m_outputData.clear();
            LOG.debug("Stopping execution ", exeId);

            try {
                Thread.sleep(500); // wait for a second
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return true;
        } else {
            LOG.warn("Trying to stop a non-existing SQL execution ", exeId);
            return false;
        }
    }

    @Override
    public boolean removeExecution(ExecutionContext context, int exeId) {
        SamzaExecution exec = m_executors.get(exeId);
        if(exec != null) {
            if (exec.getExecutionStatus().equals(ExecutionStatus.Running)) {
                LOG.error("Trying to remove a ongoing execution ", exeId);
                return false;
            }
            m_executors.remove(exeId);
            LOG.debug("Stopping execution ", exeId);
            return true;
        } else {
            LOG.warn("Trying to remove a non-existing SQL execution ", exeId);
            return false;
        }
    }

    @Override
    public ExecutionStatus queryExecutionStatus(int execId) {
        if (!m_executors.containsKey(execId)) {
            return null;
        }
        return m_executors.get(execId).getExecutionStatus();
    }

    @Override
    public String getErrorMsg() {
        return m_lastestErrorMsg;
    }

    @Override
    public List<SqlFunction> listFunctions(ExecutionContext m_exeContext) {
        List<SqlFunction> udfs = new ArrayList<>();
        udfs.add(new UdfDisplayInfo("RegexMatch", "Matches the string to the regex",
            Arrays.asList(SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING),
                SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING)),
            SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.BOOLEAN)));

        udfs.add(new UdfDisplayInfo("GetSqlField", "Get ane element from complex field (Array, MAP, ROW)",
            Arrays.asList(SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.ANY),
                SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING)),
            SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.ANY)));

        return udfs;

    }

    static RandomAccessQueue<OutgoingMessageEnvelope> getM_outputData() {
        return m_outputData;
    }


    // -- private functions ------------------------------------------

    private String getColumnTypeName(SamzaSqlFieldType fieldType) {
        if (fieldType.isPrimitiveField()) {
            return fieldType.getTypeName().toString();
        } else if (fieldType.getTypeName() == SamzaSqlFieldType.TypeName.MAP) {
            return String.format("MAP(%s)", getColumnTypeName(fieldType.getValueType()));
        } else if (fieldType.getTypeName() == SamzaSqlFieldType.TypeName.ARRAY) {
            return String.format("ARRAY(%s)", getColumnTypeName(fieldType.getElementType()));
        } else {
            SqlSchema schema = fieldType.getRowSchema();
            List<String> fieldTypes = IntStream.range(0, schema.getFieldCount())
                    .mapToObj(i -> schema.getFieldName(i) + " " + schema.getFieldTypeName(i))
                    .collect(Collectors.toList());
            String rowSchemaValue = Joiner.on(", ").join(fieldTypes);
            return String.format("STRUCT(%s)", rowSchemaValue);
        }
    }

    private SqlSchema convertAvroToSamzaSqlSchema(String schema) {
        Schema avroSchema = Schema.parse(schema);
        return getSchema(avroSchema.getFields());
    }

    private SqlSchema getSchema(List<Schema.Field> fields) {
        SqlSchemaBuilder schemaBuilder = SqlSchemaBuilder.builder();
        for (Schema.Field field : fields) {
            schemaBuilder.addField(field.name(), getColumnTypeName(getFieldType(field.schema())));
        }
        return schemaBuilder.toTableSchema();
    }

    private SamzaSqlFieldType getFieldType(org.apache.avro.Schema schema) {

        switch (schema.getType()) {
            case ARRAY:
                return SamzaSqlFieldType.createArrayFieldType(getFieldType(schema.getElementType()));
            case BOOLEAN:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.BOOLEAN);
            case DOUBLE:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.DOUBLE);
            case FLOAT:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.FLOAT);
            case ENUM:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
            case UNION:
                // NOTE: We only support Union types when they are used for representing Nullable fields in Avro
                List<org.apache.avro.Schema> types = schema.getTypes();
                if (types.size() == 2) {
                    if (types.get(0).getType() == org.apache.avro.Schema.Type.NULL) {
                        return getFieldType(types.get(1));
                    } else if ((types.get(1).getType() == org.apache.avro.Schema.Type.NULL)) {
                        return getFieldType(types.get(0));
                    }
                }
            case FIXED:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
            case STRING:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.STRING);
            case BYTES:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.BYTES);
            case INT:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.INT32);
            case LONG:
                return SamzaSqlFieldType.createPrimitiveFieldType(SamzaSqlFieldType.TypeName.INT64);
            case RECORD:
                return SamzaSqlFieldType.createRowFieldType(getSchema(schema.getFields()));
            case MAP:
                return SamzaSqlFieldType.createMapFieldType(getFieldType(schema.getValueType()));
            default:
                String msg = String.format("Field Type %s is not supported", schema.getType());
                LOG.error(msg);
                throw new SamzaException(msg);
        }
    }

    private static <T> T initializePlugin(String pluginName, String plugin, Config staticConfig,
        String pluginDomainFormat, BiFunction<Object, Config, T> factoryInvoker) {
        String pluginDomain = String.format(pluginDomainFormat, plugin);
        Config pluginConfig = staticConfig.subset(pluginDomain);
        String factoryName = pluginConfig.getOrDefault(CFG_FACTORY, "");
        Validate.notEmpty(factoryName, String.format("Factory is not set for %s", plugin));
        Object factory = ReflectionUtils.createInstance(factoryName);
        Validate.notNull(factory, String.format("Factory creation failed for %s", plugin));
        LOG.info("Instantiating {} using factory {} with props {}", pluginName, factoryName, pluginConfig);
        return factoryInvoker.apply(factory, pluginConfig);
    }

    private List<String> formatSqlStmts(List<String> statements) {
        return statements.stream().map(sql -> {
            if (!sql.toLowerCase().startsWith("insert")) {
                String formattedSql = String.format("insert into log.outputStream %s", sql);
                LOG.debug("Sql formatted. ", sql, formattedSql);
                return formattedSql;
            } else {
                return sql;
            }
        }).collect(Collectors.toList());
    }

    private void validateExecutedStmts(List<String> statements, List<String> submittedStmts,
        List<String> nonSubmittedStmts) { ;
        for (String sql: statements) {
            if (sql.isEmpty()) {
                continue;
            }
            if (!sql.toLowerCase().startsWith("insert")) {
                nonSubmittedStmts.add(sql);
            } else {
                submittedStmts.add(sql);
            }
        }
    }

    private SqlSchema generateResultSchema(Config config) {
        SamzaSqlApplicationConfig sqlConfig = new SamzaSqlApplicationConfig(config);
        QueryPlanner planner = new QueryPlanner(
            sqlConfig.getRelSchemaProviders(),
            sqlConfig.getInputSystemStreamConfigBySource(),
            sqlConfig.getUdfMetadata());
        RelRoot relRoot = planner.plan(sqlConfig.getQueryInfo().get(0).getSelectQuery());

        List<String> colNames = new ArrayList<>();
        List<String> colTypeNames = new ArrayList<>();
        for (RelDataTypeField dataTypeField : relRoot.validatedRowType.getFieldList()) {
            colNames.add(dataTypeField.getName());
            // TODO: colTypeNames.add(dataTypeField.getType().toString());
            colTypeNames.add(null);
        }
        return new SqlSchema(colNames, colTypeNames);
    }

    private String[] getFormattedRow(ExecutionContext context, OutgoingMessageEnvelope row) {
        String[] formattedRow = new String[1];
        if (context == null || !context.getMessageFormat().equals(ExecutionContext.MessageFormat.COMPACT)){
            formattedRow[0] = getPrettyFormat(row);
        } else {
            formattedRow[0] = getCompressedFormat(row);
        }
        return formattedRow;
    }

    private static Map<String, String> fetchSamzaSqlConfig(int execId) {
        HashMap<String, String> staticConfigs = new HashMap<>();

        staticConfigs.put(JobConfig.JOB_NAME(), "sql-job-" + execId);
        staticConfigs.put(JobConfig.PROCESSOR_ID(), String.valueOf(execId));
        staticConfigs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
        staticConfigs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

        staticConfigs.put(SamzaSqlApplicationConfig.CFG_IO_RESOLVER, "config");
        String configIOResolverDomain =
                String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
        staticConfigs.put(configIOResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
                ConfigBasedIOResolverFactory.class.getName());

        staticConfigs.put(SamzaSqlApplicationConfig.CFG_UDF_RESOLVER, "config");
        String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
        staticConfigs.put(configUdfResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
                ConfigBasedUdfResolver.class.getName());
        staticConfigs.put(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES,
                Joiner.on(",").join(RegexMatchUdf.class.getName(), FlattenUdf.class.getName(), GetSqlFieldUdf.class.getName()));

        staticConfigs.put("serializers.registry.string.class", StringSerdeFactory.class.getName());
        staticConfigs.put("serializers.registry.avro.class", AvroSerDeFactory.class.getName());
        staticConfigs.put(AvroSerDeFactory.CFG_AVRO_SCHEMA, ProfileChangeEvent.SCHEMA$.toString());

        String kafkaSystemConfigPrefix =
                String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_KAFKA);
        String avroSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_KAFKA);
        staticConfigs.put(kafkaSystemConfigPrefix + "samza.factory", KafkaSystemFactory.class.getName());
        staticConfigs.put(kafkaSystemConfigPrefix + "samza.key.serde", "string");
        staticConfigs.put(kafkaSystemConfigPrefix + "samza.msg.serde", "avro");
        staticConfigs.put(kafkaSystemConfigPrefix + "consumer.zookeeper.connect", "localhost:2181");
        staticConfigs.put(kafkaSystemConfigPrefix + "producer.bootstrap.servers", "localhost:9092");

        staticConfigs.put(kafkaSystemConfigPrefix + "samza.offset.reset", "true");
        staticConfigs.put(kafkaSystemConfigPrefix + "samza.offset.default", "oldest");

        staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
        staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

        String ehSystemConfigPrefix =
            String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_EVENTHUBS);
        String ehSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_EVENTHUBS);
        staticConfigs.put(ehSystemConfigPrefix + "samza.factory", EventHubSystemFactory.class.getName());
        staticConfigs.put(ehSystemConfigPrefix + "stream.list", "teststream1,teststream2");
        staticConfigs.put("streams.teststream1.eventhubs.namespace", "srinieh1");
        staticConfigs.put("streams.teststream1.eventhubs.entitypath", "teststream1");
        staticConfigs.put("sensitive.streams.teststream1.eventhubs.sas.keyname", "WriteKey");
        staticConfigs.put("sensitive.streams.teststream1.eventhubs.sas.token",
            "BFMZOHEBLbukDJcuMrx1S9HjxjjUW3feXuuc4fhD7oA=");
        staticConfigs.put("streams.teststream2.eventhubs.namespace", "srinieh1");
        staticConfigs.put("streams.teststream2.eventhubs.entitypath", "teststream2");
        staticConfigs.put("sensitive.streams.teststream2.eventhubs.sas.keyname", "WriteKey");
        staticConfigs.put("sensitive.streams.teststream2.eventhubs.sas.token",
            "BFMZOHEBLbukDJcuMrx1S9HjxjjUW3feXuuc4fhD7oA=");
        staticConfigs.put(ehSystemConfigPrefix + "samza.offset.reset", "true");
        staticConfigs.put(ehSystemConfigPrefix + "samza.offset.default", "oldest");


        staticConfigs.put(ehSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "json");
        staticConfigs.put(ehSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

        String logSystemConfigPrefix =
                String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_LOG);
        String logSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_LOG);
        staticConfigs.put(logSystemConfigPrefix + "samza.factory", CliLoggingSystemFactory.class.getName());
        staticConfigs.put(logSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "json");
        staticConfigs.put(logSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

        String avroSamzaToRelMsgConverterDomain =
                String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");

        staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
                AvroSchemaGenRelConverterFactory.class.getName());

        String jsonSamzaToRelMsgConverterDomain =
                String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "json");

        staticConfigs.put(jsonSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
                JsonRelConverterFactory.class.getName());

        String configAvroRelSchemaProviderDomain =
                String.format(SamzaSqlApplicationConfig.CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN, "config");
        staticConfigs.put(configAvroRelSchemaProviderDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
                FileSystemAvroRelSchemaProviderFactory.class.getName());

        staticConfigs.put(
                configAvroRelSchemaProviderDomain + FileSystemAvroRelSchemaProviderFactory.CFG_SCHEMA_DIR,
            "/tmp/schemas/");

        return staticConfigs;
    }
}
