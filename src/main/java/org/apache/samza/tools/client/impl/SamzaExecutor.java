package org.apache.samza.tools.client.impl;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.fn.FlattenUdf;
import org.apache.samza.sql.fn.RegexMatchUdf;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplication;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.SqlFileParser;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.tools.avro.AvroSchemaGenRelConverterFactory;
import org.apache.samza.tools.avro.AvroSerDeFactory;
import org.apache.samza.tools.client.interfaces.*;
import org.apache.samza.tools.client.util.RandomAccessQueue;
import org.apache.samza.tools.json.JsonRelConverterFactory;
import org.apache.samza.tools.schemas.PageViewEvent;
import org.apache.samza.tools.schemas.ProfileChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Joiner;

import static org.apache.samza.tools.client.util.CliUtil.*;


public class SamzaExecutor implements SqlExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutor.class);
    private static final String SAMZA_SYSTEM_KAFKA = "kafka";
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
        List<String> tableNames = new ArrayList<String>();
        tableNames.add("kafka.ProfileChangeStream");

        return tableNames;
    }

    @Override
    public TableSchema getTableScema(ExecutionContext context, String tableName) {
        return TableSchemaBuilder.builder().appendColumn("Key", "String")
                .appendColumn("Name", "String")
                .appendColumn("NewCompany", "String")
                .appendColumn("OldCompany", "String")
                .appendColumn("ProfileChangeTimestamp", "Date")
                .toTableSchema();
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
    public NonQueryResult executeNonQuery(ExecutionContext context,  URI sqlFile) {
        return executeNonQuery(context, SqlFileParser.parseSqlFile(sqlFile.getPath()));
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
        throw new ExecutionException("not supported");
    }

    static RandomAccessQueue<OutgoingMessageEnvelope> getM_outputData() {
        return m_outputData;
    }

    /**
     *TODO: API for users to pass in data schema
     */


    // -- private functions ------------------------------------------

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

    private TableSchema generateResultSchema(Config config) {
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
            colTypeNames.add(dataTypeField.getType().toString());
        }
        return new TableSchema(colNames, colTypeNames);
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
                Joiner.on(",").join(RegexMatchUdf.class.getName(), FlattenUdf.class.getName()));

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
                ConfigBasedAvroRelSchemaProviderFactory.class.getName());

        staticConfigs.put(
                configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
                        "kafka", "PageViewStream"), PageViewEvent.SCHEMA$.toString());

        staticConfigs.put(
                configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
                        "kafka", "ProfileChangeStream"), ProfileChangeEvent.SCHEMA$.toString());

        staticConfigs.put(
            configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
                "kafka", "ProfileChangeStream1"), ProfileChangeEvent.SCHEMA$.toString());

        staticConfigs.put(
            configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
                "kafka", "ProfileChangeStream_sink"), ProfileChangeEvent.SCHEMA$.toString());

        return staticConfigs;
    }
}
