package org.apache.samza.tools.client.impl;


import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplication;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.tools.avro.AvroSchemaGenRelConverterFactory;
import org.apache.samza.tools.avro.AvroSerDeFactory;
import org.apache.samza.tools.client.interfaces.*;
import org.apache.samza.tools.json.JsonRelConverterFactory;
import org.apache.samza.tools.schemas.PageViewEvent;
import org.apache.samza.tools.schemas.ProfileChangeEvent;

import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SamzaExecutor implements SqlExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutor.class);
    private static final String SAMZA_SYSTEM_KAFKA = "kafka";
    private static final String SAMZA_SYSTEM_LOG = "log";

    private static class SamzaExecution {
        SamzaSqlApplicationRunner runner;
        SamzaSqlApplication app;
        ExecutionStatus status;

        SamzaExecution(SamzaSqlApplicationRunner runner, SamzaSqlApplication app) {
            this.runner = runner;
            this.app = app;
            this.status = status;
        }

        ExecutionStatus getExecutionStatus() {
            // return new ExecutionStatus(runner.status(app).getStatusCode());
            return ExecutionStatus.New;
        }
    }
    private static AtomicInteger m_execIdSeq = new AtomicInteger(0);
    private HashMap<Integer, SamzaExecution> m_executors = new HashMap<>();


    // -- implementation of SqlExecutor ------------------------------------------

    @Override
    public void start(ExecutionContext context) {

    }

    @Override
    public void stop(ExecutionContext context) {
        Iterator it = m_executors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            stopExecution(Integer.valueOf((Integer) pair.getKey()));
        }
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
        return executeQuery(statement);
    }

    @Override
    public int getRowCount() {
        throw new ExecutionException("not supported");
    }

    @Override
    public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) {
        throw new ExecutionException("not supported");
    }

    @Override
    public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement) {
        int execId = executeSql(statement);
        return new NonQueryResult(execId, true);
    }

    @Override
    public boolean stopExecution(ExecutionContext context, int exeId) {
        return stopExecution(exeId);
    }

    @Override
    public boolean removeExecution(ExecutionContext context, int exeId) {
        throw new ExecutionException("not supported");
    }

    @Override
    public ExecutionStatus queryExecutionStatus(int execId) {
        if (!m_executors.containsKey(execId)) {
            return null;
        }
        return m_executors.get(execId).status;
    }

    @Override
    public String getErrorMsg() {
        throw new ExecutionException("not supported");
    }


    public boolean stopExecution(int exeId) {
        SamzaExecution exec = m_executors.get(exeId);
        if(exec != null) {
            exec.runner.kill(exec.app);
            m_executors.remove(exeId);
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

    public QueryResult executeQuery(String statement) {
        int execId = executeSql(Collections.singletonList(statement));

        TableSchema schema = TableSchemaBuilder.builder().
                appendColumn("All", "String").toTableSchema();

        return new QueryResult(1, schema);
    }

    public NonQueryResult executeNonQuery(List<String> statement) {
        return null;
    }


    

    // ------------------------------------------------------------------------

//    private int executeSqlFile(String sqlFile) {
//        List<String> sqlStmts = SqlFileParser.parseSqlFile(sqlFile);
//        return executeSql(sqlStmts);
//    }



    public int executeSql(List<String> sqlStmts) {

        sqlStmts = sqlStmts.stream().map(sql -> {
            if (!sql.toLowerCase().startsWith("insert")) {
                String formattedSql = String.format("insert into log.outputStream %s", sql);
                LOG.debug("Sql formatted. ", sql, formattedSql);
                return formattedSql;
            } else {
                return sql;
            }
        }).collect(Collectors.toList());

        int execId = m_execIdSeq.incrementAndGet();
        Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
        staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

        SamzaSqlApplicationConfig sqlConfig = new SamzaSqlApplicationConfig(new MapConfig(staticConfigs));
        Map<String, RelSchemaProvider> relSchemaProviders = sqlConfig.getRelSchemaProviders();

        SamzaSqlApplicationRunner runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
        SamzaSqlApplication app = new SamzaSqlApplication();
        runner.run(app);

        m_executors.put(execId, new SamzaExecution(runner, app));
        LOG.debug("Executing sql. Id ", execId);
        return execId;
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
