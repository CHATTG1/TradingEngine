package com.github.randerzander;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;

import java.io.FileReader;
import java.util.Properties;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.SpoutDeclarer;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import com.github.randerzander.StormCommon.utils.DateTimeFileNameFormat;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.common.Column;
//TODO: Fix to ConnectionProvider in 2.3.1+
import org.apache.storm.jdbc.common.ConnectionPrvoider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;

import java.sql.Types;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;

import com.github.randerzander.StormCommon.Utils;
import com.github.randerzander.StormCommon.bolts.PyBolt;

public class TradingTopology {
    public static void main(String[] args) throws Exception {
      HashMap<String, String> props = Utils.getPropertiesMap(args[0]);
      TopologyBuilder builder = new TopologyBuilder();
      Config conf = new Config();
      conf.setNumWorkers(Integer.parseInt(props.get("numWorkers")));

      //Setup Kafka spout
      SpoutConfig spoutConfig = new SpoutConfig(
        new ZkHosts(props.get("zk.hosts")),
        props.get("raw.topic"), "/kafkastorm", props.get("raw.consumerGroupID")
      );
      spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
      builder.setSpout("quotes", new KafkaSpout(spoutConfig));

      //Setup Python parser
      String[] rawFields = new String[]{"name", "price", "symbol", "ts", "type", "volume"};
      builder.setBolt("parser", new PyBolt("parser.py", rawFields, -1)).shuffleGrouping("quotes");

      //Setup strategy executor
      String[] outFields = new String[]{"ts", "strategy", "symbol", "price", "volume"};
      builder.setBolt("strategy_executor", new PyBolt("stategy_executor.py", outFields, -1))
        .fieldsGrouping("parser", new Fields("symbol"));

      //Setup Phoenix output
      Map hikariConfigMap = Maps.newHashMap();
      String[] jdbcProps = new String[]{"dataSourceClassName", "dataSource.url", "dataSource.user", "dataSource.password"};
      for (String prop: jdbcProps) hikariConfigMap.put(prop, props.get(prop));
      ConnectionPrvoider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

      List<Column> columnSchema = Lists.newArrayList();
      columnSchema.add(new Column("name", java.sql.Types.VARCHAR));
      columnSchema.add(new Column("price", java.sql.Types.FLOAT));
      columnSchema.add(new Column("symbol", java.sql.Types.VARCHAR));
      columnSchema.add(new Column("ts", java.sql.Types.BIGINT));
      columnSchema.add(new Column("type", java.sql.Types.VARCHAR));
      columnSchema.add(new Column("volume", java.sql.Types.BIGINT));
      builder.setBolt("phoenix", new JdbcInsertBolt(connectionProvider, new SimpleJdbcMapper(columnSchema))
        .withInsertQuery("upsert into QUOTES values (?, ?, ?, ?, ?, ?)")
        .withQueryTimeoutSecs(-1)
      ).shuffleGrouping("parser");

      columnSchema.add(new Column("ts", java.sql.Types.BIGINT));
      columnSchema.add(new Column("strategy", java.sql.Types.VARCHAR));
      columnSchema.add(new Column("symbol", java.sql.Types.VARCHAR));
      columnSchema.add(new Column("price", java.sql.Types.FLOAT));
      columnSchema.add(new Column("volume", java.sql.Types.BIGINT));
      builder.setBolt("phoenix", new JdbcInsertBolt(connectionProvider, new SimpleJdbcMapper(columnSchema))
        .withInsertQuery("upsert into ORDERS values (?, ?, ?, ?, ?)")
        .withQueryTimeoutSecs(-1)
      ).shuffleGrouping("strategy_executor");
        
      Utils.run(builder, props.get("topologyName"), conf, props.get("killIfRunning").equals("true"), props.get("localMode").equals("true"));
    }

}
