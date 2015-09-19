**IN PROGRESS**: not yet complete

Build the topology
```
cd TradingEngine
mvn clean package
```

Create the Phoenix/HBase tables
```
/usr/hdp/current/phoenix-client/bin/psql.py zk_server:2181:/hbase-unsecure ddl/quotes.ddl
/usr/hdp/current/phoenix-client/bin/psql.py zk_server:2181:/hbase-unsecure ddl/orders.ddl
```

Run topology
```
storm jar target/TradingEngine-SNAPSHOT.jar com.github.randerzander.TradingTopology topology.props
```
