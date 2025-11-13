curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  -d '{
    "name": "cassandra-jdbc-sink",
    "config": {
      "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
      "tasks.max": "1",
      "topics": "localhost.public.ticker_24h,localhost.public.klines,localhost.public.latest_prices",
      "connection.url": "jdbc:cassandra://cassandra:9042/crypto_keyspace",
      "connection.username": "cassandra",
      "connection.password": "cassandra",
      "auto.create": "false",
      "insert.mode": "upsert",
      "primary.key.mode": "record_key",
      "primary.key.fields": "symbol,timestamp,open_time",
      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter.schemas.enable": "false",
      "transforms": "unwrap,rename",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.rename.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
      "transforms.rename.renames": "open_time:open_time,timestamp:timestamp,symbol:symbol,price_change:price_change,price_change_percent:price_change_percent,volume:volume,high_price:high_price,low_price:low_price,open_price:open_price,close_price:close_price,close_time:close_time,price:price"
    }
  }'