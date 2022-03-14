DROP TABLE IF EXISTS valor_part_test;
CREATE EXTERNAL TABLE valor_part_test(
  k string,
  v int)
ROW FORMAT SERDE
  'ValorSerDe'
STORED BY
  'ValorStorageHandler'
WITH SERDEPROPERTIES (
  'valor.hive.relation'='sample',
  'valor.schemarepository.class'='zookeeper'
);
