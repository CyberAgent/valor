Valor Hive Storage Handler
=========

Gettign started
----
1. configure hive and deploy aux jar
```
$cat /etc/hive/conf/hive-site.xml
<configuration>
 ...
  <property>
    <name>hive.reloadable.aux.jars.path</name>
    <value>file:///opt/hive/aux/valor-hive.jar</value>
  </property>
</configuration>
$ cp valor-hive/target/valor-hive.jar /opt/hive/aux/
```

2. [create valor schema](https://github.com/ca-akb-lab/valor/blob/develop/README.md#getting-started)

```
$ ./bin/valor createRelation -f example/relation.json
$ ./bin/valor createSchema -f example/schema.json
```

3. create hive table
```
$ cat hive.ddl
CREATE EXTERNAL TABLE test_hive_integ (
  k string,
  v int)
ROW FORMAT SERDE
  'ValorSerDe'
STORED BY
  'ValorStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.zookeeper.quorum'='localhost:2181',
  'valor.hive.relation'='sample',
  'valor.schemarepository.class'='hbase')
$ hive -f hive.ddl
```
The columns of the hive table must match with the attributes of the relation (sample).
The table wraps the relation specified by 'valor.hive.relation'.
The other SERDEPROPERTIES should be configured as same as the valor config (config.json).



