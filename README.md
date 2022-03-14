valor
=========

Valor is a data integration tool based on binary schema management, which provides unified access to various kinds of data storages.



Modules
---------
* valor-sdk standard developer kit
* valor-spi service provider interface
* valor-presto Presto plugin
* valor-hive Hive Storage Handler
* valor-cli a command line interface
* valor-fs file system adapter
* valor-hbase HBase adapter
* valor-zookeepr ZooKeeper adapter
* valor-phalanx Phalanx adapter
* valor-zero Zero adapter

Getting Started
-----------

Build
````
   mvn package -DskipTests
   cp -r valor-cli/target/valor-cli/valor-cli <INSTALL DIR>/
````

Usage

````
    # hbase shell
    > create 'valor_schema', {NAME=>'s'}
    > create 'testtable', {NAME=>'t'}
    > exit
    # cd <INSTALL_DIR>/valor-cli
    # cat conf/config.json
    {
      "valor.schemarepository.class": "HBaseSchemaRepository",
      "valor.plugins.dir": "./plugins"
    }
    # cat example/relation.json
    {
      "attributes": [
        {"name": "k", "isKey": true, "type": "string"},
        {"name": "v", "isKey": false,"type": "int"}
      ]
    }
    # ./bin/valor createRelation -r sample -f example/relation.json
    # cat example/schema.json
    {
      "isPrimary": true,
      "storage": {
        "class": "HBaseStorage",
        "conf": {"hbase.zookeeper.quorum": "localhost"}
      },
      "fields": [
        {
          "name": "table",
          "format": [
            {"type": "const", "props": {"value": "testtable"}}
          ]
        },
        {
          "name": "rowkey",
          "format": [
            {"type": "attr", "props": {"attr": "k"}}
          ]
        },
        {
          "name": "family",
          "format": [
            {"type": "const", "props": {"value": "t"}}
          ]
        },
        {
          "name": "qualifier",
          "format": [
            {"type": "const", "props": {"value": "q"}}
          ]
        },
        {
          "name": "value",
          "format": [
            {"type": "attr", "props": {"attr": "v"}}
          ]
        }
      ]
    }
    # ./bin/valor createSchema -r sample -s s -f example/schema.json
    # ./bin/valor show
    sample
      s
    # ./bin/valor ql -e "insert into sample values ('k1',100)"
    # hbase shell
    hbase(main):001:0> scan 'testtable'
    ROW                                 COLUMN+CELL
     k1                                 column=t:q, timestamp=1639647830031, value=\x00\x00\x00d
    1 row(s) in 0.4270 seconds
    hbase(main):002:0> put 'testtable', 'k2', 't:q', "\x00\x00\x00\xC8"
    0 row(s) in 0.0750 seconds
    hbase(main):003:0> scan 'testtable'
    ROW                                 COLUMN+CELL
     k1                                 column=t:q, timestamp=1639647830031, value=\x00\x00\x00d
     k2                                 column=t:q, timestamp=1639717267609, value=\x00\x00\x00\xC8
    2 row(s) in 0.0210 seconds
    hbase(main):00:0> exit
    # ./bin/valor ql -e "select * from sample where k = 'k2'"
    k2      200
    {"k":"k1", "v":100}
    {"k":"k2", "v":200}
````

