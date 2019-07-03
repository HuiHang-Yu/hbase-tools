## hbase-tools

### describe
    hbase-tools-2.1.1:PutMetaRegionInfo class is maked for hbase tools for fix meta regioninfo

### usage useage :

     java -cp run.jar zk table startkey  endkey regionid  [zk_port] [hbase_znode] . 
        defult : zk_port=2181 hbase_znode=/hbase
        if you need to modify this two arguments please change them.
    
    maybe later we will add offline and split state .
 
 ### eg:
    
    java -cp ./lib/*:./meta-fixer-2.1.1.jar  org.apache.hbase.tools.meta.fixer.PutMetaRegionInfo  tempt22 YCSADDATA1 "123"  "000\x00\x00\x00\x00\x00\x00\x00\x00\x00" 1551883650730 2181 /hbase


