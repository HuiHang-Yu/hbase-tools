## hbase-tools-region-checker-2.1.1

### describe
    hbase-tools-region-checker-2.1.1:RegionChecker class is for checking weather region state is ok ! 

### usage useage :

     java -cp run.jar zk  [zk_port] [hbase_znode] . 
        defult : zk_port=2181 hbase_znode=/hbase
        if you need to modify this two arguments please change them.
   
 ### eg:
    java -cp ./lib/*:./hbase-tools-region-checker-2.1.1.jar  .RegionChecker  tempt22 


