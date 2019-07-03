package org.apache.hbase.tools.meta.fixer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class RegionChecker {
    static Configuration configuration = null;
    static final String META = "hbase:meta";
    static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    static final String ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";
    static final String ZOOKEEPER_PORT_DEFAULT = "2181";
    static final String ZOOKEEPER_ZNODE = "zookeeper.znode.parent";
    static final String ZOOKEEPER_ZNODE_DEFAULT = "/hbase";
    static {
        configuration = HBaseConfiguration.create();

    }

    public static void main(String[] args) {
        if(args.length < 2)
        {
            System.out.println("useage : java -cp run.jar zk table  [zk_port] [hbase_znode] . \n defult : zk_port=2181 hbase_znode=/hbase ## if you need to modify this two arguments please change them. \n maybe later we will add offline and split state");
            System.exit(1);
        }
        configuration.set(ZOOKEEPER_PORT, args.length > 3 ? args[2] : ZOOKEEPER_PORT_DEFAULT);    //args[5]
        configuration.set(ZOOKEEPER_ZNODE, args.length > 4 ? args[3] :ZOOKEEPER_ZNODE_DEFAULT);     // args[6]
        configuration.set(ZOOKEEPER_QUORUM,args[0]);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Admin admin = connection.getAdmin();
            List<RegionInfo> regions = admin.getRegions(TableName.valueOf(args[1]));
            regions.stream().forEach( region -> {

            });
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
