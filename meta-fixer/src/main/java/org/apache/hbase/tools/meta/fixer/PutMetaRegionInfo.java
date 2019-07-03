package org.apache.hbase.tools.meta.fixer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutMetaRegionInfo {
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

        if(args.length < 5)
        {
            System.out.println("useage : java -cp run.jar zk table startkey endkey regionid  [zk_port] [hbase_znode] . \n defult : zk_port=2181 hbase_znode=/hbase ## if you need to modify this two arguments please change them. \n maybe later we will add offline and split state");
            System.exit(1);
        }
        configuration.set(ZOOKEEPER_PORT, args.length > 5 ? args[5] : ZOOKEEPER_PORT_DEFAULT);    //args[5]
        configuration.set(ZOOKEEPER_ZNODE, args.length > 6 ? args[6] :ZOOKEEPER_ZNODE_DEFAULT);     // args[6]
        configuration.set(ZOOKEEPER_QUORUM,args[0]);
        byte [] startKey = Bytes.toBytesBinary(args[2]);
        byte [] endKey = Bytes.toBytesBinary(args[3]);
        boolean split = false;
        long ts = System.currentTimeMillis();
        RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(args[1]))
                .setStartKey(startKey)
                .setEndKey(endKey)
                .setSplit(split)
                .setRegionId(Long.valueOf(args[4]))
                .setOffline(false)
                .build();
        Put put = new Put(hri.getRegionName(), ts);
        try {
            put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                    .setRow(hri.getRegionName())
                    .setFamily(HConstants.CATALOG_FAMILY)
                    .setQualifier(HConstants.REGIONINFO_QUALIFIER)
                    .setTimestamp(put.getTimestamp())
                    .setType(Cell.Type.Put)
                    .setValue(RegionInfo.toByteArray(hri))
                    .build());

        } catch (IOException e) {
            e.printStackTrace();
        }
        Connection connection = null;
        Table table = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(META));
            table.put(put);
            System.out.println("used  time"+(System.currentTimeMillis() - ts )+"ms.");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(table != null)
                    table.close();
                if(connection !=null )
                    connection.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
