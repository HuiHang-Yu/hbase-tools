package org.apache.hbase.tools.region.checker;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.tools.region.checker.Utils.RegionUtils;
import org.apache.hbase.tools.region.checker.Utils.RegionFutureWapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/*
    this class is for check region overlap !
    no  hbase hbck can check overlap so we don't need to do this
    we only check weather the region state open is really open !
 */
public class RegionChecker {


    public static void main(String[] args) {
        if(args.length < 1)
        {
            System.out.println("useage : java -cp lib/*.jar:example.jar zk [zk_port] [hbase_znode] . \n defult : zk_port=2181 hbase_znode=/hbase ## if you need to modify this two arguments please change them. ");
            System.exit(1);
        }
        Connection connection = null;
        Admin admin = null;
        Table currentHTable = null;
        RegionInfo currentRegionInfo = null;
        List<RegionMetrics> currentMetrics = null;
        Map<String,ServerName> map = new HashMap<>();
        int maxCores = 16;
        try {
            RegionCheckerHandler handler = RegionCheckerHandler.getInstance().init(args);
            connection = ConnectionFactory.createConnection(RegionCheckerHandler.configuration);
            admin = connection.getAdmin();
            handler.collectRS(admin,map); // not singleton is ok here
            ExecutorService pool = Executors.newFixedThreadPool(maxCores);
            List<RegionFutureWapper<Boolean>> result = new ArrayList<>();
            for (TableName htable : admin.listTableNames()){
                currentHTable = connection.getTable(htable);
                List<Pair<RegionInfo, ServerName>> regions = MetaTableAccessor.getTableRegionsAndLocations(connection,htable ,true);
                for(Pair<RegionInfo, ServerName> region : regions) {
                    // what should we do if the region state is CLOSED! // scan limit too , for make sure the service is ok always !
                    //here we submit a task to run
                    handler.checkRegion(region, map , admin , currentHTable,pool,result);
                }
                if(currentHTable!=null){
                    currentHTable.close();
                }
            }
            pool.shutdown();
            result.stream().forEach(future -> {
                try {
                    if(!future.getFuture().get(60, TimeUnit.SECONDS)){
                        //  if throw exception which will be return false or true or nothing!!
                        System.out.println(String.format(RegionUtils.regionStateIllegalMsg,future.getRegion().getRegionNameAsString() ) );
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("finished");
    }
}
