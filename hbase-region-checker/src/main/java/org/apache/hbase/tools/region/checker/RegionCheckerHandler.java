package org.apache.hbase.tools.region.checker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils.RegionCloseException;
import org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils.RegionStateSuspectException;
import org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils.ServerAddressNotExistsException;
import org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils.ServerNameNotMatchException;
import org.apache.hbase.tools.region.checker.Utils.RegionFutureWapper;
import org.apache.hbase.tools.region.checker.Utils.RegionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class RegionCheckerHandler {

    public static Configuration configuration;
    static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    static final String ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";
    static final String ZOOKEEPER_PORT_DEFAULT = "2181";
    static final String ZOOKEEPER_ZNODE = "zookeeper.znode.parent";
    static final String ZOOKEEPER_ZNODE_DEFAULT = "/hbase";
    static {
        configuration = HBaseConfiguration.create();
    }
    private RegionCheckerHandler(){

    }
    private static class handlerBuilder{
        private final static RegionCheckerHandler HANDLER = new RegionCheckerHandler();
    }
    public static RegionCheckerHandler getInstance(){
        return handlerBuilder.HANDLER;
    }
    public RegionCheckerHandler init(String [] args) throws IOException {
        configuration.set(ZOOKEEPER_PORT, args.length > 1 ? args[1] : ZOOKEEPER_PORT_DEFAULT);    //args[5]
        configuration.set(ZOOKEEPER_ZNODE, args.length > 2 ? args[2] :ZOOKEEPER_ZNODE_DEFAULT);     // args[6]
        configuration.set(ZOOKEEPER_QUORUM,args[0]);
        return this;
    }
    public  RegionCheckerHandler collectRS(Admin admin, Map<String, ServerName> map) throws IOException {
        for(ServerName server: admin.getRegionServers()){
            if(map.get(server.getAddress().toString()) == null){
                map.put(server.getAddress().toString(),server);
            }else{
                System.out.println(String.format("server %s has existed ! before start code : %d and now startcode : %d",server.getAddress().toString(),server.getStartcode(),((ServerName)map.get(server.getAddress().toString())).getStartcode()));
            }
        }
        return this;
    }
    public void checkRegion(Pair<RegionInfo, ServerName> region, Map<String,ServerName> map, Admin admin , Table currentHTable, ExecutorService executors,List<RegionFutureWapper<Boolean>> list){
        RegionInfo currentRegionInfo = region.getFirst();
        Future<Boolean> future = null;
        try {
            RegionUtils.checkRegionServer(region.getSecond(),map); // check serverName startcode
            RegionUtils.checkRegion(region, admin.getRegionMetrics(region.getSecond(), currentRegionInfo.getTable()), map);
        } catch (RegionStateSuspectException e) {
            if(null != currentHTable  && null != currentRegionInfo ){
                future = executors.submit(new RegionScannerTask(currentHTable,currentRegionInfo));
                list.add(new RegionFutureWapper(future,region.getFirst()));
            }
        } catch (RegionCloseException e) {
            System.out.println(String.format(e.getReason(),currentRegionInfo.getRegionNameAsString()));
        } catch (ServerAddressNotExistsException e) {
            System.out.println(String.format(e.getReason(),currentRegionInfo.getRegionNameAsString()));
        } catch (ServerNameNotMatchException e) {
            System.out.println(String.format(e.getReason(),currentRegionInfo.getRegionNameAsString()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
