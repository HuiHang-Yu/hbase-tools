package org.apache.hbase.tools.region.checker.Utils;

import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RegionUtils {
    private final static String reigonCloseMsg = "ERROR : region %s is closed !!! ";
    private final static String regionServerNameNotMatchMsg = "ERROR : region %s server name is not match , expected %s but %s !!!";
    private final static String regionServerAddressNotExistsMsg = "ERROR : region %s server address is not exists !!! ";
    private final static String regionStateSuspectMsg = "WARNNING : suspect region %s state is illegal ";
    public final static String regionStateIllegalMsg = "ERROR : region %s state is illegal ";
    public static void  checkRegionServerName(ServerName serverA, ServerName serverB) throws RegionCloseException, ServerNameNotMatchException, ServerAddressNotExistsException {
        if( null ==  serverB) throw new ServerAddressNotExistsException(regionServerAddressNotExistsMsg);
        if(serverA.compareTo(serverB) != 0){
            if(serverA.getHostname() == null || serverB.getHostname() == null){
                throw new RegionCloseException(reigonCloseMsg);
            }else{
                throw new ServerNameNotMatchException(String.format(regionServerNameNotMatchMsg,serverA.getServerName(),serverB.getServerName()));
            }
        }
    }
    public static void checkRegionServer(ServerName serverName, Map<String,ServerName> map) throws ServerNameNotMatchException, RegionCloseException, ServerAddressNotExistsException {
        if(serverName == null ) throw new RegionCloseException(reigonCloseMsg);
        String address = serverName.getHostAndPort();
        checkRegionServerName(serverName, map.get(address));
    }
    public static void fillCollection(Collection<ServerName> source, Map<String,ServerName> map){
        source.stream().forEach(region -> map.put(region.getAddress().toString(),region));
    }
    public static void checkRegion(Pair<RegionInfo,ServerName> regionPair , List<RegionMetrics> metrics, Map<String,ServerName> map) throws RegionStateSuspectException, RegionCloseException {
        if(metrics.size() == 0){     // if the metric size == 0 so it may be closed or not open normally
            throw new RegionCloseException(reigonCloseMsg);
        }
        for(RegionMetrics metric : metrics){
            if(metric.getNameAsString().equals(regionPair.getFirst().getRegionNameAsString())){
                    checkRegionMetric(metric); //check  region state from metric infomation
            }
        }

    }
    public static void checkRegionMetric(RegionMetrics metric) throws RegionStateSuspectException {

        double storeSize = metric.getStoreFileSize().get();
        double memSize = metric.getMemStoreSize().get();
        int fileCount = metric.getStoreFileCount();
        long writeRequestCount = metric.getWriteRequestCount();
        long readRequestCount = metric.getReadRequestCount();
        if(storeSize == 0.0 && memSize == 0.0 && fileCount == 0 && writeRequestCount == 0 && readRequestCount == 0){
            // find suspect region state is illegal
            throw new RegionStateSuspectException(regionStateSuspectMsg);
        }
    }
    public static void scanRegion(Table htable, RegionInfo regionInfo) throws RegionStateIllegalException, IOException {
        Scan scan = new Scan();
        scan.setLimit(1);
        scan.setStartRow(regionInfo.getStartKey());
        scan.setStopRow(regionInfo.getEndKey());
        ResultScanner rscanner = htable.getScanner(scan);
        Iterator<Result> it = rscanner.iterator();
        while (it.hasNext()) {
            it.next();
        }
    }

}
