package org.apache.hbase.tools.region.checker.Utils;

import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.concurrent.Future;

public class RegionFutureWapper<T> {
    private Future<T> future;
    private RegionInfo region;
    public RegionFutureWapper(Future<T> future,RegionInfo region){
        this.future = future;
        this.region = region;
    }
    public Future<T> getFuture(){
        return this.future;
    }
    public RegionInfo getRegion(){
        return this.region;
    }

}
