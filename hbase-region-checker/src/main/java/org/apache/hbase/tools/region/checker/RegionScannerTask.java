package org.apache.hbase.tools.region.checker;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hbase.tools.region.checker.Utils.RegionUtils;

import java.util.concurrent.Callable;

public class RegionScannerTask implements Callable<Boolean> {
    private RegionInfo region;
    private Table table;
    public RegionScannerTask(Table table,RegionInfo region){
        this.region = region;
        this.table = table;
    }
    @Override
    public Boolean call() {
        boolean flag = false;
        try {
            if (this.table != null) {
                RegionUtils.scanRegion(table, region);
            }
            flag = true;
        }catch (Exception e){
            flag = false;
        }finally {
            return flag;
        }
    }
}
