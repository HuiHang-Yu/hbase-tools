package org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils;

public class RegionCloseException extends Exception {
    private String reason;
    public RegionCloseException(String msg){
        super(msg);
        this.reason = msg;
    }
    public String getReason(){
        return this.reason;
    }
}
