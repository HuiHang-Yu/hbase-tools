package org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils;

public class RegionStateSuspectException extends Exception {
    private String reason;
    public RegionStateSuspectException(String msg){
        super(msg);
        this.reason = msg;
    }
    public String getReason(){
        return this.reason;
    }
}
