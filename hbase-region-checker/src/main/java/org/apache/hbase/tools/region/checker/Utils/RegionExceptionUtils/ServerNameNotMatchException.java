package org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils;

public class ServerNameNotMatchException extends Exception {
    private String reason;
    public ServerNameNotMatchException(String msg){
        super(msg);
        this.reason = msg;
    }
    public String getReason(){
        return this.reason;
    }
}
