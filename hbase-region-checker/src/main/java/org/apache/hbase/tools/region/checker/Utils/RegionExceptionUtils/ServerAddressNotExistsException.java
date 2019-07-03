package org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils;

public class ServerAddressNotExistsException extends Exception {
    private String reason;
    public ServerAddressNotExistsException(String msg){
        super(msg);
        this.reason = msg;
    }
    public String getReason(){
        return this.reason;
    }
}
