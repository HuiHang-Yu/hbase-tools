package org.apache.hbase.tools.region.checker.Utils.RegionExceptionUtils;

public class RegionStateIllegalException extends  Exception {
    private String reason ;
    public RegionStateIllegalException(String msg){
        super(msg);
        this.reason = msg;
    }

    public String getReason() {
        return reason;
    }
}
