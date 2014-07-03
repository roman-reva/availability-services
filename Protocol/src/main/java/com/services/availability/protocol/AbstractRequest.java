package com.services.availability.protocol;

import java.io.Serializable;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-23 17:21
 */
public abstract class AbstractRequest implements Serializable {
    private final byte requestType;

    protected AbstractRequest(RequestType requestType) {
        this.requestType = requestType.code;
    }

    public byte getRequestType() {
        return requestType;
    }

    protected enum RequestType {
        SYSTEM_REQUEST(1),
        USER_REQUEST(2);

        private byte code;

        RequestType(int code) {
            this.code = (byte)code;
        }
    }
}
