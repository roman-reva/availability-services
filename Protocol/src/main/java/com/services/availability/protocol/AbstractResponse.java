package com.services.availability.protocol;

import java.io.Serializable;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-23 17:30
 */
public abstract class AbstractResponse implements Serializable {
    private boolean error = false;
    private byte errorMsg = 0;

    protected AbstractResponse() {}

    protected AbstractResponse(boolean error, ResponseErrorCodes errorCode) {
        this.error = error;
        this.errorMsg = errorCode.code;
    }

    public boolean isSuccess() {
        return !error;
    }

    public boolean isError() {
        return error;
    }

    public static enum ResponseErrorCodes {
        IOEXCEPTION(1),
        ITEM_NOT_FOUND(2);
        ResponseErrorCodes(int code) {
            this.code = (byte)code;
        }
        private final byte code;
    }

}
