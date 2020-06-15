package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.ftplet.FtpFile;

public class FtpNegativeCompletionException extends Exception {

    private final int ftpReturnCode;
    private final String subId;
    private final String basicMessage;
    private final FtpFile ftpFile;

    public FtpNegativeCompletionException(int ftpReturnCode, String subId, String basicMessage, FtpFile ftpFile) {
        super(subId);
        this.ftpReturnCode = ftpReturnCode;
        this.subId = subId;
        this.basicMessage = basicMessage;
        this.ftpFile = ftpFile;
    }

    public int getFtpReturnCode() {
        return ftpReturnCode;
    }

    public String getSubId() {
        return subId;
    }

    public String getBasicMessage() {
        return basicMessage;
    }

    public FtpFile getFtpFile() {
        return ftpFile;
    }
}
