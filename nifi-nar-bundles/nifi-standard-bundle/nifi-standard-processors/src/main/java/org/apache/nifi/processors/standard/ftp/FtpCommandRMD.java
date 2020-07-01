package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FtpCommandRMD extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(FtpCommandRMD.class);

    @Override
    public void execute(FtpIoSession session, FtpServerContext context, FtpRequest request) throws IOException {
        // reset state variables
        session.resetState();

        session.write(new DefaultFtpReply(FtpReply.REPLY_502_COMMAND_NOT_IMPLEMENTED, "Operation (RMD) not supported."));
    }
}
