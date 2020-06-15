package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;

import java.io.IOException;

public class FtpCommandDELE extends AbstractCommand {
    @Override
    public void execute(FtpIoSession session, FtpServerContext context, FtpRequest request) throws IOException {
        // reset state variables
        session.resetState();

        session.write(new DefaultFtpReply(FtpReply.REPLY_502_COMMAND_NOT_IMPLEMENTED, "Deletion of file system entries is not supported."));
    }
}
