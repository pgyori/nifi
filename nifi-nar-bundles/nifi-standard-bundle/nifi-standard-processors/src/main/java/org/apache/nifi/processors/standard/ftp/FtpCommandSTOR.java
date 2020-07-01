/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DataConnection;
import org.apache.ftpserver.ftplet.DataConnectionFactory;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.IODataConnectionFactory;
import org.apache.ftpserver.impl.LocalizedDataTransferFtpReply;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.ftpserver.impl.ServerFtpStatistics;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.standard.ListenFTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.atomic.AtomicReference;

public class FtpCommandSTOR extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(FtpCommandSTOR.class);
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference;

    public FtpCommandSTOR(final AtomicReference<ProcessSessionFactory> sessionFactoryReference) {
        this.sessionFactoryReference = sessionFactoryReference;
    }

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession ftpSession, final FtpServerContext context, final FtpRequest request) {
        try {
            executeCommand(ftpSession, context, request);
        } catch (FtpNegativeCompletionException ftpNegativeCompletionException) {
            if (ftpNegativeCompletionException.getSubId() == null) {
                ftpSession.write(new DefaultFtpReply(ftpNegativeCompletionException.getFtpReturnCode(), ftpNegativeCompletionException.getBasicMessage()));
            } else {
                ftpSession.write(LocalizedDataTransferFtpReply.translate(ftpSession, request, context,
                        ftpNegativeCompletionException.getFtpReturnCode(),
                        ftpNegativeCompletionException.getSubId(),
                        ftpNegativeCompletionException.getBasicMessage(),
                        ftpNegativeCompletionException.getFtpFile()));
            }
        } finally {
            ftpSession.resetState();
            ftpSession.getDataConnection().closeDataConnection();
        }
    }

    private void executeCommand(FtpIoSession ftpSession, FtpServerContext context, FtpRequest request)
            throws FtpNegativeCompletionException {

        final String fileName = getArgument(request); //TODO: request argument might contain target path as well (already handled in VirtualFtpFile.path.resolve() but double check!)

        checkDataConnection(ftpSession);

        final FtpFile ftpFile = getFtpFile(ftpSession, fileName);

        checkWritePermission(ftpFile);

        sendReturnCode150(ftpSession, context, request, ftpFile.getAbsolutePath());

        final DataConnection dataConnection = openDataConnection(ftpSession, ftpFile);

        transferData(dataConnection, ftpSession, context, request, ftpFile);
    }

    private String getArgument(final FtpRequest request) throws FtpNegativeCompletionException {
        final String argument = request.getArgument();
        if (argument == null) {
            throw new FtpNegativeCompletionException(FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS, "STOR", null, null);
        }
        return argument;
    }

    private void checkDataConnection(final FtpIoSession ftpSession) throws FtpNegativeCompletionException {
        DataConnectionFactory dataConnectionFactory = ftpSession.getDataConnection();
        if (dataConnectionFactory instanceof IODataConnectionFactory) {
            InetAddress address = ((IODataConnectionFactory) dataConnectionFactory)
                    .getInetAddress();
            if (address == null) {
                throw new FtpNegativeCompletionException(FtpReply.REPLY_503_BAD_SEQUENCE_OF_COMMANDS, null, "PORT or PASV must be issued first", null);
            }
        }
    }

    private FtpFile getFtpFile(final FtpIoSession ftpSession, final String fileName) throws FtpNegativeCompletionException {
        FtpFile ftpFile = null;
        try {
            ftpFile = ftpSession.getFileSystemView().getFile(fileName);
        } catch (FtpException e) {
            LOG.debug("Exception getting file object", e);
        }
        if (ftpFile == null) {
            throw new FtpNegativeCompletionException(FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "STOR.invalid", fileName, ftpFile);
        }
        return ftpFile;
    }

    private void checkWritePermission(final FtpFile ftpFile) throws FtpNegativeCompletionException {
        if (!ftpFile.isWritable()) {
            throw new FtpNegativeCompletionException(FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "STOR.permission", ftpFile.getAbsolutePath(), ftpFile);
        }
    }

    private void sendReturnCode150(final FtpIoSession ftpSession, final FtpServerContext context, final FtpRequest request, final String fileAbsolutePath) {
        ftpSession.write(LocalizedFtpReply.translate(ftpSession, request, context,
                FtpReply.REPLY_150_FILE_STATUS_OKAY,
                "STOR",
                fileAbsolutePath)).awaitUninterruptibly(10000);
    }

    private DataConnection openDataConnection(final FtpIoSession ftpSession, final FtpFile ftpFile) throws FtpNegativeCompletionException {
        final DataConnection dataConnection;
        try {
            dataConnection = ftpSession.getDataConnection().openConnection();
        } catch (Exception exception) {
            LOG.debug("Exception getting the input data stream", exception);
            throw new FtpNegativeCompletionException(FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        }
        return dataConnection;
    }

    private void transferData(final DataConnection dataConnection, final FtpIoSession ftpSession,
                              final FtpServerContext context, final FtpRequest request, final FtpFile ftpFile)
            throws FtpNegativeCompletionException {

        final ProcessSession processSession = createProcessSession();
        FlowFile flowFile = processSession.create();
        long transferredBytes = 0L;
        try {
            try (OutputStream flowFileOutputStream = processSession.write(flowFile)) {
                transferredBytes = dataConnection.transferFromClient(ftpSession.getFtpletSession(), flowFileOutputStream);
            }

            LOG.info("File received {}", ftpFile.getAbsolutePath());

            // notify the statistics component
            ServerFtpStatistics ftpStat = (ServerFtpStatistics) context.getFtpStatistics();
            ftpStat.setUpload(ftpSession, ftpFile, transferredBytes);

            //TODO: add flowfile attributes
            processSession.putAttribute(flowFile, CoreAttributes.FILENAME.key(), ftpFile.getName());
            processSession.putAttribute(flowFile, CoreAttributes.PATH.key(), getPath(ftpFile));
            //TODO: provenance event
            processSession.transfer(flowFile, ListenFTP.RELATIONSHIP_SUCCESS);
            processSession.commit();

        } catch (SocketException socketException) {
            LOG.debug("Socket exception during data transfer", socketException);
            processSession.rollback();
            throw new FtpNegativeCompletionException(FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        } catch (IOException ioException) {
            LOG.debug("IOException during data transfer", ioException);
            processSession.rollback();
            throw new FtpNegativeCompletionException(FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
                    "STOR",
                    ftpFile.getAbsolutePath(),
                    ftpFile);
        }

        // if data transfer ok - send transfer complete message
        ftpSession.write(LocalizedDataTransferFtpReply.translate(ftpSession, request, context,
                FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "STOR",
                ftpFile.getAbsolutePath(), ftpFile, transferredBytes));
    }

    private String getPath(FtpFile ftpFile) {
        String absolutePath = ftpFile.getAbsolutePath();
        int endIndex = absolutePath.length() - ftpFile.getName().length();
        return ftpFile.getAbsolutePath().substring(0, endIndex);
    }

    private ProcessSession createProcessSession() {
        final ProcessSessionFactory processSessionFactory = getProcessSessionFactory();
        final ProcessSession processSession = processSessionFactory.createSession();
        return processSession;
    }

    private ProcessSessionFactory getProcessSessionFactory() {
        ProcessSessionFactory processSessionFactory;
        do {
            processSessionFactory = sessionFactoryReference.get();
            if (processSessionFactory == null) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                }
            }
        } while (processSessionFactory == null);
        return processSessionFactory;
    }
}
