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
import org.apache.ftpserver.command.impl.MKD;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFileActionFtpReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FtpCommandMKD extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(FtpCommandMKD.class);

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
                        final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {

        // reset state
        session.resetState();

        // argument check
        String fileName = request.getArgument();
        if (fileName == null) {
            session.write(LocalizedFileActionFtpReply.translate(session, request, context,
                    FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                    "MKD", null, null));
        } else {
            session.write(new DefaultFtpReply(FtpReply.REPLY_257_PATHNAME_CREATED, "Directory created."));
        }
    }
}
