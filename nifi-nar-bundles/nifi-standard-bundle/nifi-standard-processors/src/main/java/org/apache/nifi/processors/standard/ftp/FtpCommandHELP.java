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
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FtpCommandHELP extends AbstractCommand {

    private static String DEFAULT_HELP = "The following commands are supported.\n" +
            "ABOR CDUP CWD HELP LIST\n" +
            "MKD MODE NLST NOOP PASS PASV PORT PWD\n" +
            "QUIT REST RETR RMD RNFR RNTO SITE SIZE\n" +
            "STAT STOR STOU STRU SYST TYPE USER\n" +
            "End of help.";

    private static Map<String, String> COMMAND_SPECIFIC_HELP;

    static {
        Map<String, String> commands = new HashMap<>();
        commands.put("ABOR", "Syntax: ABOR");
        commands.put("CDUP", "Syntax: CDUP");
        commands.put("CWD", "Syntax: CWD <sp> <pathname>");
        commands.put("EPRT", "Syntax: EPRT<space><d><net-prt><d><net-addr><d><tcp-port><d>");
        commands.put("HELP", "Syntax: HELP [<sp> <string>]");
        commands.put("LIST", "Syntax: LIST [<sp> <pathname>]");
        commands.put("MKD", "Syntax: MKD <sp> <pathname>");
        commands.put("MODE", "Syntax: MODE <sp> <mode-code>");
        commands.put("NLST", "Syntax: NLST [<sp> <pathname>]");
        commands.put("NOOP", "Syntax: NOOP");
        commands.put("PASS", "Syntax: PASS <sp> <password>");
        commands.put("PASV", "Syntax: PASV");
        commands.put("PORT", "Syntax: PORT <sp> <host-port>");
        commands.put("PWD", "Syntax: PWD");
        commands.put("QUIT", "Syntax: QUIT");
        commands.put("REST", "Syntax: RETR <sp> <marker>");
        commands.put("RETR", "Syntax: RETR <sp> <pathname>");
        commands.put("RMD", "Syntax: RMD <sp> <pathname>");
        commands.put("RNFR", "Syntax: RNFR <sp> <pathname>");
        commands.put("RNTO", "Syntax: RNTO <sp> <pathname>");
        commands.put("SITE", "Syntax: SITE <sp> <string>");
        commands.put("STOR", "Syntax: STOR <sp> <pathname>");
        commands.put("STOU", "Syntax: STOU");
        commands.put("SYST", "Syntax: SYST");
        commands.put("TYPE", "Syntax: TYPE <sp> <type-code>");
        commands.put("USER", "Syntax: USER <sp> <username>");
        COMMAND_SPECIFIC_HELP = Collections.unmodifiableMap(commands);
    }

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
                        final FtpServerContext context, final FtpRequest request)
            throws IOException {

        // reset state variables
        session.resetState();

        if (!request.hasArgument()) {
            sendDefaultHelpMessage(session);
        } else {
            handleRequestWithArgument(session, request);
        }
    }

    private void sendDefaultHelpMessage(FtpIoSession session) {
        sendCustomHelpMessage(session, DEFAULT_HELP);
    }

    private void sendCustomHelpMessage(FtpIoSession session, String message) {
        session.write(new DefaultFtpReply(FtpReply.REPLY_214_HELP_MESSAGE, message));
    }

    private void handleRequestWithArgument(FtpIoSession session, FtpRequest request) {
        // Send command-specific help if available
        String ftpCommand = request.getArgument().toUpperCase();
        String commandSpecificHelp = COMMAND_SPECIFIC_HELP.get(ftpCommand);

        if (commandSpecificHelp == null) {
            sendDefaultHelpMessage(session);
        } else {
            sendCustomHelpMessage(session, commandSpecificHelp);
        }
    }
}
