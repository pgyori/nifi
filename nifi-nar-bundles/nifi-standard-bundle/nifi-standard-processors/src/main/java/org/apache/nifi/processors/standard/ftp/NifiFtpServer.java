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

import org.apache.ftpserver.ConnectionConfig;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.CommandFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.command.impl.ABOR;
import org.apache.ftpserver.command.impl.ACCT;
import org.apache.ftpserver.command.impl.AUTH;
import org.apache.ftpserver.command.impl.CDUP;
import org.apache.ftpserver.command.impl.CWD;
import org.apache.ftpserver.command.impl.EPRT;
import org.apache.ftpserver.command.impl.EPSV;
import org.apache.ftpserver.command.impl.FEAT;
import org.apache.ftpserver.command.impl.LIST;
import org.apache.ftpserver.command.impl.MKD;
import org.apache.ftpserver.command.impl.MLSD;
import org.apache.ftpserver.command.impl.MODE;
import org.apache.ftpserver.command.impl.NLST;
import org.apache.ftpserver.command.impl.NOOP;
import org.apache.ftpserver.command.impl.OPTS;
import org.apache.ftpserver.command.impl.PASS;
import org.apache.ftpserver.command.impl.PASV;
import org.apache.ftpserver.command.impl.PBSZ;
import org.apache.ftpserver.command.impl.PORT;
import org.apache.ftpserver.command.impl.PROT;
import org.apache.ftpserver.command.impl.PWD;
import org.apache.ftpserver.command.impl.QUIT;
import org.apache.ftpserver.command.impl.REIN;
import org.apache.ftpserver.command.impl.REST;
import org.apache.ftpserver.command.impl.RETR;
import org.apache.ftpserver.command.impl.RMD;
import org.apache.ftpserver.command.impl.RNFR;
import org.apache.ftpserver.command.impl.RNTO;
import org.apache.ftpserver.command.impl.SITE;
import org.apache.ftpserver.command.impl.SITE_DESCUSER;
import org.apache.ftpserver.command.impl.SITE_HELP;
import org.apache.ftpserver.command.impl.SITE_STAT;
import org.apache.ftpserver.command.impl.SITE_WHO;
import org.apache.ftpserver.command.impl.SITE_ZONE;
import org.apache.ftpserver.command.impl.SIZE;
import org.apache.ftpserver.command.impl.STAT;
import org.apache.ftpserver.command.impl.STOU;
import org.apache.ftpserver.command.impl.STRU;
import org.apache.ftpserver.command.impl.SYST;
import org.apache.ftpserver.command.impl.TYPE;
import org.apache.ftpserver.command.impl.USER;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.nifi.processor.ProcessSessionFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class NifiFtpServer {

    private final FtpServerFactory serverFactory = new FtpServerFactory();
    private final FtpServer server;
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference;
    private final int port;
    private final String username;
    private final String password;
    private final String homeDirectory = "/virtual/ftproot";

    public NifiFtpServer(AtomicReference<ProcessSessionFactory> sessionFactoryReference, String username, String password, int port) throws FtpException {
        this.sessionFactoryReference = sessionFactoryReference;
        this.username = username;
        this.password = password;
        this.port = port;

        serverFactory.setFileSystem(new VirtualFileSystemFactory());
        serverFactory.setCommandFactory(createCommandFactory(createCommandMap()));
        boolean anonymousLoginEnabled = (this.username == null);
        serverFactory.setConnectionConfig(createConnectionConfig(anonymousLoginEnabled));
        serverFactory.addListener("default", createListener(this.port));
        if (anonymousLoginEnabled) {
            serverFactory.getUserManager().save(createAnonymousUser(this.homeDirectory, Collections.singletonList(new WritePermission())));
        } else {
            serverFactory.getUserManager().save(createUser(this.username, this.password, this.homeDirectory, Collections.singletonList(new WritePermission()))); //TODO: can throw an exception that is propagated to @OnScheduled. Proper solution: catch in OnScheduled and wrap in a ProcessException.
        }

        server = serverFactory.createServer();
    }

    public void start() throws FtpException {
        server.start();
    }

    public void stop() {
        server.stop();
    }

    public boolean isStopped() {
        return server.isStopped();
    }

    private CommandFactory createCommandFactory(Map<String, Command> commandMap) {
        CommandFactoryFactory commandFactoryFactory = new CommandFactoryFactory();
        commandFactoryFactory.setUseDefaultCommands(false);
        commandFactoryFactory.setCommandMap(commandMap);
        return commandFactoryFactory.createCommandFactory();
    }

    private ConnectionConfig createConnectionConfig(boolean anonymousLoginEnabled) {
        ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
        connectionConfigFactory.setAnonymousLoginEnabled(anonymousLoginEnabled);
        return connectionConfigFactory.createConnectionConfig();
    }

    private Listener createListener(int port) {
        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setPort(port);
        return listenerFactory.createListener();
    }

    private User createUser(String username, String password, String homeDirectory, List<Authority> authorities) {
        BaseUser user = new BaseUser();
        user.setName(username);
        user.setPassword(password);
        user.setHomeDirectory(homeDirectory);
        user.setAuthorities(authorities);
        return user;
    }

    private User createAnonymousUser(String homeDirectory, List<Authority> authorities) {
        BaseUser user = new BaseUser();
        user.setName("anonymous");
        user.setHomeDirectory(homeDirectory);
        user.setAuthorities(authorities);
        return user;
    }

    private Map<String, Command> createCommandMap() {
        Map<String, Command> commandMap = new HashMap<>();

        commandMap.put("ABOR", new ABOR());
        commandMap.put("ACCT", new ACCT());
        commandMap.put("APPE", new FtpCommandAPPE());
        commandMap.put("AUTH", new AUTH());
        commandMap.put("CDUP", new CDUP());
        commandMap.put("CWD", new CWD());
        commandMap.put("DELE", new FtpCommandDELE());
        commandMap.put("EPRT", new EPRT());
        commandMap.put("EPSV", new EPSV());
        commandMap.put("FEAT", new FEAT());
        commandMap.put("HELP", new FtpCommandHELP());
        commandMap.put("LIST", new LIST());
        commandMap.put("MFMT", new FtpCommandMFMT());
        commandMap.put("MKD", new MKD());
        commandMap.put("MLSD", new MLSD());
        commandMap.put("MODE", new MODE());
        commandMap.put("NLST", new NLST());
        commandMap.put("NOOP", new NOOP());
        commandMap.put("OPTS", new OPTS());
        commandMap.put("PASS", new PASS());
        commandMap.put("PASV", new PASV());
        commandMap.put("PBSZ", new PBSZ());
        commandMap.put("PORT", new PORT());
        commandMap.put("PROT", new PROT());
        commandMap.put("PWD", new PWD());
        commandMap.put("QUIT", new QUIT());
        commandMap.put("REIN", new REIN());
        commandMap.put("REST", new REST());
        commandMap.put("RETR", new RETR());
        commandMap.put("RMD", new RMD());
        commandMap.put("RNFR", new RNFR());
        commandMap.put("RNTO", new RNTO());
        commandMap.put("SITE", new SITE());
        commandMap.put("SIZE", new SIZE());
        commandMap.put("SITE_DESCUSER", new SITE_DESCUSER());
        commandMap.put("SITE_HELP", new SITE_HELP());
        commandMap.put("SITE_STAT", new SITE_STAT());
        commandMap.put("SITE_WHO", new SITE_WHO());
        commandMap.put("SITE_ZONE", new SITE_ZONE());

        commandMap.put("STAT", new STAT());
        commandMap.put("STOR", new FtpCommandSTOR(sessionFactoryReference));
        commandMap.put("STOU", new STOU());
        commandMap.put("STRU", new STRU());
        commandMap.put("SYST", new SYST());
        commandMap.put("TYPE", new TYPE());
        commandMap.put("USER", new USER());

        return commandMap;
    }
}
