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
import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.CommandFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.command.impl.ABOR;
import org.apache.ftpserver.command.impl.AUTH;
import org.apache.ftpserver.command.impl.CDUP;
import org.apache.ftpserver.command.impl.CWD;
import org.apache.ftpserver.command.impl.EPRT;
import org.apache.ftpserver.command.impl.EPSV;
import org.apache.ftpserver.command.impl.FEAT;
import org.apache.ftpserver.command.impl.LIST;
import org.apache.ftpserver.command.impl.MDTM;
import org.apache.ftpserver.command.impl.MKD;
import org.apache.ftpserver.command.impl.MLSD;
import org.apache.ftpserver.command.impl.MLST;
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
import org.apache.ftpserver.command.impl.RMD;
import org.apache.ftpserver.command.impl.SITE;
import org.apache.ftpserver.command.impl.SITE_DESCUSER;
import org.apache.ftpserver.command.impl.SITE_HELP;
import org.apache.ftpserver.command.impl.SITE_STAT;
import org.apache.ftpserver.command.impl.SITE_WHO;
import org.apache.ftpserver.command.impl.SITE_ZONE;
import org.apache.ftpserver.command.impl.SIZE;
import org.apache.ftpserver.command.impl.STAT;
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
import org.apache.nifi.processors.standard.ftp.commands.FtpCommandHELP;
import org.apache.nifi.processors.standard.ftp.commands.FtpCommandSTOR;
import org.apache.nifi.processors.standard.ftp.commands.NotSupportedCommand;
import org.apache.nifi.processors.standard.ftp.filesystem.DefaultVirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystemFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class NifiFtpServer {

    private Map<String, Command> commandMap = new HashMap<>();
    private FtpCommandHELP customHelpCommand = new FtpCommandHELP();

    private final FtpServer server;
    private static final String HOME_DIRECTORY = "/virtual/ftproot";

    public NifiFtpServer(AtomicReference<ProcessSessionFactory> sessionFactory, String username, String password, String bindAddress, int port)
            throws FtpException, FtpServerConfigurationException {
        initializeCommandMap(sessionFactory);

        FtpServerFactory serverFactory = new FtpServerFactory();
        VirtualFileSystem fileSystem = new DefaultVirtualFileSystem();
        serverFactory.setFileSystem(new VirtualFileSystemFactory(fileSystem));
        serverFactory.setCommandFactory(createCommandFactory(commandMap));
        boolean anonymousLoginEnabled = (username == null);
        serverFactory.setConnectionConfig(createConnectionConfig(anonymousLoginEnabled));
        serverFactory.addListener("default", createListener(bindAddress, port));
        if (anonymousLoginEnabled) {
            serverFactory.getUserManager().save(createAnonymousUser(HOME_DIRECTORY, Collections.singletonList(new WritePermission())));
        } else {
            serverFactory.getUserManager().save(createUser(username, password, HOME_DIRECTORY, Collections.singletonList(new WritePermission()))); //TODO: can throw an exception that is propagated to @OnScheduled. Proper solution: catch in OnScheduled and wrap in a ProcessException.
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

    private Listener createListener(String bindAddress, int port) throws FtpServerConfigurationException {
        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setServerAddress(bindAddress);
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

    private void initializeCommandMap(AtomicReference<ProcessSessionFactory> sessionFactory) {
        addToCommandMap("ABOR", new ABOR());
        addToCommandMap("ACCT", new NotSupportedCommand("Operation (ACCT) not supported."));
        addToCommandMap("APPE", new NotSupportedCommand("Operation (APPE) not supported."));
        addToCommandMap("AUTH", new AUTH());
        addToCommandMap("CDUP", new CDUP());
        addToCommandMap("CWD", new CWD());
        addToCommandMap("DELE", new NotSupportedCommand("Operation (DELE) not supported."));
        addToCommandMap("EPRT", new EPRT());
        addToCommandMap("EPSV", new EPSV());
        addToCommandMap("FEAT", new FEAT());
        addToCommandMap("HELP", customHelpCommand);
        addToCommandMap("LIST", new LIST());
        addToCommandMap("MFMT", new NotSupportedCommand("Operation (MFMT) not supported."));
        addToCommandMap("MDTM", new MDTM());
        addToCommandMap("MLST", new MLST());
        addToCommandMap("MKD", new MKD());
        addToCommandMap("MLSD", new MLSD());
        addToCommandMap("MODE", new MODE());
        addToCommandMap("NLST", new NLST());
        addToCommandMap("NOOP", new NOOP());
        addToCommandMap("OPTS", new OPTS());
        addToCommandMap("PASS", new PASS());
        addToCommandMap("PASV", new PASV());
        addToCommandMap("PBSZ", new PBSZ());
        addToCommandMap("PORT", new PORT());
        addToCommandMap("PROT", new PROT());
        addToCommandMap("PWD", new PWD());
        addToCommandMap("QUIT", new QUIT());
        addToCommandMap("REIN", new REIN());
        addToCommandMap("REST", new NotSupportedCommand("Operation (REST) not supported."));
        addToCommandMap("RETR", new NotSupportedCommand("Operation (RETR) not supported."));
        addToCommandMap("RMD", new RMD());
        //addToCommandMap("RNFR", new RNFR());
        //addToCommandMap("RNTO", new RNTO());
        addToCommandMap("SITE", new SITE());
        addToCommandMap("SIZE", new SIZE());
        addToCommandMap("SITE_DESCUSER", new SITE_DESCUSER());
        addToCommandMap("SITE_HELP", new SITE_HELP());
        addToCommandMap("SITE_STAT", new SITE_STAT());
        addToCommandMap("SITE_WHO", new SITE_WHO());
        addToCommandMap("SITE_ZONE", new SITE_ZONE());

        addToCommandMap("STAT", new STAT());
        addToCommandMap("STOR", new FtpCommandSTOR(sessionFactory));
        addToCommandMap("STOU", new FtpCommandSTOR(sessionFactory));
        addToCommandMap("STRU", new STRU());
        addToCommandMap("SYST", new SYST());
        addToCommandMap("TYPE", new TYPE());
        addToCommandMap("USER", new USER());
    }

    private void addToCommandMap(String command, Command instance) {
        commandMap.put(command, instance);
        if (!(instance instanceof NotSupportedCommand)) {
            customHelpCommand.addCommand(command);
        }
    }
}
