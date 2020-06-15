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

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestVirtualFileSystemView {

    private FileSystemView fileSystemView;

    @Before
    public void setup() throws FtpException {
        User user = createUser();
        FileSystemFactory fileSystemFactory = new VirtualFileSystemFactory();
        fileSystemView = fileSystemFactory.createFileSystemView(user);
    }

    @Test
    public void testInRootDirectory() throws FtpException {

        // WHEN
        // We do not change directories

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToAnotherDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("/ghi");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/ghi");
    }

    @Test
    public void testChangeToRootDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("/");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToUnspecifiedDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory("");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToSameDirectory() throws FtpException {

        // WHEN
        fileSystemView.changeWorkingDirectory(".");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToSameDirectoryNonRoot() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/ghi");

        // WHEN
        fileSystemView.changeWorkingDirectory(".");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/ghi");
    }

    @Test
    public void testChangeToParentDirectory() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/ghi");

        // WHEN
        fileSystemView.changeWorkingDirectory("..");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/");
    }

    @Test
    public void testChangeToParentDirectoryNonRoot() throws FtpException {
        // GIVEN
        fileSystemView.changeWorkingDirectory("/ghi");
        fileSystemView.changeWorkingDirectory("/jkl");

        // WHEN
        fileSystemView.changeWorkingDirectory("..");

        // THEN
        assertHomeDirectory("/");
        assertCurrentDirectory("/ghi");
    }

    private User createUser() {
        BaseUser user = new BaseUser();
        user.setName("Username");
        user.setPassword("Password");
        user.setHomeDirectory("/abc/def");
        user.setAuthorities(Collections.singletonList(new WritePermission()));
        return user;
    }

    private void assertHomeDirectory(String expectedHomeDirectory) throws FtpException {
        FtpFile homeDirectory = fileSystemView.getHomeDirectory();
        assertEquals(expectedHomeDirectory, homeDirectory.getAbsolutePath());
    }

    private void assertCurrentDirectory(String expectedCurrentDirectory) throws FtpException {
        FtpFile currentDirectory = fileSystemView.getWorkingDirectory();
        assertEquals(expectedCurrentDirectory, currentDirectory.getAbsolutePath());
    }
}
