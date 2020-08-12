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
package org.apache.nifi.processors.standard.ftp.filesystem;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualFileSystemView implements FileSystemView {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualFileSystemView.class);
    private VirtualPath currentDirectory = new VirtualPath("/");
    private VirtualFileSystem fileSystem;

    public VirtualFileSystemView(User user, VirtualFileSystem fileSystem) throws IllegalArgumentException {
        if (user == null || fileSystem == null) {
            throw new IllegalArgumentException("User and filesystem cannot be null.");
        } else {
            LOG.info("Virtual filesystem view created for user \"{}\"", user.getName());
            this.fileSystem = fileSystem;
        }
    }

    @Override
    public FtpFile getHomeDirectory() {
        return new VirtualFtpFile(new VirtualPath("/"), fileSystem);
    }

    @Override
    public FtpFile getWorkingDirectory() {
        return new VirtualFtpFile(currentDirectory, fileSystem);
    }

    @Override
    public boolean changeWorkingDirectory(String targetPath) {
        VirtualPath targetDirectory = currentDirectory.resolve(targetPath);
        if (fileSystem.exists(targetDirectory)) {
            currentDirectory = targetDirectory;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public FtpFile getFile(String fileName) {
        VirtualPath filePath = currentDirectory.resolve(fileName);
        return new VirtualFtpFile(filePath, fileSystem);
    }

    @Override
    public boolean isRandomAccessible() {
        return false;
    }

    @Override
    public void dispose() { }
}
