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

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class VirtualFileSystemView implements FileSystemView {

    private final Logger LOG = LoggerFactory.getLogger(VirtualFileSystemView.class);

    // the root directory will always end with '/'.
    private String rootDir;

    // the first and the last character will always be '/'
    // It is always with respect to the root directory.
    private String currDir;

    private final User user; //TODO: remove if not needed

    protected VirtualFileSystemView(User user) throws IllegalArgumentException {
        if (user == null) {
            throw new IllegalArgumentException("User cannot be null.");
        } else if (user.getHomeDirectory() == null) {
            throw new IllegalArgumentException("User home directory not set.");
        } else {
            this.user = user;
            this.rootDir = endWithSlash(normalizeSeparatorChar(user.getHomeDirectory()));
            currDir = "/";
            LOG.debug("Virtual filesystem view created for user \"{}\" with root \"{}\"", user.getName(), rootDir);
        }
    }

    /**
     * Separator character should be '/' always.
     */
    private String normalizeSeparatorChar(final String pathName) {
        String normalizedPathName = pathName.replace(File.separatorChar, '/');
        normalizedPathName = normalizedPathName.replace('\\', '/');
        return normalizedPathName;
    }

    @Override
    public FtpFile getHomeDirectory() {
        return new VirtualFtpFile("/");
    }

    @Override
    public FtpFile getWorkingDirectory() {
        return new VirtualFtpFile(currDir);
    }

    @Override
    public boolean changeWorkingDirectory(String targetPath) {

        currDir = getNavigationResult(currDir, targetPath);

        return true;
    }

    private String getNavigationResult(String currentPath, String targetPath) {
        String newCurrentPath;
        if (targetPath.equals("")) {
            newCurrentPath = currentPath;
        } else if (targetPath.equals(".")) {
            newCurrentPath = currentPath;
        } else if (targetPath.equals("..")) {
            newCurrentPath = getParentPath(currentPath);
        } else {
            newCurrentPath = getSimpleNavigationResult(currentPath, targetPath);
        }
        return newCurrentPath;
    }

    private String getParentPath(String currentPath) {
        String parentPath;
        if (currentPath.equals("/")) {
            parentPath = currentPath;
        } else {
            int indexOfLastSlash = currentPath.substring(0, currentPath.length()-1).lastIndexOf("/");
            parentPath = currentPath.substring(0, indexOfLastSlash + 1);
        }
        return parentPath;
    }

    private String getSimpleNavigationResult(String currentPath, String targetPath) {
        String newCurrentPath;
        if (targetPath.startsWith("/")) {
            newCurrentPath = currentPath + targetPath.substring(1); // Skip the targetPath's starting '/'
        } else {
            newCurrentPath = currentPath + targetPath;
        }
        newCurrentPath = endWithSlash(newCurrentPath);
        return newCurrentPath;
    }

    private String endWithSlash(String subject) {
        if (subject.endsWith("/")) {
            return subject;
        } else {
            return subject.concat("/");
        }
    }

    @Override
    public FtpFile getFile(String fileName) {
        String normalizedFileName = normalizeSeparatorChar(fileName);
        FtpFile ftpFile;
        if (normalizedFileName.startsWith("/")) { // absolute path
            ftpFile = new VirtualFtpFile(normalizedFileName);
        } else {                                  // relative path
            ftpFile = new VirtualFtpFile(currDir + normalizedFileName);
        }
        return ftpFile;
    }

    @Override
    public boolean isRandomAccessible() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFileSystemView.isRandomAccessible()");
    }

    @Override
    public void dispose() { }
}
