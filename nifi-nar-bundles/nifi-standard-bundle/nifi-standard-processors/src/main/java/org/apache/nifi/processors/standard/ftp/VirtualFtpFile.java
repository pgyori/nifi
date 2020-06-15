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

import org.apache.ftpserver.ftplet.FtpFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class VirtualFtpFile implements FtpFile {

    // the file name with respect to the user root.
    // The path separator character will be '/' and
    // it will always begin with '/'.
    private final String fileName;

    /**
     * Constructor, internal do not use directly.
     */
    protected VirtualFtpFile(String fileName) throws IllegalArgumentException {
        if (fileName == null) {
            throw new IllegalArgumentException("fileName cannot be null");
        } else if (fileName.length() == 0) {
            throw new IllegalArgumentException("fileName cannot be empty");
        } else if (fileName.charAt(0) != '/') {
            throw new IllegalArgumentException("fileName must be an absolute path");
        }

        this.fileName = fileName;
    }

    @Override
    public String getAbsolutePath() {
        return removeLastSlash(fileName);
    }

    private String removeLastSlash(String path) {
        String pathWithLastSlashRemoved;
        if ((path.length() != 1) && (path.endsWith("/"))) {
            pathWithLastSlashRemoved = path.substring(0, path.length() - 1);
        } else {
            pathWithLastSlashRemoved = path;
        }
        return pathWithLastSlashRemoved;
    }

    @Override
    public String getName() {
        return getFileNameFromPath(fileName);
    }

    private String getFileNameFromPath(String path) {
        if (path.equals("/")) {
            return path;
        }
        String pathWithLastSlashRemoved = removeLastSlash(fileName);
        int lastSlashIndex = pathWithLastSlashRemoved.lastIndexOf('/');
        if (lastSlashIndex != -1) {
            return pathWithLastSlashRemoved.substring(lastSlashIndex + 1);
        } else {
            return pathWithLastSlashRemoved;
        }
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isDirectory() {
        return true; //TODO: for now, only directories are handled since files are converted into flowfiles immediately. Double check!
    }

    @Override
    public boolean isFile() {
        return false; //TODO: for now, only directories are handled since files are converted into flowfiles immediately. Double check!
    }

    @Override
    public boolean doesExist() {
        return true;
    }

    @Override
    public boolean isReadable() {
        return true;
    }

    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public boolean isRemovable() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.isRemovable()");
    }

    @Override
    public String getOwnerName() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.getOwnerName()");
    }

    @Override
    public String getGroupName() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.getGroupName()");
    }

    @Override
    public int getLinkCount() {
        return 1;
    }

    @Override
    public long getLastModified() {
        return Calendar.getInstance().getTimeInMillis();
    }

    @Override
    public boolean setLastModified(long l) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.setLastModified()");
    }

    @Override
    public long getSize() {
        return 0;
    }

    @Override
    public Object getPhysicalFile() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.getPhysicalFile()");
    }

    @Override
    public boolean mkdir() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.mkdir()");
    }

    @Override
    public boolean delete() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.delete()");
    }

    @Override
    public boolean move(FtpFile ftpFile) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.move()");
    }

    @Override
    public List<? extends FtpFile> listFiles() throws UnsupportedOperationException {
        return Collections.emptyList();
    }

    @Override
    public OutputStream createOutputStream(long l) throws IOException,  UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.createOutputStream()");
    }

    @Override
    public InputStream createInputStream(long l) throws IOException,  UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.createInputStream()");
    }
}
