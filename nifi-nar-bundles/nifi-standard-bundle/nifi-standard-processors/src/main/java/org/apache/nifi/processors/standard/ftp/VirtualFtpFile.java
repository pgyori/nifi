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
import java.util.List;

public class VirtualFtpFile implements FtpFile {

    private VirtualPath path;
    private VirtualFileSystem fileSystem;

    protected VirtualFtpFile(VirtualPath path, VirtualFileSystem fileSystem) throws IllegalArgumentException {
        if (path == null || fileSystem == null) {
            throw new IllegalArgumentException("File path and fileSystem cannot be null");
        }
        this.path = path;
        this.fileSystem = fileSystem;
    }

    @Override
    public String getAbsolutePath() {
        return path.toString();
    }

    @Override
    public String getName() {
        return path.getFileName();
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
        return fileSystem.exists(path);
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
    public boolean isRemovable() {
        return true; //Every virtual directory can be deleted
    }

    @Override
    public String getOwnerName() {
        return "Owner"; //TODO: used in NLST -> LISTFileFormater class
    }

    @Override
    public String getGroupName() {
        return "Group"; //TODO: used in NLST -> LISTFileFormater class
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
    public boolean mkdir() {
        return fileSystem.mkdir(path);
    }

    @Override
    public boolean delete() {
        return fileSystem.delete(path);
    }

    @Override
    public boolean move(FtpFile ftpFile) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.move()");
    }

    @Override
    public List<? extends FtpFile> listFiles() {
        return fileSystem.listChildren(path);
    }

    @Override
    public OutputStream createOutputStream(long l) throws IOException,  UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.createOutputStream()");
    }

    @Override
    public InputStream createInputStream(long l) throws IOException,  UnsupportedOperationException {
        throw new UnsupportedOperationException("VirtualFtpFile.createInputStream()");
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof VirtualFtpFile)) {
            return false;
        }
        VirtualFtpFile other = (VirtualFtpFile) o;
        return path.equals(other.path);
    }

}
