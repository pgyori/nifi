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

import java.util.ArrayList;
import java.util.List;

public class DefaultVirtualFileSystem implements VirtualFileSystem {

    private volatile List<VirtualPath> existingPaths;

    public DefaultVirtualFileSystem() {
        existingPaths = new ArrayList<>();
        existingPaths.add(new VirtualPath("/"));
    }

    @Override
    public synchronized boolean mkdir(VirtualPath newFile) {
        if (existingPaths.contains(newFile)) {
            return false;
        } else {
            existingPaths.add(newFile);
            return true;
        }
    }

    @Override
    public synchronized boolean exists(VirtualPath virtualFile) {
        return existingPaths.contains(virtualFile);
    }

    @Override
    public synchronized boolean delete(VirtualPath virtualFile) {
        VirtualPath root = new VirtualPath("/");
        if (virtualFile.equals(root)) { // Root cannot be deleted
            return false;
        } else if (existingPaths.contains(virtualFile)) {
            existingPaths.removeIf(e -> isChildOf(virtualFile, e));
            return true;
        } else {
            return false;
        }
    }

    private boolean isChildOf(VirtualPath parent, VirtualPath childCandidate) {
        VirtualPath root = new VirtualPath("/");
        if (parent.equals(root)) {
            return !childCandidate.equals(root); // Every file is the child of root except for root itself.
        } else if (parent.getNameCount() > childCandidate.getNameCount()) { // The parent's absolute path must be shorter
            return false;
        } else if (parent.getNameCount() == childCandidate.getNameCount()) {
            return parent.equals(childCandidate);
        } else {
            return isChildOf(parent, childCandidate.getParent());
        }
    }

    @Override
    public synchronized List<VirtualFtpFile> listChildren(VirtualPath parent) {
        List<VirtualFtpFile> children = new ArrayList<>();
        VirtualPath root = new VirtualPath("/");

        if (parent.equals(root)) {
            for (VirtualPath existingPath : existingPaths) {
                if (!existingPath.equals(root)) {
                    if (existingPath.getNameCount() == 1) {
                        children.add(new VirtualFtpFile(existingPath, this));
                    }
                }
            }
        } else {
            int parentNameCount = parent.getNameCount();
            for (VirtualPath existingPath : existingPaths) {
                if ((existingPath.getParent() != null) && existingPath.getParent().equals(parent)) {
                    if (existingPath.getNameCount() == (parentNameCount + 1)) {
                        children.add(new VirtualFtpFile(existingPath, this));
                    }
                }
            }
        }
        return children;
    }

    @Override
    public synchronized int getTotalNumberOfFiles() {
        return existingPaths.size();
    }

}
