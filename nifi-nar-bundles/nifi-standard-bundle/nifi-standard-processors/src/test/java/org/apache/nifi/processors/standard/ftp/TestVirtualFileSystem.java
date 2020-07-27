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


import org.apache.nifi.processors.standard.ftp.filesystem.DefaultVirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFtpFile;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualPath;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestVirtualFileSystem {

    private VirtualFileSystem fileSystem;

    @Before
    public void setup() {
        setupVirtualDirectoryStructure();
    }

    private void setupVirtualDirectoryStructure() {
        fileSystem = new DefaultVirtualFileSystem();
        fileSystem.mkdir(new VirtualPath("/Directory1"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory1"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"));
        fileSystem.mkdir(new VirtualPath("/Directory1/SubDirectory2"));
        fileSystem.mkdir(new VirtualPath("/Directory2"));
        fileSystem.mkdir(new VirtualPath("/Directory2/SubDirectory3"));
        fileSystem.mkdir(new VirtualPath("/Directory2/SubDirectory4"));
    }

    @Test
    public void testListContentsOfDirectory() {
        // GIVEN
        VirtualPath parent = new VirtualPath("/Directory1");
        VirtualFtpFile[] expectedSubDirectories = {
                new VirtualFtpFile(new VirtualPath("/Directory1/SubDirectory1"), fileSystem),
                new VirtualFtpFile(new VirtualPath("/Directory1/SubDirectory2"), fileSystem)
        };

        // WHEN
        List<VirtualFtpFile> subDirectories = fileSystem.listChildren(parent);

        // THEN
        assertThat(subDirectories, containsInAnyOrder(expectedSubDirectories));
    }

    @Test
    public void testListContentsOfRoot() {
        // GIVEN
        VirtualPath parent = new VirtualPath("/");
        VirtualFtpFile[] expectedSubDirectories = {
                new VirtualFtpFile(new VirtualPath("/Directory1"), fileSystem),
                new VirtualFtpFile(new VirtualPath("/Directory2"), fileSystem)
        };

        // WHEN
        List<VirtualFtpFile> subDirectories = fileSystem.listChildren(parent);

        // THEN
        assertThat(subDirectories, containsInAnyOrder(expectedSubDirectories));
    }

    @Test
    public void testListContentsOfEmptyDirectory() {
        // GIVEN
        VirtualPath parent = new VirtualPath("/Directory2/SubDirectory3");

        // WHEN
        List<VirtualFtpFile> subDirectories = fileSystem.listChildren(parent);

        // THEN
        assertEquals(0, subDirectories.size());
    }

    @Test
    public void testDeleteNonEmptyDirectory() {
        // GIVEN
        List<VirtualPath> expectedRemainingDirectories = Arrays.asList(
                new VirtualPath("/"),
                new VirtualPath("/Directory2"),
                new VirtualPath("/Directory2/SubDirectory3"),
                new VirtualPath("/Directory2/SubDirectory4")
        );

        // WHEN
        boolean success = fileSystem.delete(new VirtualPath("/Directory1"));

        // THEN
        assertTrue(success);
        assertAllDirectoriesAre(expectedRemainingDirectories);
    }

    @Test
    public void testDeleteEmptyDirectory() {
        // GIVEN
        List<VirtualPath> expectedRemainingDirectories = Arrays.asList(
                new VirtualPath("/"),
                new VirtualPath("/Directory1"),
                new VirtualPath("/Directory1/SubDirectory1"),
                new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"),
                new VirtualPath("/Directory1/SubDirectory2"),
                new VirtualPath("/Directory2"),
                new VirtualPath("/Directory2/SubDirectory4")
        );

        // WHEN
        boolean success = fileSystem.delete(new VirtualPath("/Directory2/SubDirectory3"));

        // THEN
        assertTrue(success);
        assertAllDirectoriesAre(expectedRemainingDirectories);
    }

    @Test
    public void testDeleteRoot() {

        // WHEN
        boolean success = fileSystem.delete(new VirtualPath("/"));

        // THEN
        assertFalse(success);
    }

    @Test
    public void testDeleteNonExistentDirectory() {
        // GIVEN
        List<VirtualPath> expectedRemainingDirectories = Arrays.asList(
                new VirtualPath("/"),
                new VirtualPath("/Directory1"),
                new VirtualPath("/Directory1/SubDirectory1"),
                new VirtualPath("/Directory1/SubDirectory1/SubSubDirectory"),
                new VirtualPath("/Directory1/SubDirectory2"),
                new VirtualPath("/Directory2"),
                new VirtualPath("/Directory2/SubDirectory3"),
                new VirtualPath("/Directory2/SubDirectory4")
        );

        // WHEN
        boolean success = fileSystem.delete(new VirtualPath("/Directory3"));

        // THEN
        assertFalse(success);
        assertAllDirectoriesAre(expectedRemainingDirectories);
    }

    private void assertAllDirectoriesAre(List<VirtualPath> expectedDirectories) {
        if (expectedDirectories.size() != fileSystem.getTotalNumberOfFiles()) {
            fail();
        } else {
            for (VirtualPath path : expectedDirectories) {
                if (!fileSystem.exists(path)) {
                    fail();
                }
            }
        }
    }

}
