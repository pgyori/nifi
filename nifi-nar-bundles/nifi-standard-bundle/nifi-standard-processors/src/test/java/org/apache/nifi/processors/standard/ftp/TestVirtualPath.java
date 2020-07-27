package org.apache.nifi.processors.standard.ftp;

import org.apache.nifi.processors.standard.ftp.filesystem.VirtualPath;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestVirtualPath {

    @Test
    public void testCreatePathStartingWithSlash() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals("/Directory1/Directory2", result);
    }

    @Test
    public void testCreatePathNotStartingWithSlash() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals("/Directory1/Directory2", result);
    }

    @Test
    public void testCreatPathToRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals("/", result);
    }

    @Test
    public void testEmptyPathPointsToRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals("/", result);
    }

    @Test
    public void testPathIsNormalized() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1///Directory2\\\\Directory3/Directory4/../..");

        // WHEN
        String result = objectUnderTest.toString();

        // THEN
        assertEquals("/Directory1/Directory2", result);
    }

    @Test
    public void getFileNameForRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN, THEN
        assertEquals("/", objectUnderTest.getFileName());
    }

    @Test
    public void testGetFileNameForNonRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2/file.txt");

        // WHEN
        String result = objectUnderTest.getFileName();

        // THEN
        assertEquals("file.txt", result);
    }

    @Test
    public void getParentForRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/");

        // WHEN, THEN
        assertNull(objectUnderTest.getParent());
    }

    @Test
    public void testGetParentForNonRoot() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2/file.txt");

        // WHEN
        VirtualPath parent = objectUnderTest.getParent();

        // THEN
        assertEquals("/Directory1/Directory2", parent.toString());
    }

    @Test
    public void testResolveToARelativePath() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("Directory3/Directory4").toString();

        // THEN
        assertEquals("/Directory1/Directory2/Directory3/Directory4", result);
    }

    @Test
    public void testResolveToParent() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("..").toString();

        // THEN
        assertEquals("/Directory1", result);
    }

    @Test
    public void testResolveToAnAbsolutePath() {
        // GIVEN
        VirtualPath objectUnderTest = new VirtualPath("/Directory1/Directory2");

        // WHEN
        String result = objectUnderTest.resolve("/Directory3/Directory4").toString();

        // THEN
        assertEquals("/Directory3/Directory4", result);
    }

    @Test
    public void testEquals() {
        // GIVEN
        VirtualPath path1 = new VirtualPath("/Directory1/Directory2");
        VirtualPath path2 = new VirtualPath("/Directory1/Directory2");

        // WHEN, THEN
        assertEquals(path1, path2);
    }

    @Test
    public void testDoesNotEqual() {
        // GIVEN
        VirtualPath path1 = new VirtualPath("/Directory1/Directory2");
        VirtualPath path2 = new VirtualPath("/directory1/Directory2");

        // WHEN, THEN
        assertNotEquals(path1, path2);
    }

}
