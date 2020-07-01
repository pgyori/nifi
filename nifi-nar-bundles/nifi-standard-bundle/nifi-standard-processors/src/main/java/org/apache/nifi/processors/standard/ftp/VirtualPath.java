package org.apache.nifi.processors.standard.ftp;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class VirtualPath {

    private Path path; // always normalized

    public VirtualPath(String path) {
        String absolutePath = "/" + normalizeSeparator(path);
        this.path = Paths.get(absolutePath).normalize();
    }

    private String normalizeSeparator(String path) {
        String normalizedPath = path.replace(File.separatorChar, '/');
        normalizedPath = normalizedPath.replace('\\', '/');
        return normalizedPath;
    }

    public String getFileName() {
        if (path.getFileName() == null) {
            return "/";
        } else {
            return path.getFileName().toString();
        }
    }

    public VirtualPath getParent() {
        if (path.getParent() == null) {
            return null;
        } else {
            return new VirtualPath(path.getParent().toString());
        }
    }

    public boolean isAbsolute() {
        return path.isAbsolute();
    }

    public VirtualPath resolve(String otherPath) {
        return new VirtualPath(path.resolve(otherPath).normalize().toString());
    }

    public String toString() {
        return path.toString();
    }

    public int getNameCount() {
        return path.getNameCount();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof VirtualPath)) {
            return false;
        }
        VirtualPath other = (VirtualPath) o;
        return path.equals(other.path);
    }
}
