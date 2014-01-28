package doh2.api;

import org.apache.hadoop.fs.Path;

public class SingleHDFSLocation extends HDFSLocation {
    private final Path path;

    public SingleHDFSLocation(Path path) {
        this.path = path;
    }

    @Override
    public boolean isSingle() {
        return true;
    }

    public Path getPath() {
        return path;
    }

    @Override
    public Path[] getPaths() {
        return new Path[]{path};
    }
}
