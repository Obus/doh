package doh2.api;

import org.apache.hadoop.fs.Path;

public class MultiHDFSLocation extends HDFSLocation {
    private final Path[] paths;

    public MultiHDFSLocation(Path[] path) {
        this.paths = path;
    }

    @Override
    public Path[] getPaths() {
        return paths;
    }

    @Override
    public boolean isSingle() {
        return false;
    }
}
