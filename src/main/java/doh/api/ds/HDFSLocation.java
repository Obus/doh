package doh.api.ds;

import org.apache.hadoop.fs.Path;

public abstract class HDFSLocation implements Location {
    @Override
    public boolean isHDFS() {
        return true;
    }

    public abstract boolean isSingle();

    public static class SingleHDFSLocation extends HDFSLocation {
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
    }

    public static class MultyHDFSLocation extends HDFSLocation {
        private final Path[] paths;

        public MultyHDFSLocation(Path[] path) {
            this.paths = path;
        }

        public Path[] getPaths() {
            return paths;
        }

        @Override
        public boolean isSingle() {
            return false;
        }
    }
}
