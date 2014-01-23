package doh2.api;

import doh.api.ds.Location;
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

        @Override
        public Path[] getPaths() {
            return new Path[]{path};
        }
    }

    public static class MultiHDFSLocation extends HDFSLocation {
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

    public abstract Path[] getPaths();
}
