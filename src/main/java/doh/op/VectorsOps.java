package doh.op;

import doh.NamedFunction;
import doh.ds.Clusters;
import doh.ds.Doubles;
import doh.ds.Vectors;


public class VectorsOps {

    public Vectors sample(Vectors vectors, int size) {
        return null;
    }

    public Vectors kMaxDistant(Vectors vectors, int K) {
        return null;
    }

    public Doubles pairWiseDistances(Vectors vectors) {
        return null;
    }

    public Vectors canopies(Vectors vectors, double ... t1t2t3t4) {
        return null;
    }

    public Clusters toClusters(Vectors vectors) {
        return null;
    }

    public static class RandomSampleFun implements NamedFunction<Vectors, Vectors> {
        @Override
        public String name() {
            return "randomSample";
        }

        @Override
        public Vectors apply(Vectors arg) {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
