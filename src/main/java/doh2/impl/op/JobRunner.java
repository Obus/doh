package doh2.impl.op;

import org.apache.hadoop.mapreduce.Job;

public interface JobRunner {
    public void runJob(Job job) throws Exception;
}
