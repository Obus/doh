package doh.crazy;

import org.apache.hadoop.mapreduce.Job;

public class JobRunner {
    public void runJob(Job job) throws Exception {
        job.waitForCompletion(true);
    }
}
