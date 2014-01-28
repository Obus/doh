package doh2.impl.ondemand;

import doh2.api.DSContext;
import doh2.api.HDFSLocation;
import doh2.api.SingleHDFSLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobPackExecutor {

    private final DSContext dsContext;
    private final OpJobMaker opJobMaker;

    public JobPackExecutor(DSContext dsContext, OpJobMaker opJobMaker) {
        this.dsContext = dsContext;
        this.opJobMaker = opJobMaker;
    }

    public void execute(JobPack jobPack) throws Exception {
        if (jobPack.mapTaskOp != null) {
            dsContext.getOpSerializer().saveMapperOp(dsContext.conf(), jobPack.mapTaskOp);
        }
        if (jobPack.reduceTaskOp != null) {
            dsContext.getOpSerializer().saveReducerOp(dsContext.conf(), jobPack.reduceTaskOp);
        }

        final Job job = opJobMaker.makeJob(dsContext.conf(),
                jobPack.input.getKeyClass(),
                jobPack.input.getValueClass(),
                jobPack.input.getLocation().getPaths(),
                jobPack.mapTaskOp, jobPack.reduceTaskOp);

        specifyDatSetDetails(job, jobPack.output.details());

        job.setInputFormatClass(jobPack.input.details().inputFormatClass());

        dsContext.getJobRunner().runJob(job);

        jobPack.output.setReady();

    }



    private DSDetails specifyDatSetDetails(Job job, DSDetails details) {
        if (details.formatClass == null) {
            details.formatClass = dsContext.getDefaultFormatClass();
        }
        if (singleLocationPath(details.location) == null) {
            details.location = new SingleHDFSLocation(dsContext.getTempPathManager().getNextPath());
        }

        job.setOutputFormatClass(details.formatClass);
        FileOutputFormat.setOutputPath(job, singleLocationPath(details.location));
        if (details.numReducers != null) {
            job.setNumReduceTasks(details.numReducers);
        }
        return details;
    }


    private Path singleLocationPath(HDFSLocation location) {
        if (location == null) {
            return null;
        }
        if (location.isSingle()) {
            return ((SingleHDFSLocation)location).getPath();
        }
        else {
            throw new IllegalArgumentException("Output path should have single output path");
        }
    }



}
