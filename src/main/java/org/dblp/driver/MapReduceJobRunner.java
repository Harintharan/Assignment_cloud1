package org.dblp.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dblp.bucketing.BucketingMapper;
import org.dblp.bucketing.BucketingReducer;
import org.dblp.input.DBLPXmlInputFormat;
import org.dblp.utils.Constants;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MapReduceJobRunner {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: MapReduceJobRunner <input-path> <output-path> <job-name> [bucket-size]");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String jobName = args[2];
        int bucketSize = args.length >= 4 ? Integer.parseInt(args[3]) : Integer.parseInt(Constants.DEFAULT_BUCKET_SIZE);

        Configuration conf = new Configuration();

        // Add your custom config values
        conf.set(Constants.JOB_NAME, jobName);
        conf.setInt(Constants.BUCKET_SIZE, bucketSize);

        // Define possible start and end tags for XML
        conf.setStrings(Constants.POSSIBLE_START_TAGS,
                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
        conf.setStrings(Constants.POSSIBLE_END_TAGS,
                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");

        Job job = Job.getInstance(conf, "DBLP Bucketing Job - " + jobName);
        job.setJarByClass(MapReduceJobRunner.class);

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // Input format
        job.setInputFormatClass(DBLPXmlInputFormat.class);

        // Mapper and Reducer
        job.setMapperClass(BucketingMapper.class);
        job.setReducerClass(BucketingReducer.class);

        // Output key-value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Submit the job and wait
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
