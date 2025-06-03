//package org.dblp.driver;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.dblp.bucketing.BucketingMapper;
//import org.dblp.bucketing.BucketingReducer;
//import org.dblp.input.DBLPXmlInputFormat;
//import org.dblp.utils.Constants;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//
//public class MapReduceJobRunner {
//
//    public static void main(String[] args) throws Exception {
//
//        if (args.length < 3) {
//            System.err.println("Usage: MapReduceJobRunner <input-path> <output-path> <job-name> [bucket-size]");
//            System.exit(-1);
//        }
//
//        String inputPath = args[0];
//        String outputPath = args[1];
//        String jobName = args[2];
//        int bucketSize = args.length >= 4 ? Integer.parseInt(args[3]) : Integer.parseInt(Constants.DEFAULT_BUCKET_SIZE);
//
//        Configuration conf = new Configuration();
//
//        // Add your custom config values
//        conf.set(Constants.JOB_NAME, jobName);
//        conf.setInt(Constants.BUCKET_SIZE, bucketSize);
//
//        // Define possible start and end tags for XML
//        conf.setStrings(Constants.POSSIBLE_START_TAGS,
//                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
//                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
//        conf.setStrings(Constants.POSSIBLE_END_TAGS,
//                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
//                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");
//
//        Job job = Job.getInstance(conf, "DBLP Bucketing Job - " + jobName);
//        job.setJarByClass(MapReduceJobRunner.class);
//
//        // Input and output paths
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//
//        // Input format
//        job.setInputFormatClass(DBLPXmlInputFormat.class);
//
//        // Mapper and Reducer
//        job.setMapperClass(BucketingMapper.class);
//        job.setReducerClass(BucketingReducer.class);
//
//        // Output key-value types
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        // Submit the job and wait
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}


//package org.dblp.driver;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.dblp.authorship.AuthorshipScoreMapper;
//import org.dblp.authorship.AuthorshipScoreReducer;
//import org.dblp.input.DBLPXmlInputFormat;
//import org.dblp.utils.Constants;
//
//public class MapReduceJobRunner {
//
//    public static void main(String[] args) throws Exception {
//
//        if (args.length < 3) {
//            System.err.println("Usage: MapReduceJobRunner <input-path> <output-path> <job-name>");
//            System.exit(-1);
//        }
//
//        String inputPath = args[0];
//        String outputPath = args[1];
//        String jobName = args[2];
//
//        Configuration conf = new Configuration();
//
//        // Add your custom config values
//        conf.set(Constants.JOB_NAME, jobName);
//
//        // Define possible start and end tags for XML
//        conf.setStrings(Constants.POSSIBLE_START_TAGS,
//                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
//                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
//        conf.setStrings(Constants.POSSIBLE_END_TAGS,
//                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
//                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");
//
//        Job job = Job.getInstance(conf, "DBLP Authorship Score Job - " + jobName);
//        job.setJarByClass(MapReduceJobRunner.class);
//
//        // Input and output paths
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//
//        // Input format
//        job.setInputFormatClass(DBLPXmlInputFormat.class);
//
//        // Mapper and Reducer
//        job.setMapperClass(AuthorshipScoreMapper.class);
//        job.setReducerClass(AuthorshipScoreReducer.class);
//        job.setCombinerClass(AuthorshipScoreReducer.class);
//
//        // Output key-value types
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(FloatWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(FloatWritable.class);
//
//        // Submit the job and wait
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}

//
//package org.dblp.driver;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.dblp.input.DBLPXmlInputFormat;
//import org.dblp.maxmedian.MaxMedianAvgMapper;
//import org.dblp.maxmedian.MaxMedianAvgReducer;
//import org.dblp.maxmedian.MaxMedianAvgWritable;
//import org.dblp.utils.Constants;
//
//public class MapReduceJobRunner {
//
//    public static void main(String[] args) throws Exception {
//
//        if (args.length < 3) {
//            System.err.println("Usage: MapReduceJobRunner <input-path> <output-path> <job-name>");
//            System.exit(-1);
//        }
//
//        String inputPath = args[0];
//        String outputPath = args[1];
//        String jobName = args[2];
//
//        Configuration conf = new Configuration();
//        conf.set(Constants.JOB_NAME, jobName);
//
//        // Set XML tags
//        conf.setStrings(Constants.POSSIBLE_START_TAGS,
//                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
//                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
//        conf.setStrings(Constants.POSSIBLE_END_TAGS,
//                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
//                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");
//
//        Job job = Job.getInstance(conf, "DBLP MaxMedianAvg Job - " + jobName);
//        job.setJarByClass(MapReduceJobRunner.class);
//
//        // Input/output
//        FileInputFormat.addInputPath(job, new Path(inputPath));
//        FileOutputFormat.setOutputPath(job, new Path(outputPath));
//        job.setInputFormatClass(DBLPXmlInputFormat.class);
//
//        // Mapper and Reducer
//        job.setMapperClass(MaxMedianAvgMapper.class);
//        job.setReducerClass(MaxMedianAvgReducer.class);
//
//        // Output key/value types
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(MaxMedianAvgWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        // Run the job
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}



package org.dblp.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dblp.input.DBLPXmlInputFormat;
import org.dblp.sortByNumCoAuthors.SortByNumCoAuthorsReducer;
import org.dblp.sortByNumCoAuthors.SortByNumCoAuthorsMapper;
import org.dblp.sortByNumCoAuthors.InverseNumCoAuthorsMapper;
import org.dblp.sortByNumCoAuthors.InverseNumCoAuthorsReducer;
import org.dblp.sortByNumCoAuthors.DescendingOrderComparator;




import org.dblp.utils.Constants;

public class MapReduceJobRunner {

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println("Usage: MapReduceJobRunner <input-path> <temp-output-path> <final-output-path> <job-name>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String tempOutputPath = args[1];   // intermediate output for stage 1
        String finalOutputPath = args[2];  // final sorted result
        String jobName = args[3];

        Configuration conf = new Configuration();
        conf.set(Constants.JOB_NAME, jobName);

        conf.setStrings(Constants.POSSIBLE_START_TAGS,
                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
        conf.setStrings(Constants.POSSIBLE_END_TAGS,
                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");

        // ------------------ Stage 1: Author → Co-author Count ------------------
        Job stage1 = Job.getInstance(conf, "Stage 1 - Count CoAuthors - " + jobName);
        stage1.setJarByClass(MapReduceJobRunner.class);

        FileInputFormat.addInputPath(stage1, new Path(inputPath));
        FileOutputFormat.setOutputPath(stage1, new Path(tempOutputPath));

        stage1.setInputFormatClass(DBLPXmlInputFormat.class);
        stage1.setMapperClass(SortByNumCoAuthorsMapper.class);
        stage1.setReducerClass(SortByNumCoAuthorsReducer.class);
        stage1.setMapOutputKeyClass(Text.class);
        stage1.setMapOutputValueClass(Text.class);
        stage1.setOutputKeyClass(Text.class);
        stage1.setOutputValueClass(IntWritable.class);

        boolean stage1Success = stage1.waitForCompletion(true);
        if (!stage1Success) {
            System.err.println("Stage 1 failed.");
            System.exit(1);
        }

        // ------------------ Stage 2: Sort by Co-author Count (Descending) ------------------
        Job stage2 = Job.getInstance(conf, "Stage 2 - Sort Descending - " + jobName);
        stage2.setJarByClass(MapReduceJobRunner.class);

        FileInputFormat.addInputPath(stage2, new Path(tempOutputPath));
        FileOutputFormat.setOutputPath(stage2, new Path(finalOutputPath));

        stage2.setMapperClass(InverseNumCoAuthorsMapper.class);
        stage2.setReducerClass(InverseNumCoAuthorsReducer.class);
        stage2.setSortComparatorClass(DescendingOrderComparator.class);
        stage2.setMapOutputKeyClass(IntWritable.class);
        stage2.setMapOutputValueClass(Text.class);
        stage2.setOutputKeyClass(Text.class);
        stage2.setOutputValueClass(IntWritable.class);

        System.exit(stage2.waitForCompletion(true) ? 0 : 1);
    }
}

