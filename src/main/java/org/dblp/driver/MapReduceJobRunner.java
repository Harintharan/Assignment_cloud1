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

//
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

//
//package org.dblp.driver;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.dblp.input.DBLPXmlInputFormat;
//import org.dblp.sortByNumCoAuthors.SortByNumCoAuthorsReducer;
//import org.dblp.sortByNumCoAuthors.SortByNumCoAuthorsMapper;
//import org.dblp.sortByNumCoAuthors.InverseNumCoAuthorsMapper;
//import org.dblp.sortByNumCoAuthors.InverseNumCoAuthorsReducer;
//import org.dblp.sortByNumCoAuthors.DescendingOrderComparator;
//
//
//
//
//import org.dblp.utils.Constants;
//
//public class MapReduceJobRunner {
//
//    public static void main(String[] args) throws Exception {
//
//        if (args.length < 4) {
//            System.err.println("Usage: MapReduceJobRunner <input-path> <temp-output-path> <final-output-path> <job-name>");
//            System.exit(-1);
//        }
//
//        String inputPath = args[0];
//        String tempOutputPath = args[1];   // intermediate output for stage 1
//        String finalOutputPath = args[2];  // final sorted result
//        String jobName = args[3];
//
//        Configuration conf = new Configuration();
//        conf.set(Constants.JOB_NAME, jobName);
//
//        conf.setStrings(Constants.POSSIBLE_START_TAGS,
//                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
//                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
//        conf.setStrings(Constants.POSSIBLE_END_TAGS,
//                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
//                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");
//
//        // ------------------ Stage 1: Author → Co-author Count ------------------
//        Job stage1 = Job.getInstance(conf, "Stage 1 - Count CoAuthors - " + jobName);
//        stage1.setJarByClass(MapReduceJobRunner.class);
//
//        FileInputFormat.addInputPath(stage1, new Path(inputPath));
//        FileOutputFormat.setOutputPath(stage1, new Path(tempOutputPath));
//
//        stage1.setInputFormatClass(DBLPXmlInputFormat.class);
//        stage1.setMapperClass(SortByNumCoAuthorsMapper.class);
//        stage1.setReducerClass(SortByNumCoAuthorsReducer.class);
//        stage1.setMapOutputKeyClass(Text.class);
//        stage1.setMapOutputValueClass(Text.class);
//        stage1.setOutputKeyClass(Text.class);
//        stage1.setOutputValueClass(IntWritable.class);
//
//        boolean stage1Success = stage1.waitForCompletion(true);
//        if (!stage1Success) {
//            System.err.println("Stage 1 failed.");
//            System.exit(1);
//        }
//
//        // ------------------ Stage 2: Sort by Co-author Count (Descending) ------------------
//        Job stage2 = Job.getInstance(conf, "Stage 2 - Sort Descending - " + jobName);
//        stage2.setJarByClass(MapReduceJobRunner.class);
//
//        FileInputFormat.addInputPath(stage2, new Path(tempOutputPath));
//        FileOutputFormat.setOutputPath(stage2, new Path(finalOutputPath));
//
//        stage2.setMapperClass(InverseNumCoAuthorsMapper.class);
//        stage2.setReducerClass(InverseNumCoAuthorsReducer.class);
//        stage2.setSortComparatorClass(DescendingOrderComparator.class);
//        stage2.setMapOutputKeyClass(IntWritable.class);
//        stage2.setMapOutputValueClass(Text.class);
//        stage2.setOutputKeyClass(Text.class);
//        stage2.setOutputValueClass(IntWritable.class);
//
//        System.exit(stage2.waitForCompletion(true) ? 0 : 1);
//    }
//}



package org.dblp.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dblp.input.DBLPXmlInputFormat;
import org.dblp.bucketing.BucketingMapper;
import org.dblp.bucketing.BucketingReducer;
import org.dblp.authorship.AuthorshipScoreMapper;
import org.dblp.authorship.AuthorshipScoreReducer;
import org.dblp.maxmedian.MaxMedianAvgMapper;
import org.dblp.maxmedian.MaxMedianAvgReducer;
import org.dblp.maxmedian.MaxMedianAvgWritable;
import org.dblp.sortByNumCoAuthors.*;
import org.dblp.utils.Constants;

public class MapReduceJobRunner {

    public static void main(String[] args) throws Exception {

        if (args.length < 6) {
            System.err.println("Usage: MapReduceJobRunner <input-path> <output-bucketing> <output-authorship> <output-maxmedian> <output-sort-stage1> <output-sort-stage2>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputBucketing = args[1];
        String outputAuthorship = args[2];
        String outputMaxMedian = args[3];
        String outputSortStage1 = args[4];
        String outputSortStage2 = args[5];

        Configuration conf = new Configuration();
        conf.setStrings(Constants.POSSIBLE_START_TAGS,
                "<article ", "<inproceedings ", "<proceedings ", "<book ", "<incollection ",
                "<phdthesis ", "<mastersthesis ", "<www ", "<person ", "<data ");
        conf.setStrings(Constants.POSSIBLE_END_TAGS,
                "</article>", "</inproceedings>", "</proceedings>", "</book>", "</incollection>",
                "</phdthesis>", "</mastersthesis>", "</www>", "</person>", "</data>");

        // ------------------ Stage 1: Bucketing Publications ------------------
        Job stage1 = Job.getInstance(conf, "Stage 1 - Bucketing Publications");
        stage1.setJarByClass(MapReduceJobRunner.class);
        FileInputFormat.addInputPath(stage1, new Path(inputPath));
        FileOutputFormat.setOutputPath(stage1, new Path(outputBucketing));
        stage1.setInputFormatClass(DBLPXmlInputFormat.class);
        stage1.setMapperClass(BucketingMapper.class);
        stage1.setReducerClass(BucketingReducer.class);
        stage1.setMapOutputKeyClass(Text.class);
        stage1.setMapOutputValueClass(IntWritable.class);
        stage1.setOutputKeyClass(Text.class);
        stage1.setOutputValueClass(IntWritable.class);

        if (!stage1.waitForCompletion(true)) {
            System.err.println("Stage 1 (Bucketing) failed.");
            System.exit(1);
        }

        // ------------------ Stage 2: Authorship Score ------------------
        Job stage2 = Job.getInstance(conf, "Stage 2 - Authorship Score");
        stage2.setJarByClass(MapReduceJobRunner.class);
        FileInputFormat.addInputPath(stage2, new Path(inputPath));
        FileOutputFormat.setOutputPath(stage2, new Path(outputAuthorship));
        stage2.setInputFormatClass(DBLPXmlInputFormat.class);
        stage2.setMapperClass(AuthorshipScoreMapper.class);
        stage2.setReducerClass(AuthorshipScoreReducer.class);
        stage2.setCombinerClass(AuthorshipScoreReducer.class);
        stage2.setMapOutputKeyClass(Text.class);
        stage2.setMapOutputValueClass(FloatWritable.class);
        stage2.setOutputKeyClass(Text.class);
        stage2.setOutputValueClass(FloatWritable.class);

        if (!stage2.waitForCompletion(true)) {
            System.err.println("Stage 2 (Authorship) failed.");
            System.exit(1);
        }

        // ------------------ Stage 3: MaxMedianAvg ------------------
        Job stage3 = Job.getInstance(conf, "Stage 3 - MaxMedianAvg");
        stage3.setJarByClass(MapReduceJobRunner.class);
        FileInputFormat.addInputPath(stage3, new Path(inputPath));
        FileOutputFormat.setOutputPath(stage3, new Path(outputMaxMedian));
        stage3.setInputFormatClass(DBLPXmlInputFormat.class);
        stage3.setMapperClass(MaxMedianAvgMapper.class);
        stage3.setReducerClass(MaxMedianAvgReducer.class);
        stage3.setMapOutputKeyClass(Text.class);
        stage3.setMapOutputValueClass(MaxMedianAvgWritable.class);
        stage3.setOutputKeyClass(Text.class);
        stage3.setOutputValueClass(Text.class);

        if (!stage3.waitForCompletion(true)) {
            System.err.println("Stage 3 (MaxMedianAvg) failed.");
            System.exit(1);
        }

        // ------------------ Stage 4: Sort by Co-author Count ------------------
        Job stage4_1 = Job.getInstance(conf, "Stage 4.1 - CoAuthor Count");
        stage4_1.setJarByClass(MapReduceJobRunner.class);
        FileInputFormat.addInputPath(stage4_1, new Path(inputPath));
        FileOutputFormat.setOutputPath(stage4_1, new Path(outputSortStage1));
        stage4_1.setInputFormatClass(DBLPXmlInputFormat.class);
        stage4_1.setMapperClass(SortByNumCoAuthorsMapper.class);
        stage4_1.setReducerClass(SortByNumCoAuthorsReducer.class);
        stage4_1.setMapOutputKeyClass(Text.class);
        stage4_1.setMapOutputValueClass(Text.class);
        stage4_1.setOutputKeyClass(Text.class);
        stage4_1.setOutputValueClass(IntWritable.class);

        if (!stage4_1.waitForCompletion(true)) {
            System.err.println("Stage 4.1 (CoAuthor Count) failed.");
            System.exit(1);
        }

        Job stage4_2 = Job.getInstance(conf, "Stage 4.2 - Sort Descending");
        stage4_2.setJarByClass(MapReduceJobRunner.class);
        FileInputFormat.addInputPath(stage4_2, new Path(outputSortStage1));
        FileOutputFormat.setOutputPath(stage4_2, new Path(outputSortStage2));
        stage4_2.setMapperClass(InverseNumCoAuthorsMapper.class);
        stage4_2.setReducerClass(InverseNumCoAuthorsReducer.class);
        stage4_2.setSortComparatorClass(DescendingOrderComparator.class);
        stage4_2.setMapOutputKeyClass(IntWritable.class);
        stage4_2.setMapOutputValueClass(Text.class);
        stage4_2.setOutputKeyClass(Text.class);
        stage4_2.setOutputValueClass(IntWritable.class);

        if (!stage4_2.waitForCompletion(true)) {
            System.err.println("Stage 4.2 (Sorting) failed.");
            System.exit(1);
        }

        System.exit(0);
    }
}


