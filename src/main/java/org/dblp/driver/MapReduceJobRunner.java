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


