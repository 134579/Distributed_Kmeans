package hk.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class PointInputFormat extends FileInputFormat<IntWritable, Text>
{
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename)
	{
		// TODO auto generated
		return false;
	}
	
	@Override
	public RecordReader<IntWritable, Text> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter)
			throws IOException
	{
		reporter.setStatus(split.toString());
		return new PointRecordReader((FileSplit)split,job);
		// TODO auto generated
		
	}
	
}
