package hk.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

public class PointRecordReader implements RecordReader<IntWritable, Text>
{
	FileSplit split;
	Configuration conf;
	Path path;
	int currentline = 0;
	long currentByteRead = 0;
	long filelength = 0;
	public String filePrefix = "point";
	FSDataInputStream fsDataInputStream = null;
	BufferedReader bufferedReader = null;
	int pointtype;

	public PointRecordReader(FileSplit split, Configuration conf)
			throws IOException
	{
		this.split = split;
		this.conf = conf;
		this.path = split.getPath();
		filelength = split.getLength();
		pointtype = Integer.parseInt(path.getName().substring(filePrefix.length()));
		FileSystem fs = FileSystem.get(new Configuration(true));
		fsDataInputStream = fs.open(path);
		InputStreamReader reader = new InputStreamReader(fsDataInputStream);
		bufferedReader = new BufferedReader(reader);
	}

	// key for type
	// value for vector string
	@Override
	public boolean next(IntWritable key, Text value) throws IOException
	{
		String string;
		while (true)
		{
			string = bufferedReader.readLine();
			if (string == null)
				return false;
			if (string.isEmpty())
				continue;
			else
				break;
		}
		currentByteRead = fsDataInputStream.getPos();
		key.set(pointtype);
		value.set(string);
		return true;
	}

	@Override
	public IntWritable createKey()
	{
		return new IntWritable();
	}

	@Override
	public Text createValue()
	{
		return new Text();
	}

	@Override
	public long getPos() throws IOException
	{
		return currentByteRead;
	}

	@Override
	public void close() throws IOException
	{
	}

	@Override
	public float getProgress() throws IOException
	{
		// TODO auto generated
		return ((float) currentByteRead) / filelength;
	}
}
