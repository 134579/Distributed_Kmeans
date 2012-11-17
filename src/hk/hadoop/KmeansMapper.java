package hk.hadoop;

import hk.hadoop.io.PointWritable;
import hk.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import hk.util.Vector;

import hk.exception.EmptyLineException;
import hk.exception.VectorSizeDismatchException;

public class KmeansMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, IntWritable, PointWritable>
{
	/**
	 * centroids
	 */
	Point[] centroids = null;
	/**
	 * k for kmeans
	 */
	int k;

	/**
	 * Initilize k and centroids
	 * 
	 * @throws IOException
	 * @throws EmptyLineException
	 */
	public KmeansMapper() throws IOException, EmptyLineException
	{
		getKFromFile();
		getCentroidsFromFile();
	}

	public void getKFromFile() throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration(true));
		FSDataInputStream fsDataInputStream = fs.open(new Path(Kmeans.KMEANS_CONFIG_FILE));
		InputStreamReader inputStreamReader=new InputStreamReader(fsDataInputStream);
		BufferedReader reader=new BufferedReader(inputStreamReader);
		k=Integer.parseInt(reader.readLine());
		fsDataInputStream.close();
	}

	/**
	 * Get centroids from file using {@link Kmeans.getCentroidsFromFile}
	 * 
	 * @throws IOException
	 * @throws EmptyLineException
	 */
	public void getCentroidsFromFile() throws IOException, EmptyLineException
	{
		centroids = Kmeans.getCentroidsFromFile(k);
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, PointWritable> output,
			Reporter reporter) throws IOException
	{
		String string = value.toString();
		double mindis = Double.MAX_VALUE;
		int assignedtype = Point.NONEXIST;
		Vector vector = null;
		try
		{
			vector = new Vector(string);
		}
		catch (EmptyLineException e)
		{
			e.printStackTrace();
			return;
		}
		PointWritable thisPoint = new PointWritable(Point.NONEXIST,Point.NONEXIST,
				vector);
		for (Point centroid : centroids)
		{
			double thisdis = 0;
			try
			{
				thisdis = centroid.getDistance(thisPoint);
			}
			catch (VectorSizeDismatchException e)
			{
				e.printStackTrace();
			}
			if (thisdis < mindis)
			{
				mindis = thisdis;
				assignedtype = centroid.truetype;
			}
		}
		thisPoint.assigntype = assignedtype;
		output.collect(new IntWritable(assignedtype), thisPoint);
	}
}
