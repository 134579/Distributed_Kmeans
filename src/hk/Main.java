package hk;

import hk.Point;
import hk.hadoop.io.PointWritable;
import hk.hadoop.Kmeans;
import hk.hadoop.KmeansMapper;
import hk.hadoop.KmeansReducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.mortbay.jetty.AbstractGenerator.OutputWriter;

import hk.util.Vector;

import hk.exception.EmptyLineException;
import hk.exception.VectorSizeDismatchException;

public class Main
{
	static int k;
	static String[] input;
	static String out;
	
	
	
	/**
	 * Assign each point to a centroid, the true type of the point depends on
	 * which file it's in
	 * 
	 * @param filelist
	 * @return
	 * @throws EmptyLineException
	 * @throws IOException
	 * @throws VectorSizeDismatchException
	 */
	static double getAccuracy(String[] filelist) throws EmptyLineException,
			IOException, VectorSizeDismatchException
	{
		int nright = 0;
		int ntotal = 0;
		Point[] centroids = Kmeans.getCentroidsFromFile(k);
		FileSystem fs = FileSystem.get(new Configuration(true));
		for (int i = 0; i < filelist.length; i++)
		{
			String filename = filelist[i];
			FSDataInputStream fsDataInputStream = fs.open(new Path(filename));
			InputStreamReader inputStreamReader = new InputStreamReader(
					fsDataInputStream);
			BufferedReader reader = new BufferedReader(inputStreamReader);
			String line = null;
			line = reader.readLine();
			int truetype = i;
			while (line != null)
			{
				Vector vector = new Vector(line);
				Point thisPoint = new Point(Point.NONEXIST, i, vector);
				double mindis = Double.MAX_VALUE;
				int assignedtype = -1;
				for (int j = 0; j < centroids.length; j++)
				{
					double thisdis = centroids[j].getDistance(thisPoint);
					if (thisdis < mindis)
					{
						mindis = thisdis;
						assignedtype = j;
					}
				}
				if (truetype == assignedtype)
				{
					nright++;
				}
				ntotal++;
				line = reader.readLine();
			}
		}
		return ((double) nright) / ntotal;
	}

	/**
	 * Write k to file 
	 * @throws IOException 
	 */
	static void writeKToFile() throws IOException
	{
		String filename=Kmeans.KMEANS_CONFIG_FILE;
		FileSystem fs=FileSystem.get(new Configuration(true));
		FSDataOutputStream fsDataOutputStream=fs.create(new Path(filename),true);
		OutputStreamWriter writer=new OutputStreamWriter(fsDataOutputStream);
		writer.write(Integer.toString(k));
		writer.close();
		
	}
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException,
			EmptyLineException, VectorSizeDismatchException
	{
		// TODO auto generated
		if (args.length < 2)
		{
			System.err.println("usage: <inputs> <output>");
			return;
		}
		input = new String[args.length - 1];
		out=new String (args[args.length-1]);
		k = input.length;
		writeKToFile();
		Kmeans.deleteFileWithPrefix(args[args.length - 1]);
		Kmeans.deleteFileWithPrefix("centroid");
		for (int i = 0; i < input.length; i++)
		{
			input[i] = args[i];
		}
		Kmeans kmeans = new Kmeans(k);
		kmeans.loadFile(input);
		Point[] centroids = kmeans.getFirstKPoints();
		for (Point centroid : centroids)
		{
			kmeans.writeCentroidToFile(centroid);
		}
		
		JobConf jobconf = new JobConf(Main.class);
		for (int i = 0; i < args.length - 1; i++)
			FileInputFormat.addInputPath(jobconf, new Path(args[i]));
		FileOutputFormat.setOutputPath(jobconf, new Path(out));
		jobconf.setMapperClass(KmeansMapper.class);
		jobconf.setReducerClass(KmeansReducer.class);
		jobconf.setMapOutputKeyClass(IntWritable.class);
		jobconf.setMapOutputValueClass(PointWritable.class);
		
		// conf.setInputFormat(PointInputFormat.class);
		
		Point[] oldcentroids = kmeans.getCentroidsFromFile(k);
		JobClient.runJob(jobconf);
		Point[] newcentroids = null;
		do
		{
			double accuracy=getAccuracy(input);
			appendfile("result", accuracy);
			
			newcentroids = Kmeans.getCentroidsFromFile(k);
			
			if(Point.equals(oldcentroids, newcentroids))
				break;
			Kmeans.deleteFileWithPrefix(out);
			JobClient.runJob(jobconf);
			
			oldcentroids=newcentroids;
		} while (true);
	}

	static void appendfile(String filename, Object data) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration(true));
		if(!fs.exists(new Path(filename)))
		{
			FSDataOutputStream fsDataOutputStream=fs.create(new Path(filename), true);
			OutputStreamWriter outputStreamWriter=new OutputStreamWriter(fsDataOutputStream);
			
			outputStreamWriter.write(data.toString());
			outputStreamWriter.close();
			return;
		}
		FSDataInputStream fsDataInpStream = fs.open(new Path(filename));
		InputStreamReader inputStreamReader=new InputStreamReader(fsDataInpStream);
		BufferedReader reader=new BufferedReader(inputStreamReader);
		String line=reader.readLine();
		line=line+" "+data.toString();
		reader.close();
		FSDataOutputStream fsDataOutputStream=fs.create(new Path(filename), true);
		OutputStreamWriter outputStreamWriter=new OutputStreamWriter(fsDataOutputStream);
		
		outputStreamWriter.write(line);
		outputStreamWriter.close();
	}
}
