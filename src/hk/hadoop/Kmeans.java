package hk.hadoop;

import hk.Point;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hk.exception.EmptyLineException;

import hk.util.Vector;

public class Kmeans
{
	String[] pointFile;
	// # of points per file
	int[] nPointPerFile;
	int nfile;
	/* k for kmeans */
	int k = 0;
	// default centroid file name prefix
	public static final String CENTROID_FILE_PREFIX = "centroid";
	
	/**
	 * kmeans config file
	 */
	public static final String KMEANS_CONFIG_FILE = "kmeansConf";
	
	public Kmeans(int k)
	{
		// TODO auto generated
		this.k = k;
	}

	/**
	 * Load files
	 * 
	 * @param inputfilename
	 * @throws IOException
	 */
	public void loadFile(String[] inputfilename) throws IOException
	{
		nfile = inputfilename.length;
		pointFile = new String[nfile];
		System.arraycopy(inputfilename, 0, pointFile, 0, nfile);
		getNPerFile();
	}

	/**
	 * Delete file with given prefix
	 * 
	 * @param filenameprefix
	 * @throws IOException
	 */
	public static void deleteFileWithPrefix(String filenameprefix)
			throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration(true));
		FileStatus[] fileStatuses = fs.listStatus(new Path("."));
		for (FileStatus fileStatus : fileStatuses)
		{
			Path path = fileStatus.getPath();
			if (path.getName().startsWith(filenameprefix))
				fs.delete(path, true);
		}
	}

	/**
	 * Get # of points(non-empty line) per file
	 * 
	 * @throws IOException
	 */
	public void getNPerFile() throws IOException
	{
		nPointPerFile = new int[nfile];
		for (int i = 0; i < nfile; i++)
		{
			String filename = pointFile[i];
			FileSystem fs = FileSystem.get(new Configuration(true));
			FSDataInputStream fsDataInputStream = fs.open(new Path(filename));
			InputStreamReader inputStreamReader = new InputStreamReader(
					fsDataInputStream);
			BufferedReader reader = new BufferedReader(inputStreamReader);
			nPointPerFile[i] = 0;
			String string = reader.readLine();
			while (string != null)
			{
				if (!string.isEmpty())
					nPointPerFile[i]++;
				string = reader.readLine();
			}
		}
	}

	/**
	 * Get the first k centroids
	 * 
	 * @return
	 * @throws IOException
	 * @throws EmptyLineException
	 */
	public Point[] getFirstKPoints() throws IOException, EmptyLineException
	{
		Point[] centroids = new Point[k];
		for (int i = 0; i < centroids.length; i++)
		{
			centroids[i] = new Point();
		}
		assert k <= nfile;
		for (int i = 0; i < k; i++)
		{
			centroids[i].truetype = i;
			int selectedline = new Random().nextInt(nPointPerFile[i]);
			FileSystem fs = FileSystem.get(new Configuration(true));
			FSDataInputStream fsDataInputStream = fs
					.open(new Path(pointFile[i]));
			InputStreamReader inputStreamReader = new InputStreamReader(
					fsDataInputStream);
			BufferedReader bufferedReader = new BufferedReader(
					inputStreamReader);
			String line = bufferedReader.readLine();
			int nline = 0;
			while (line != null)
			{
				if (line.isEmpty())
					continue;
				if (nline == selectedline)
				{
					centroids[i].vector = new Vector(line);
					break;
				}
				nline++;
				line = bufferedReader.readLine();
			}
			bufferedReader.close();
		}
		return centroids;
	}

	/**
	 * Write one centroid to file {CENTROID_FILE_PREFIX}+centroid.truetype
	 * 
	 * @param centroid
	 * @throws IOException
	 */
	public static void writeCentroidToFile(Point centroid) throws IOException
	{
		FileSystem fs = FileSystem.get(new Configuration(true));
		FSDataOutputStream fsDataOutputStream = fs.create(new Path(
				CENTROID_FILE_PREFIX + centroid.truetype), true);
		OutputStreamWriter writer = new OutputStreamWriter(fsDataOutputStream);
		for (int j = 0; j < centroid.vector.getLength(); j++)
		{
			// fsDataOutputStream.writeDouble(centroid.vector.get(j));
			// fsDataOutputStream.writeChar(' ');
			writer.write(Double.toString(centroid.vector.get(j)));
			writer.write(" ");
		}
		writer.close();
	}

	/**
	 * Get centroids from file {CENTROID_FILE_PREFIX}+0/1/2...
	 * 
	 * @param k
	 * @return point[] array of centroids
	 * @throws EmptyLineException
	 * @throws IOException
	 */
	public static Point[] getCentroidsFromFile(int k)
			throws EmptyLineException, IOException
	{
		// ArrayList<Point> centroids = new ArrayList<>(k);
		Point[] centroids = new Point[k];
		for (int i = 0; i < centroids.length; i++)
		{
			centroids[i] = new Point();
		}
		FileSystem fs = FileSystem.get(new Configuration(true));
		for (int i = 0; i < k; i++)
		{
			FSDataInputStream inputStream = fs.open(new Path(
					CENTROID_FILE_PREFIX + i));
			InputStreamReader inputStreamReader = new InputStreamReader(
					inputStream);
			BufferedReader reader = new BufferedReader(inputStreamReader);
			String line = reader.readLine();
			Vector vector = new Vector(line);
			Point centroid = new Point(Point.NONEXIST, i, vector);
			centroids[i] = centroid;
		}
		return centroids;
	}
}
