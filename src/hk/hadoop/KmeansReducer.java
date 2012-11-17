package hk.hadoop;

import hk.hadoop.io.PointWritable;
import hk.Point;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import hk.exception.VectorSizeDismatchException;

import hk.util.Vector;

public class KmeansReducer extends MapReduceBase implements
		Reducer<IntWritable, PointWritable, IntWritable, PointWritable>
{
	Vector centroidvector = null;

	/**
	 * Re-calculate centroid of a cluster
	 */
	@Override
	public void reduce(IntWritable key, Iterator<PointWritable> values,
			OutputCollector<IntWritable, PointWritable> output,
			Reporter reporter) throws IOException
	{
		if (!values.hasNext())
			return;
		int type = key.get();
		Vector vector = null;
		int n = 0;
		while (values.hasNext())
		{
			PointWritable pointWritable = (PointWritable) values.next();
			n++;
			if (vector == null)
				vector = new Vector(pointWritable.vector.getLength());
			try
			{
				vector = vector.add(pointWritable.vector);
			}
			catch (VectorSizeDismatchException e)
			{
				// TODO 自动生成的 catch 块
				e.printStackTrace();
				return;
			}
		}
		vector = vector.div(n);
		Point centroid = new Point(Point.NONEXIST, type, vector);
		// writeCentroidToFile(new Path("centroid" + type));
		Kmeans.writeCentroidToFile(centroid);
	}
}
