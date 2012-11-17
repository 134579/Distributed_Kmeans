package hk;

import hk.util.Vector;
import hk.exception.*;

public class Point
{
	// ith point 
	public int index=NONEXIST;
	
	public static final int NONEXIST = Integer.MIN_VALUE;
	// just used for evaluation
	public int truetype=NONEXIST;
	// belong to this cluster
	private Cluster cluster=null;
	
	public int assigntype=NONEXIST;
	
	public Vector vector=null;

	public Point(int index, int truetype,Vector vector)
	{
		this.index = index;
		this.truetype = truetype;
		this.vector = vector;
	}
	
	public Point()
	{
		// TODO auto generated
	}

	public double getDistance(Point item) throws VectorSizeDismatchException
	{
		Vector delta=vector.sub(item.vector);
		return delta.square().sum();
	}

	public static Point getMean(Point[] points) throws VectorSizeDismatchException
	{
		int n = points.length;
		if (n == 0) return null;
		Vector v=new Vector(points[0].vector.getLength());
		for (int i = 0; i < n; i++)
		{
			v=v.add(points[i].vector);
		}
		v.div(n);
		return new Point(NONEXIST,NONEXIST,v);
	}

	/**
	 * @return cluster
	 */
	public Cluster getCluster()
	{
		return cluster;
	}

	/**
	 * @param cluster 要设置的 cluster
	 */
	public void setCluster(Cluster cluster)
	{
		this.cluster = cluster;
	}
	
	/**
	 * Compare two group of points 
	 * @param oldcentroids
	 * @param newcentroids
	 * @return
	 */
	public static boolean equals(Point[] oldcentroids, Point[] newcentroids)
	{
		if (oldcentroids == null && newcentroids != null)
			return false;
		if (oldcentroids != null && newcentroids == null)
			return false;
		if (oldcentroids.length != newcentroids.length)
			return false;
		for (int i = 0; i < newcentroids.length; i++)
		{
			if (!oldcentroids[i].vector.equals(newcentroids[i].vector))
				return false;
		}
		return true;
	}
}
