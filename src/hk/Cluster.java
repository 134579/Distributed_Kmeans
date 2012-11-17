package hk;

import java.util.HashSet;
import java.util.Iterator;
import hk.util.Vector;

import hk.exception.CentroidNotExistException;
import hk.exception.VectorSizeDismatchException;

public class Cluster extends HashSet<Point>
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -575907867848170516L;
	public Point centroid = null;

	// the type of estimate points in the cluster
	private int type;
	
	
	public Cluster()
	{
		super();
	}

	public Point getCentroid()
	{
		return centroid;
	}

	public double getDistance(Point p) throws VectorSizeDismatchException,
			CentroidNotExistException
	{
		if (centroid != null)
			return p.getDistance(centroid);
		else
			throw new CentroidNotExistException();
	}

//	public void updateCentroid() throws VectorSizeDismatchException
//	{
//		if (isEmpty())
//			centroid = null;
//		Vector v = new Vector(iterator().next().vector.getLength());
//		for (Iterator<Point> iterator = iterator(); iterator.hasNext();)
//		{
//			Point nextpoint = iterator.next();
//			v = v.add(nextpoint.vector.multiply(nextpoint.vector));
//		}
//		v = v.div(size());
//		for (int i = 0; i < v.getLength(); i++)
//		{
//			v.set(i,Math.sqrt(v.get(i)));
//		}
//		// centroid.vector = v;
//		centroid = new Point(Point.NONEXIST, Point.NONEXIST, v);
//	}
	
	public void updateCentroid() throws VectorSizeDismatchException
	{
		if (isEmpty())
			centroid = null;
		Vector v = new Vector(iterator().next().vector.getLength());
		for (Iterator<Point> iterator = iterator(); iterator.hasNext();)
		{
			Point nextpoint = iterator.next();
			v = v.add(nextpoint.vector);
		}
		v = v.div(size());
		// centroid.vector = v;
		centroid = new Point(Point.NONEXIST, Point.NONEXIST, v);
	}
	/**
	 * @return type
	 */
	public int getType()
	{
		return type;
	}

	/**
	 * @param type 要设置的 type
	 */
	public void setType(int type)
	{
		this.type = type;
	}
}
