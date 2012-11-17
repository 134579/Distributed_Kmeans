/**
 * 
 */
package hk.util;

import hk.exception.EmptyLineException;
import hk.exception.VectorSizeDismatchException;

/**
 * @author huangkai
 * @param <E>
 * 
 */
public class Vector /* extends ArrayList<Double> */
{
	/**
	 * 
	 */
	private int length;

	public int getLength()
	{
		return length;
	}

	private double[] data;

	public Vector(int length)
	{
		if (length <= 0)
			throw new IllegalArgumentException("invalid capacity: " + length);
		this.length = length;
		data = new double[length];
	}

	public Vector(double[] v)
	{
		this.length = v.length;
		data = new double[length];
		System.arraycopy(v, 0, data, 0, length);
	}

	// string with \\s splited
	public Vector(String s) throws EmptyLineException
	{
		String[] strings = s.split("\\s+");
		if (strings.length == 0)
			throw new EmptyLineException("empty line found in: " + s);
		double[] v = new double[strings.length];
		for (int i = 0; i < v.length; i++)
		{
			v[i] = Double.parseDouble(strings[i]);
		}
		data = v;
		length = data.length;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o instanceof Vector)
		{
			Vector v = (Vector) o;
			if (data == v.data)
				return true;
			if (length != v.length)
				return false;
			for (int i = 0; i < data.length; i++)
			{
				if (data[i] != v.data[i])
					return false;
			}
			return true;
		}
		return false;
	}

	public double get(int i)
	{
		if (i >= length)
			throw new IndexOutOfBoundsException();
		else
			return data[i];
	}

	public void set(int i, double value)
	{
		if (i >= length)
			throw new IndexOutOfBoundsException();
		else
			data[i] = value;
	}

	public Vector add(Vector v) throws VectorSizeDismatchException
	{
		if (length != v.getLength())
			throw new VectorSizeDismatchException();
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] + v.get(i));
		return r;
	}

	public Vector sub(Vector v) throws VectorSizeDismatchException
	{
		if (length != v.getLength())
			throw new VectorSizeDismatchException();
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] - v.get(i));
		return r;
	}

	public Vector multiply(Vector v) throws VectorSizeDismatchException
	{
		if (length != v.getLength())
			throw new VectorSizeDismatchException();
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] * v.get(i));
		return r;
	}

	public Vector multiply(double multiplier)
	{
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] * multiplier);
		return r;
	}

	public Vector div(double divisor)
	{
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] / divisor);
		return r;
	}

	public Vector square()
	{
		if(length==0)
			return null;
		Vector r = new Vector(length);
		for (int i = 0; i < length; i++)
			r.set(i, data[i] * data[i]);
		return r;
	}

	public double sum()
	{
		double sum = 0;
		for (int i = 0; i < length; i++)
			sum += data[i];
		return sum;
	}
}
