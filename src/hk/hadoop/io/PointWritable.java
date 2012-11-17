package hk.hadoop.io;

import hk.Point;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import hk.util.Vector;

public class PointWritable extends Point implements Writable
{
	public PointWritable()
	{
	}
	
	public PointWritable(int index, int truetype, Vector vector)
	{
		super(index, truetype, vector);
		// TODO auto generated
	}

	/*
	 * （非 Javadoc）
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 * 2.write the index of point
	 * 2.write the true type of point
	 * 3.write the assigned type of point
	 * 4.write the length of the vector
	 * 5.write the content of the vector
	 */
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO auto generated
		out.writeInt(index);
		out.writeInt(truetype);
		out.writeInt(assigntype);
		out.writeInt(vector.getLength());
		int length=vector.getLength();
		for (int i = 0; i < length; i++)
		{
			out.writeDouble(vector.get(i));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO auto generated
		index=in.readInt();
		truetype=in.readInt();
		assigntype=in.readInt();
		int length=in.readInt();
		vector=new Vector(length);
		for (int i = 0; i < length; i++)
		{
			vector.set(i, in.readDouble());
		}
	}
}
