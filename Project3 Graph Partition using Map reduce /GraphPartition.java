
import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {

	public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth

    public Vertex() {
		id=0;
		adjacent=new Vector<Long>();
		centroid=0;
		depth=0;
	}
	public Vertex(long id, Vector<Long> adjacent, long centroid, short depth) {

		this.id = id;
		this.adjacent = adjacent;
		this.centroid = centroid;
		this.depth = depth;
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);

		//for(int i=0;i<adjacent.size();i++) {
		//	Long l=adjacent.elementAt(i);
		//	out.writeLong(l);
	//	}
		LongWritable size=new LongWritable(adjacent.size());
		size.write(out);
		for(Long adjacentVector: adjacent) {
			out.writeLong(adjacentVector);
			//System.out.println("adjacnet"+adjacentVector);
		}
	}
	public void readFields(DataInput in) throws IOException,EOFException {
		// TODO Auto-generated method stub
try{
	id=in.readLong();
	//	long l=in.readLong();
	//	adjacent.addElement(l);
	adjacent.clear();
		LongWritable size=new LongWritable();
		size.readFields(in);
		for(int i=0;i<size.get();i++) {
			LongWritable adjacentVector=new LongWritable();
			adjacentVector.readFields(in);
			adjacent.add(adjacentVector.get());

		}

		centroid=in.readLong();
		depth=in.readShort();
	} catch (EOFException e) {
   // ... this is fine
    } catch(IOException e) {
        // handle exception which is not expected
            e.printStackTrace();
           }

}
}
public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>(100);
    final static short max_depth = 8;
    static short BFS_depth = 0;

    public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex>{


    	@Override
    	protected void map(Object key, Text value, Mapper<Object, Text, LongWritable, Vertex>.Context context)
    			throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    	Vertex v=new Vertex();
	String line=value.toString();
    	String[] longParse=line.split(",");
    	 v.id=Long.parseLong(longParse[0]);
    //	Long centroid=(long) 0;
    	System.out.println("v.id"+v.id);
	for(int i=1;i<longParse.length;i++) {
    		Long parsedVertex=Long.parseLong(longParse[i]);
    		v.adjacent.addElement(parsedVertex);
    	System.out.println(v.adjacent)	;

}
	int count=0;
    	for(int i=0;i<v.adjacent.size();i++) {

    		if(i<10) {
    		count++;
		System.out.println(count);
		v.centroid=v.id;
    		}else {
    			v.centroid=-1;
    		}

    	}
        context.write(new LongWritable(v.id), new Vertex(v.id,v.adjacent,v.centroid,(short)0));


    	}

    }
    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex >{

    	public  void map(LongWritable key,Vertex vertex,Context context ) throws IOException, InterruptedException {
    		context.write(new LongWritable(vertex.id), vertex);
    	System.out.println("MApper2vertexid "+vertex.id);
		if(vertex.centroid>0) {

    			for(long n:vertex.adjacent) {
    				context.write(new LongWritable(n), new Vertex(n,null,vertex.centroid,BFS_depth));//check this

    			}
    		}
    	}
    }
    public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{

    	public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
    		short min_depth=1000;
		long a=key.get();
    		Vector<Long> nakliad=new Vector(100);
    		Vertex m=new Vertex(a,null,(long)-1,(short)0);
 	   	System.out.println("Reducer 2 key"+a);
    		for(Vertex v:values) {
    			if(v.adjacent!=null) {
    				m.adjacent=v.adjacent;
  				System.out.println("v.adjacent in reducer 2"+v.adjacent);
	}
    			if (v.centroid > 0 && v.depth < min_depth) {
    				min_depth = v.depth;
    				m.centroid = v.centroid;
    			}
    		m.depth = min_depth;
    		}
	System.out.println("In Reducer2 m /centroid "+m.centroid+"\n");
    		context.write(new LongWritable(a), m);

System.out.println("key in reducer 2"+key);
    	}
    }
    public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
    //	System.out.println("Mapper 3");
	@Override
	public void map(LongWritable key,Vertex values,Context context) throws IOException, InterruptedException {

		System.out.println("In map of mapper3");
    		context.write(new LongWritable(values.centroid),new LongWritable(1));
    		System.out.println("mapper 3 values.centroid"+values.centroid);
	}
    }
    public static class Reducer3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{

    	protected void reduce(LongWritable arg0, Iterable<LongWritable> arg1,Reducer<LongWritable, LongWritable, LongWritable,LongWritable>.Context arg2) throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    	//	System.out.println("arg1"+arg1.toString());
		LongWritable m=new LongWritable(0);
    		Long a =m.get();
    		for(LongWritable i:arg1) {

			a=a+i.get();
					}
    		arg2.write(arg0,new LongWritable(a));


    		}

    	}



    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);


        job.setMapperClass(Mapper1.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path( args[1]+"/i0"));



        /* ... First Map-Reduce job to read the graph */


        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);

            FileInputFormat.setInputPaths(job, new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/i"+(i+1)));



            /* ... Second Map-Reduce job to do BFS */
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.waitForCompletion(true);
    }
}
