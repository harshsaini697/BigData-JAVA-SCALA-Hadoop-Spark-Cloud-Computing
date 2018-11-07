package project2;
import org.apache.hadoop.io.WritableComparable;
import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.Vector;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;



import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

	class Point implements WritableComparable<Point> {
		public double x;
		public double y;
		Point(){x=0;y=0;}

		Point ( double m, double n ) {
			x=m;y=n;
		}
		public void write(DataOutput out) throws IOException {

			out.writeDouble(x);
			out.writeDouble(y);
		}
		public void readFields(DataInput in) throws IOException {

			x = in.readDouble();
			y=  in.readDouble();
		}

		public int compareTo(Point p) {
			double xValue = this.x;
	        double xpoint = p.x;
	        double yvalue=this.y;
	        double ypoint=p.y;
	        if(xValue < xpoint)
	         { return -1;}
	       	if(yvalue  >ypoint)
	       	{	return 1;}
	        if (xValue>xpoint)
	      	 { return 1;}
	        if (yvalue< ypoint)
	        	{return -1;
		}
		return 0;
		}
		public String toString() {
			return this.x + "," + this.y + " ";
		}


	}
	class Avg implements Writable{
	public double sumX;
	public double sumY;
	public long count;
	Avg(){sumX=0;sumY=0;count=0;}
	Avg(double m,double n,long count){
		sumX=m;
		sumY=n;
		this.count=count;
	}
	public void write(DataOutput out) throws IOException {

	                out.writeDouble(sumX);
	                out.writeDouble(sumY);
			out.writeLong(count);
	        }
	        public void readFields(DataInput in) throws IOException {

	               sumX = in.readDouble();
	               sumY=  in.readDouble();
			count=in.readLong();
	        }


	}

	public class KMeans {
	static Vector<Point> centroids = new Vector<Point>(100);
	static Hashtable<Point,Avg> table;
		public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {
			Point point=new Point();
			public void setup (Context context)throws IOException, InterruptedException  {
			      URI[] paths = context.getCacheFiles();
				    Configuration conf = context.getConfiguration();
				    FileSystem fs = FileSystem.get(conf);
				    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
				    String s="";
				    s=reader.readLine();
				    while(s!=null) {
					      Scanner	scanner = new Scanner(s.toString());
					      Scanner s1 = scanner.useDelimiter(",");
					      Point point=new Point(s1.nextDouble(),s1.nextDouble());
					      centroids.add(point);
					      s=reader.readLine();

			}
	    table=new Hashtable<Point,Avg>();
		}
			@Override
			protected void cleanup ( Context context ) throws IOException,InterruptedException {
				Set<Point> keys=table.keySet();
				for (Point key: keys)
					context. write (key,table.get(key ));
				table.clear();
			}

			public double euclidean(Point p1,Point p2) {
				double distance =0;
				distance = Math.sqrt((p1.x-p2.x)*(p1.x-p2.x)+ (p1.y-p2.y)*(p1.y-p2.y));
				return distance;
			}
			@Override
			public void map (Object key,Text value, Context context ) throws NumberFormatException, IOException {
				String s=value.toString();
				String[] arr=s.split(",");
				Point point1 = new Point(Double.parseDouble(arr[0]),Double.parseDouble(arr[1]));
				Point point3= new Point();
			        double minimum=Double.MAX_VALUE;
				int count=0;
				for(Point point2:centroids) {
					count++;
					double temp=Math.sqrt((point1.x-point2.x)*(point1.x-point2.x)+((point1.y-point2.y)*(point1.y-point2.y)));
					if(temp<minimum) {
						 minimum=temp;
						 point3=point2;}}
					//	System.out.println("point3.x "+point3.x+"point3.y"+point3.y+"point1.x"+point1.x+"point1.y"+point1.y );
					if (table.get(point3) == null)
	              table.put(point3,new Avg(point1.x,point1.y,1) );
	        else table.put(point3,new Avg(table.get(point3).sumX+point1.x,table.get(point3).sumY+point1.y,table.get(point3).count+1 ));

	      }
	}






		public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {
			public void reduce(Point key,Iterable<Avg> values,Context context)throws IOException, InterruptedException{
	          double sx=0;double sy=0;long count=0;
	            //System.out.println("Values"+values);
				       //System.out.println("Key in reduce"+key);
				    for (Avg a: values) {
					      count+=a.count;
					          sx+=a.sumX;
					              sy+=a.sumY;
				//	System.out.println("Count:in reduce "+count);
				}
			//	System.out.println("Sx"+sx);
				     key.x=sx/count;
				     key.y=sy/count;
				try{
					   context.write(key, null);
				}
				     catch(Exception e){e.printStackTrace();}}
			}




		public static void main ( String[] args ) throws Exception {
			Job job = Job.getInstance();
			job.setJobName("KMeansJob");
			job.setJarByClass(KMeans.class);
			job.addCacheFile(new URI(args[1]));
			job.setOutputKeyClass(Point.class);
			job.setOutputValueClass(Object.class);
			job.setMapOutputKeyClass(Point.class);
			job.setMapOutputValueClass(Avg.class);
			job.setMapperClass(AvgMapper.class);
			job.setReducerClass(AvgReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));//(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[2]));
			job.waitForCompletion(true);

		}

	}


