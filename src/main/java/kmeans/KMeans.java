package kmeans;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.ArrayRealVector
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {
    

    public static class KMeansMapper extends Mapper<LongWritable,Text,IntWritable, DataPoint>{
        final int EUCLIDEAN = 0;
        final int MANHATTAN = 1;
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            DataPoint data = new DataPoint(value.toString());
            int k = conf.getInt("k" , 0); // k centroids
            int distanceType = conf.getInt("distance type"); // 0 for euclidean, 1 for manhattan
            double cost = -1;
            int index = -1;
            for(int i = 0; i < k; i++){
                DataPoint centroid = new DataPoint(conf.get("centroid" + k));
                if(distanceType = EUCLIDEAN){
                    double tmp = centroid.getL2Distance(data);
                    if(cost < tmp){
                        cost = tmp;
                        index = i;
                    }
                } 
                else{
                    cost = centroid.getL1Distance(data);
                    if(cost < tmp){
                        cost = tmp;
                        index = i;
                    }
                }
                context.write(new IntWritable(index),data);
                context.write(new IntWritable(-1),new DataPoint(cost));
            }    
        }
    }


    public class KMeansReducer extends Reducer<IntWritable,DataPoint,NullWritable, DataPoint>{
        public void reduce(IntWritable key, Iterable<DataPoint> values,Context context)throws IOException, InterruptedException{
            if (key.get() == -1){
                //cost
                double cost = 0;
                for(DataPoint p: values){
                    cost += p.getCost();
                    Configuration conf = context.getConfiguration();
                    int itrNumber = conf.getInt("itrNumber", 0);
                    conf.setDouble("cost" + itrTimes, cost);
                }
            }
            else{
                Iterator<DataPoint> iterator = values.iterator();
                ArrayRealVector init = new ArrayRealVector(iterator.next().getDimension(),0.0);
                DataPoint newCentroid = new DataPoint(init);
                int count = 0
                for(DataPoint p: values){
                    newCentroid.add(p.getPoint());
                    count++;
                }
                newCentroid.mapDivideToSelf(count);
            }
            context.write(NullWritable.get(),newCentroid);
        }
    }

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 6) {
            System.err.println("Usage: K-Means <in> <out> <init> <distance type> <k> <MaxIter>");
            System.exit(4);
        }
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];        
        String initPath = otherArgs[2];
        conf.set("distance type", Integer.valueOf(otherArgs[3])); // 0 for euclidean, 1 for manhattan
        conf.setInt("k", Integer.valueOf(otherArgs[4]));
        int maxIter =  Integer.valueOf(otherArgs[5]);
        int i;
        FileSystem f =FileSystem.get(conf);
        for(i = 0; i <= maxIter; i++){
            f.delete(new Path(otherArgs[1]+"/out"+(i+1)),true);
            f.delete(new Path(otherArgs[1]+"/util"+i),true);

            Job rankJob = new Job(conf, "K-Means");
	        rankJob.setJarByClass(KMeans.class);
	        rankJob.setMapperClass(KMeansMapper.class);
	        rankJob.setReducerClass(KMeansReducer.class);

	        rankJob.setMapOutputKeyClass(IntWritable.class);
	        rankJob.setMapOutputValueClass(DataPoint.class);
	        rankJob.setOutputKeyClass(NullWritable.class);
	        rankJob.setOutputValueClass(DataPoint.class);
            
	        FileInputFormat.addInputPath(rankJob, new Path(inputPath));
	        FileOutputFormat.setOutputPath(rankJob, new Path(outputPath+"/out"+i));
	        rankJob.waitForCompletion(true);
            initCentroids(conf,new Path(outputPath+"/out"+i));
        }

    }
  
    public static void initCentroids(Configuration conf,Path path)
        throws FileNotFoundException, IOException, UnsupportedEncodingException {
       
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)){
            FSDataInputStream fsStream = fs.open(path);
            BufferedReader in = new BufferedReader(new InputStreamReader(fsStream,"UTF-8"));
            String line;
            int k = 0;
            while((line = in.readLine()) != null ){
               conf.set("centroid" + k , line);
               k++;
            }
            in.close();
        }
    }
	
}
