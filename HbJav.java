


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;



public class HbJav {

	static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	   private long ts;
	   static byte[] family = Bytes.toBytes("readings");
	   
	   @Override
	   protected void setup(Context context) 
     {
	   	ts = System.currentTimeMillis();
	   }

		@Override
		public void map(LongWritable offset, Text value, Context context) throws IOException {
			try {
				System.out.println("Loading table values...");
				System.out.println("key: " +offset+ " value: "+ value);
				
        byte[] bRowKey = Bytes.toBytes(value.toString().split(",")[0]);
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(bRowKey);
				Put p = new Put(bRowKey);
				p.add(Bytes.toBytes("details"), Bytes.toBytes("name"), ts, Bytes.toBytes(value.toString().split(",")[1]));
				System.out.println("2) "+ value.toString().split(",")[1]);
				p.add(Bytes.toBytes("details"), Bytes.toBytes("location"), ts, Bytes.toBytes(value.toString().split(",")[2]));
				System.out.println("3) "+ value.toString().split(",")[2]);
				p.add(Bytes.toBytes("details"), Bytes.toBytes("age"), ts, Bytes.toBytes(value.toString().split(",")[3]));
				System.out.println("4) "+ value.toString().split(",")[3]);
				context.write(rowKey, p);
				
				// Output of every Mapper will be sorted on rowKey by the framework
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	   
		Configuration conf = HBaseConfiguration.create();
		String tableName = "customer";
		Path inputDir = new Path("/home/acadgild/customers.dat");
		Job job = new Job(conf, "bulk_load_mapreduce"); 
		
		job.setJarByClass(ImportMapper.class);
		FileInputFormat.setInputPaths(job, inputDir);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImportMapper.class);
	
		@SuppressWarnings("deprecation")
		HTableDescriptor ht = new HTableDescriptor(tableName);
		ht.addFamily( new HColumnDescriptor ("details"));
		HBaseAdmin hba = new HBaseAdmin (conf);
		
		if (hba.tableExists(tableName))
		{	
			System.out.println("Table: " + tableName + " already exists");
			System.out.println("Deleting Table: " + tableName + "...");
			hba.disableTable(tableName);
			hba.deleteTable(tableName);
			System.out.println("Recreating Table: " + tableName + "...");
			hba.createTable(ht);
			
		}
		else 
		{
			System.out.println("Creating Table: " + tableName + "...");
			hba.createTable(ht);
		}
	
	
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			// above will set TableOutputFormat as the output format
			
			job.setNumReduceTasks(0);
		
		
		TableMapReduceUtil.addDependencyJars(job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		hba.close();
	}
}
