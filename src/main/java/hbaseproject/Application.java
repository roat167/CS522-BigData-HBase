package hbaseproject;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Application {
	private static final String TABLE_NAME = "Employee";
	
	public static class MyMapper extends TableMapper<Text, IntWritable>  {  	

	   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
	   		//take column family, qualifier
	        String val = new String(value.getValue(Bytes.toBytes("position"), Bytes.toBytes("pos")));		   			
	        context.write(new Text(val), new IntWritable(1));
	   	}
	}
	
	 public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				int i = 0;
				for (IntWritable val : values) {
					i += val.get();
				}
				context.write(key, new IntWritable(i));
			}
		}
		    
	 
	 private static void addData(Configuration conf, TableName tablename)  {
		 try {
		 	//add record emp001
			//TableName tableName, String rowKey, String family, String qualifier, String value, Configuration conf
			HBaseUtil.addRecord(tablename, "emp1", "contact", "email", "test1@gmail.com", conf);
			HBaseUtil.addRecord(tablename, "emp1", "contact", "phone", "515 441 0000", conf);
			HBaseUtil.addRecord(tablename, "emp1", "contact", "home", "Fairfield IA", conf);
			HBaseUtil.addRecord(tablename, "emp1", "position", "pos", "Manager", conf);
			HBaseUtil.addRecord(tablename, "emp1", "slr", "", "85000", conf);
			
			// add record emp002
			HBaseUtil.addRecord(tablename, "emp2", "contact", "email", "test2@gmail.com", conf);
			HBaseUtil.addRecord(tablename, "emp2", "contact", "phone", "614 441 0000", conf);
			HBaseUtil.addRecord(tablename, "emp2", "contact", "home", "Fairfield IA", conf);
			HBaseUtil.addRecord(tablename, "emp2", "position", "pos", "Developer", conf);
			HBaseUtil.addRecord(tablename, "emp2", "contact", "home", "Fairfield IA", conf);
			HBaseUtil.addRecord(tablename, "emp2", "slr", "", "65000", conf);
			
			//add record emp003
			HBaseUtil.addRecord(tablename, "emp3", "contact", "email", "test3@gmail.com", conf);
			HBaseUtil.addRecord(tablename, "emp3", "contact", "phone", "614 555 0000", conf);
			HBaseUtil.addRecord(tablename, "emp3", "contact", "home", "Washington IA", conf);
			HBaseUtil.addRecord(tablename, "emp3", "position", "pos", "Developer", conf);
			HBaseUtil.addRecord(tablename, "emp3", "slr", "", "70000", conf);
			
			//add record emp004
			HBaseUtil.addRecord(tablename, "emp4", "contact", "email", "test4@gmail.com", conf);
			HBaseUtil.addRecord(tablename, "emp4", "contact", "phone", "614 555 0000", conf);
			HBaseUtil.addRecord(tablename, "emp4", "contact", "home", "Fairfield IA", conf);
			HBaseUtil.addRecord(tablename, "emp4", "position", "pos", "Designer", conf);
			HBaseUtil.addRecord(tablename, "emp4", "slr", "", "60000", conf);
			
			//add record emp005
			HBaseUtil.addRecord(tablename, "emp5", "contact", "email", "test5@gmail.com", conf);
			HBaseUtil.addRecord(tablename, "emp5", "contact", "phone", "614 555 0000", conf);
			HBaseUtil.addRecord(tablename, "emp5", "position", "pos", "Designer", conf);
			HBaseUtil.addRecord(tablename, "emp5", "slr", "", "550000", conf);
		 } catch (Exception e) {	
			 e.printStackTrace();
		 }

	 }
	    
	public static void main(String[] args) throws Exception {	
		try {
			Configuration conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.property.clientport", "2181");
	        	        
			TableName tablename = TableName.valueOf(TABLE_NAME.getBytes());
			String[] familys = { "contact", "position", "slr" };			
			
			HBaseUtil.creatTable(tablename, familys, conf);

			//generate Data
			addData(conf, tablename);
			
			HBaseUtil.getRecord(tablename, "emp1", conf);
			HBaseUtil.getAllRecord(tablename, conf);

			HBaseUtil.delRecord(tablename, "emp2", conf);
			HBaseUtil.getAllRecord(tablename, conf);

			System.out.println("===List current records:=== ");
			HBaseUtil.getAllRecord(tablename, conf);
			
		
			Job job = new Job(conf, "Summary");
			job.setJarByClass(Application.class);     // class that contains mapper and reducer

			Scan scan = new Scan();
			scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs

			TableMapReduceUtil.initTableMapperJob(
				tablename,        // input table
				scan,               // Scan instance to control CF and attribute selection
				MyMapper.class,     // mapper class
				Text.class,         // mapper output key
				IntWritable.class,  // mapper output value
				job);
			job.setReducerClass(MyReducer.class); 
			job.setNumReduceTasks(1);				
			
			FileOutputFormat.setOutputPath(job, new Path("outputhbase"));  

			System.exit(job.waitForCompletion(true) ? 0 : 1);
       } catch (Exception e) {
            e.printStackTrace();
        }
		
	}	
}
