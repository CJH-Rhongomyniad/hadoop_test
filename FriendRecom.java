import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
 
public class FriendRecom {
 
	static class ValuePair implements WritableComparable<ValuePair> {
 
		private String friend;
		private String union;//连结关系
 
		public String getFriend() {
			return friend;
		}
 
		public void setFriend(String friend) {
			this.friend = friend;
		}
 
		public String getUnion() {
			return union;
		}
 
		public void setUnion(String union) {
			this.union = union;
		}
 
		public ValuePair() {
			// TODO Auto-generated constructor stub
		}
 
		public ValuePair(String n, String u) {
			this.friend = n;
			this.union = u;
		}
 
		@Override
		public void readFields(DataInput in) throws IOException {
			this.friend = in.readUTF();
			this.union = in.readUTF();
		}
 
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.friend);
			out.writeUTF(this.union);
		}
 
		@Override
		public int compareTo(ValuePair o) {
			if (this.friend.equals(o.getFriend())) {
				return this.union.compareTo(o.getUnion());
			} else {
				return this.friend.compareTo(o.getFriend());
			}
		}
 
	}
 
	static class Map extends Mapper<LongWritable, Text, Text, ValuePair> {
 
		private Text outkey = new Text();
		private ValuePair outvalue = new ValuePair();
 
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = value.toString();
			String[] sz = input.split(" ");
			this.outkey.set(sz[0]);
			String[] sz1 = sz[1].split(",");
			for (int i = 0; i < sz1.length; i++) {
				this.outvalue.setFriend(sz1[i]);
				this.outvalue.setUnion("yes"); //yes表示key与outvalue.friend已经为好友关系
				context.write(this.outkey, this.outvalue);
			}
 
			for (int i = 0; i < sz1.length; i++) {
				for (int j = i + 1; j < sz1.length; j++) {
					this.outkey.set(sz1[i]);
					this.outvalue.setFriend(sz1[j]);
					this.outvalue.setUnion(sz[0]); //表示key与outvalue.friend共同的好友
					context.write(this.outkey, this.outvalue);
 
					this.outkey.set(sz1[j]);
					this.outvalue.setFriend(sz1[i]);
					context.write(this.outkey, this.outvalue);
				}
			}
		}
	}
 
	static class Reduce extends Reducer<Text, ValuePair, Text, Text> {
 
		private Text outKey = new Text();
		private Text outValue = new Text();
 
		@Override
		protected void reduce(Text key, Iterable<ValuePair> values,
				Context context) throws IOException, InterruptedException {
			this.outKey = key;
			Multimap<String, String> map = HashMultimap.create();
			for (ValuePair v : values) {
				//归约所有与key相关联的人及与key的关联关系
				map.put(v.getFriend(), v.getUnion()); 
//				String tmp = v.getFriend() + " " + v.getUnion();				
//				this.outValue.set(tmp);
//				context.write(this.outKey, this.outValue);
			}
 
			StringBuilder outString = new StringBuilder();//用来保存输出值
			Set<String> keys = map.keySet();//取得所有与key有关的人
			for(String s : keys){
				StringBuilder outStr = new StringBuilder();
				boolean isok = true;//标志是否与key是好友关系
				int acc = 0;
				Collection<String> v = map.get(s);//v表示与key的关系：yes或共同好友
				outStr.append(s + " " + "(");//s表示关联人
				String tmp = new String();//用来存放key和s的共同好友
				for(String u : v){
					if(u.equals("yes")){//表示key与s已经是好友关系了
						isok = false;
						break;//推出该循环，不推荐s
					}
					tmp += u + ", ";//记录key与s的共同好友
				}
				if(tmp.length()>2){//去除tmp结尾的”， “
					tmp = tmp.substring(0, tmp.length()-2);
				}
				if(isok){
					outStr.append(v.size() + ": " + "[" + tmp.trim() + "])");
					outString.append(outStr);
				}
			}
			
			outValue.set(outString.toString());
			context.write(outKey, outValue);
			
		}
 
	}
 
	private static String inputPath = "in-friend";
	private static String outputPath = "out-friend";
 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "friend recomment");
		job.setJarByClass(FriendRecom.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
 
		// job.setPartitionerClass(Prt.class);
		// job.setNumReduceTasks(6);
		// job.setGroupingComparatorClass(Grp.class);
 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ValuePair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = FileSystem.get(conf);
		Path inPath = new Path(inputPath);
		if (fs.exists(inPath)) {
			FileInputFormat.addInputPath(job, inPath);
		}
 
		Path outPath = new Path(outputPath);
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 
}