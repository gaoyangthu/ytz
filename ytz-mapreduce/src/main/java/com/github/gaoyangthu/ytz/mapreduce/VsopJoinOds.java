package com.github.gaoyangthu.ytz.mapreduce;

import com.github.gaoyangthu.ytz.security.hash.MobileNbrHash;
import com.github.gaoyangthu.ytz.security.tripledes.Decryptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/12/25 0025
 */
public class VsopJoinOds {
	/* Decryptor key */
	private static final String DE_KEY = "asiainfo3Des";

	/* Any future time is less than MAX_TIME  */
	private static long MAX_TIME = 20200000000000L;

	/**
	 * Generate the user-readable state by original state
	 *
	 * ODS data includes eight original statuses
	 * Mapped statuses returned to users seem to be 1, 0 and -1.
	 * Their mapping relations are as blow:
	 * 100000 <------> 1
	 * 110000 <------> 0
	 * 120000 <------> 1
	 * 130000 <------> 1
	 * 140000 <------> 1
	 * 119999 <------> 0
	 * 999999 <------> 1
	 * -1     <------> 1
	 *
	 * VSOP data includes six original statuses.
	 * Mapped statuses returned to users seem to be 1, 0.
	 * Their mapping relations are as blow:
	 * 1001   <------> 1
	 * 1101   <------> 1
	 * 1102   <------> 0
	 * 1201   <------> 1
	 * 1203   <------> 1
	 * 1204   <------> 1
	 *
	 * Anything else will be mapped to -1.
	 *
	 * @param state
	 * @return user-readable state
	 */
	public static String getUserState(String state) {
		if (state == null || state.length() == 0) {
			return "-1";
		} else {
			if (state.equals("100000") || state.equals("120000") || state.equals("130000")
				|| state.equals("140000") || state.equals("1001") || state.equals("1101")
				|| state.equals("1201") || state.equals("1203") || state.equals("1204")) {
				return "1";
			} else if (state.equals("110000") || state.equals("119999") || state.equals("1102")) {
				return "0";
			} else {
				return "-1";
			}
		}
	}

	/**
	 * The value of ods or vsop record
	 */
	public static class Record implements Writable {
		private String prdState;
		private long openDate;
		private long uninstallDate;
		private long modifyDate;
		private int flag;

		public Record() {
		}

		public Record(String prdState, long openDate, long uninstallDate, long modifyDate, int flag) {
			this.prdState = prdState;
			this.openDate = openDate;
			this.uninstallDate = uninstallDate;
			this.modifyDate = modifyDate;
			this.flag = flag;
		}

		public String getPrdState() {
			return prdState;
		}

		public void setPrdState(String prdState) {
			this.prdState = prdState;
		}

		public long getOpenDate() {
			return openDate;
		}

		public void setOpenDate(long openDate) {
			this.openDate = openDate;
		}

		public long getUninstallDate() {
			return uninstallDate;
		}

		public void setUninstallDate(long uninstallDate) {
			this.uninstallDate = uninstallDate;
		}

		public long getModifyDate() {
			return modifyDate;
		}

		public void setModifyDate(long modifyDate) {
			this.modifyDate = modifyDate;
		}

		public int getFlag() {
			return flag;
		}

		public void setFlag(int flag) {
			this.flag = flag;
		}

		public void set(String prdState, long openDate, long uninstallDate, long modifyDate, int flag) {
			this.prdState = prdState;
			this.openDate = openDate;
			this.uninstallDate = uninstallDate;
			this.modifyDate = modifyDate;
			this.flag = flag;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, prdState);
			out.writeLong(openDate);
			out.writeLong(uninstallDate);
			out.writeLong(modifyDate);
			out.writeInt(flag);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			prdState = Text.readString(in);
			openDate = in.readLong();
			uninstallDate = in.readLong();
			modifyDate = in.readLong();
			flag = in.readInt();
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(prdState).append("\t");
			sb.append(openDate).append("\t");
			sb.append(uninstallDate).append("\t");
			sb.append(modifyDate).append("\t");
			sb.append(flag);
			return sb.toString();
		}
	}

	/**
	 * Reducer output object, also the final result
	 */
	public static class Result implements WritableComparable<Result> {
		private long accsNbr;
		private String prdState;
		private long openDate;
		private long uninstallDate;

		public Result() {
		}

		public Result(long accsNbr, String prdState, long openDate, long uninstallDate) {
			this.accsNbr = accsNbr;
			this.prdState = prdState;
			this.openDate = openDate;
			this.uninstallDate = uninstallDate;
		}

		public long getAccsNbr() {
			return accsNbr;
		}

		public void setAccsNbr(long accsNbr) {
			this.accsNbr = accsNbr;
		}

		public String getPrdState() {
			return prdState;
		}

		public void setPrdState(String prdState) {
			this.prdState = prdState;
		}

		public long getOpenDate() {
			return openDate;
		}

		public void setOpenDate(long openDate) {
			this.openDate = openDate;
		}

		public long getUninstallDate() {
			return uninstallDate;
		}

		public void setUninstallDate(long uninstallDate) {
			this.uninstallDate = uninstallDate;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(accsNbr);
			Text.writeString(out, prdState);
			out.writeLong(openDate);
			out.writeLong(uninstallDate);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			accsNbr = in.readLong();
			prdState = Text.readString(in);
			openDate = in.readLong();
			uninstallDate = in.readLong();
		}

		@Override
		public int compareTo(Result o) {
			if (accsNbr != o.accsNbr) {
				return accsNbr < o.accsNbr ? -1 : 1;
			} else {
				return 0;
			}
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(accsNbr).append("\t");
			sb.append(prdState).append("\t");
			sb.append(openDate).append("\t");
			sb.append(uninstallDate);
			return sb.toString();
		}
	}

	/**
	 * The class for partition method
	 */
//	public static class KeyPartitioner extends Partitioner<LongWritable, Record> {
//		@Override
//		public int getPartition(LongWritable key, Record value, int numPartitions) {
//			return Math.abs((int)key.get()%numPartitions);
//		}
//	}

	/**
	 * Comparator
	 */
//	public static class GroupingComparator extends WritableComparator {
//		protected GroupingComparator() {
//			super(LongWritable.class, true);
//		}
//
//		@Override
//		public int compare(WritableComparable w1, WritableComparable w2) {
//			LongWritable l = (LongWritable) w1;
//			LongWritable r = (LongWritable) w2;
//			if (l.get() == r.get()) {
//				return 0;
//			} else {
//				return l.get() < r.get() ? -1 : 1;
//			}
//		}
//	}

	/**
	 * Pre-process ods history data
	 */
	public static class OdsHistoryMapper extends Mapper<Object, Text, LongWritable, Record> {
		/**
		 * Map phase
		 *
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * Parse ods record
			 */
			String[] items = value.toString().split("\t", -1);
			Record record = new Record();
			try {
				String nbrStr = Decryptor.getDecryptedString(DE_KEY, items[0]);
				Long nbr = Long.parseLong(nbrStr);
				/**
				 * Filter mobile phone number
				 */
				if (nbr > 13000000000L && nbr < 19000000000L) {
					record.setPrdState(items[1]);
					/**
					 * If a field is empty, fill it with zero
					 */
					if (items[2] == null || items[2].length() == 0) {
						record.setOpenDate(0L);
					} else {
						record.setOpenDate(Long.parseLong(items[2]));
					}
					if (items[3] == null || items[3].length() == 0) {
						record.setUninstallDate(0L);
					} else {
						record.setUninstallDate(Long.parseLong(items[3]));
					}
					if (items[4] == null || items[4].length() == 0) {
						record.setModifyDate(0L);
					} else {
						record.setModifyDate(Long.parseLong(items[4]));
					}
					/**
					 * The flag of ods data is 1
					 */
					record.setFlag(1);
					context.write(new LongWritable(nbr), record);
				}
			} catch (NumberFormatException e) {
				// if nbr is not a mobile phone number, skip this record
			}
		}
	}

	/**
	 * Pre-process vsop history data
	 */
	public static class VsopHistoryMapper extends Mapper<Object, Text, LongWritable, Record> {
		/**
		 * Map phase
		 *
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * Parse ods record
			 */
			String[] items = value.toString().split("\t", -1);
			Record record = new Record();
			try {
				String nbrStr = Decryptor.getDecryptedString(DE_KEY, items[0]);
				Long nbr = Long.parseLong(nbrStr);
				/**
				 * Filter mobile phone number
				 */
				if (nbr > 13000000000L && nbr < 19000000000L) {
					record.setPrdState(items[1]);
					if (items[2] != null && items[2].length() != 0) {
						record.setModifyDate(Long.parseLong(items[2]));
						/**
						 * 1001 means the mobile phone number is given to a new user
						 * 1102 means the old user will not use this number any more
						 */
						if (record.getPrdState().equals("1001")) {
							record.setOpenDate(record.getModifyDate());
							record.setUninstallDate(0L);
						} else if (record.getPrdState().equals("1102")) {
							record.setOpenDate(0L);
							record.setUninstallDate(record.getModifyDate());
						} else {
							record.setOpenDate(0L);
							record.setUninstallDate(0L);
						}
						/**
						 * The flag of vsop history data is 2
						 */
						record.setFlag(2);
						context.write(new LongWritable(nbr), record);
					}
				}
			} catch (NumberFormatException e) {
				// if nbr is not a mobile phone number, skip this record
			}
		}
	}

	/**
	 * Pre-process vsop today data
	 */
	public static class VsopTodayMapper extends Mapper<Object, Text, LongWritable, Record> {
		/**
		 * Map phase
		 *
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/**
			 * Parse ods record
			 */
			String[] items = value.toString().split("\t", -1);
			Record record = new Record();
			try {
				String nbrStr = Decryptor.getDecryptedString(DE_KEY, items[0]);
				Long nbr = Long.parseLong(nbrStr);
				/**
				 * Filter mobile phone number
				 */
				if (nbr > 13000000000L && nbr < 19000000000L) {
					record.setPrdState(items[1]);
					if (items[2] != null && items[2].length() != 0) {
						record.setModifyDate(Long.parseLong(items[2]));
						/**
						 * 1001 means the mobile phone number is given to a new user
						 * 1102 means the old user will not use this number any more
						 */
						if (record.getPrdState().equals("1001")) {
							record.setOpenDate(record.getModifyDate());
							record.setUninstallDate(0L);
						} else if (record.getPrdState().equals("1102")) {
							record.setOpenDate(0L);
							record.setUninstallDate(record.getModifyDate());
						} else {
							record.setOpenDate(0L);
							record.setUninstallDate(0L);
						}
						/**
						 * The flag of vsop latest data is 3
						 */
						record.setFlag(3);
						context.write(new LongWritable(nbr), record);
					}
				}
			} catch (NumberFormatException e) {
				// if nbr is not a mobile phone number, skip this record
			}
		}
	}

	/**
	 * Implement the join process between vsop and ods
	 */
	public static class ReduceClass extends Reducer<LongWritable, Record, Result, NullWritable> {
		private static final NullWritable n = NullWritable.get();

		private Record initRecord(Record r, String s, int f) {
			r.setPrdState(s);
			r.setOpenDate(-1L);
			r.setUninstallDate(-1L);
			r.setModifyDate(-1L);
			r.setFlag(f);
			return r;
		}

		private long getReasonableTime(long time) {
			if (time < MAX_TIME) {
				return time;
			} else {
				return 0L;
			}
		}

		private Record getLatestOds(List<Record> list) {
			/* The latest ods record */
			Record latest = new Record();
			/* The latest in used ods record */
			Record latestInused = new Record();
			/* The latest unused ods record */
			Record latestUnused = new Record();
			/* The latest unknown ods record */
			Record latestUnknown = new Record();

			/* Initialize records above */
			initRecord(latest, "-1", 1);
			initRecord(latestInused, "-1", 1);
			initRecord(latestUnused, "-1", 1);
			initRecord(latestUnknown, "-1", 1);

			/* Get the latest in used, unused and unknown record */
			for (Record record: list) {
				String state = record.getPrdState();
				long oDate = getReasonableTime(record.getOpenDate());
				long uDate = getReasonableTime(record.getUninstallDate());
				long mDate = getReasonableTime(record.getModifyDate());
				if (getUserState(state).equals("1")) {
					if ((mDate > 0L && mDate > latestInused.getModifyDate())
						|| (mDate == 0L && oDate > latestInused.getOpenDate())) {
						latestInused.setPrdState(state);
						latestInused.setOpenDate(oDate);
						latestInused.setUninstallDate(uDate);
						latestInused.setModifyDate(mDate);
					}
				} else if (getUserState(state).equals("0")) {
					long vMax = Math.max(oDate, Math.max(uDate, mDate));
					long lMax = Math.max(latestUnused.getOpenDate(),
						Math.max(latestUnused.getUninstallDate(), latestUnused.getModifyDate()));
					if (vMax > lMax) {
						latestUnused.setPrdState(state);
						latestUnused.setOpenDate(oDate);
						latestUnused.setUninstallDate(uDate);
						latestUnused.setModifyDate(mDate);
					}
				} else {
					if ((mDate > 0L && mDate > latestUnknown.getModifyDate())
						|| (mDate == 0L && oDate > latestUnknown.getOpenDate())) {
						latestUnknown.setPrdState(state);
						latestUnknown.setOpenDate(oDate);
						latestUnknown.setUninstallDate(uDate);
						latestUnknown.setModifyDate(mDate);
					}
				}
			}
			
			/* Get the latest ods record */
			if (latestInused.getModifyDate() >= latestUnknown.getModifyDate()
				|| latestInused.getOpenDate() >= latestUnknown.getOpenDate()) {
				latest.setPrdState(latestInused.getPrdState());
				latest.setOpenDate(latestInused.getOpenDate());
				latest.setUninstallDate(latestInused.getUninstallDate());
				latest.setModifyDate(latestInused.getModifyDate());
			} else {
				latest.setPrdState(latestUnknown.getPrdState());
				latest.setOpenDate(latestUnknown.getOpenDate());
				latest.setUninstallDate(latestUnknown.getUninstallDate());
				latest.setModifyDate(latestUnknown.getModifyDate());
			}
			if (latestUnused.getModifyDate() != -1L) {
				if (latest.getModifyDate() == -1L || (latestUnused.getModifyDate() >= latest.getModifyDate()
					&& latestUnused.getOpenDate() >= latest.getOpenDate())) {
					latest.setPrdState(latestUnused.getPrdState());
					latest.setOpenDate(latestUnused.getOpenDate());
					latest.setUninstallDate(latestUnused.getUninstallDate());
					latest.setModifyDate(latestUnused.getModifyDate());
				} else {
					latest.setUninstallDate(latestUnused.getUninstallDate());
				}
			}

			return latest;
		}

		private Record getLatestVsop(List<Record> list) {
			/* The latest vsop record */
			Record latest = new Record();
			initRecord(latest, "-1", 2);

			/* Get the latest vsop record */
			for (Record record: list) {
				if (record.getModifyDate() > latest.getModifyDate()) {
					latest.setPrdState(record.getPrdState());
					latest.setModifyDate(record.getModifyDate());
				}
				if (record.getOpenDate() > latest.getOpenDate()) {
					latest.setOpenDate(record.getOpenDate());
				}
				if (record.getUninstallDate() > latest.getUninstallDate()) {
					latest.setUninstallDate(record.getUninstallDate());
				}
			}

			return latest;
		}

		/**
		 * Reduce phase
		 *
		 * @param key
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public  void reduce(LongWritable key, Iterable<Record> values, Context context) throws IOException, InterruptedException {
			List<Record> odsHistory = new LinkedList<Record>();
			List<Record> vsopHistory = new LinkedList<Record>();
			boolean todayExists = false;

			/* Classify different data */
			for (Iterator<Record> it = values.iterator(); it.hasNext(); ) {
				Record value = it.next();
				if (value.getFlag() == 1) {
					odsHistory.add(new Record(value.getPrdState(), value.getOpenDate(), value.getUninstallDate(), value.getModifyDate(), value.getFlag()));
				} else {
					vsopHistory.add(new Record(value.getPrdState(), value.getOpenDate(), value.getUninstallDate(), value.getModifyDate(), value.getFlag()));
					/* Detect vsop latest data */
					if (value.getFlag() == 3) {
						todayExists = true;
					}
				}
				value = null;
			}

			/* If there exists vsop latest data */
				if (todayExists) {
				Result result = new Result();
				result.setAccsNbr(MobileNbrHash.encryptLong(key.get()));
				//result.setAccsNbr(key.get());

				/* Get latest records of ods and vsop */
				Record latestOH = getLatestOds(odsHistory);
				Record latestVH = getLatestVsop(vsopHistory);

				result.setPrdState(getUserState(latestVH.getPrdState()));
				//result.setPrdState(latestVH.getPrdState());

				/* Compare latest records of ods and vsop and update its status */
				if (latestOH.getModifyDate() == -1L) {
					result.setOpenDate(latestVH.getOpenDate());
					result.setUninstallDate(latestVH.getUninstallDate());
				} else {
					if (latestVH.getOpenDate() == 0L || latestVH.getUninstallDate() == 0L) {
						result.setOpenDate(latestOH.getOpenDate());
					} else {
						result.setOpenDate(Math.max(latestOH.getOpenDate(), latestVH.getOpenDate()));
					}

					if (latestVH.getUninstallDate() != 0L) {
						result.setUninstallDate(latestVH.getUninstallDate());
					} else {
						result.setUninstallDate(latestOH.getUninstallDate());
					}
				}
				context.write(result, n);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * Set a job and its configurations
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: VsopJoinOds <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "vsop join ods");
		job.setJarByClass(VsopJoinOds.class);

		/**
		 * set the reducer class
		 */
		job.setReducerClass(ReduceClass.class);
		//job.setNumReduceTasks(127);

//		job.setPartitionerClass(KeyPartitioner.class);
//		job.setGroupingComparatorClass(GroupingComparator.class);

		/**
		 * Set output class of mapper and reducer
		 */
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(Result.class);
		job.setOutputValueClass(NullWritable.class);

		/**
		 * set the mapper class and their input files path
		 */
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, OdsHistoryMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, VsopHistoryMapper.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, VsopTodayMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
