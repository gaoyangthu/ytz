package com.github.gaoyangthu.ytz.mapreduce;

import com.github.gaoyangthu.ytz.security.hash.MobileNbrHash;
import com.github.gaoyangthu.ytz.security.tripledes.Decryptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/12/31 0031
 */
public class SelectOds {
	/* Decryptor key */
	private static final String DE_KEY = "asiainfo3Des";

	/* Any future time is less than MAX_TIME  */
	private static long MAX_TIME = 20200000000000L;

	/**
	 * Generate the user-readable state by original state
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

		public Record() {
		}

		public Record(String prdState, long openDate, long uninstallDate, long modifyDate) {
			this.prdState = prdState;
			this.openDate = openDate;
			this.uninstallDate = uninstallDate;
			this.modifyDate = modifyDate;
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

		public void set(String prdState, long openDate, long uninstallDate, long modifyDate) {
			this.prdState = prdState;
			this.openDate = openDate;
			this.uninstallDate = uninstallDate;
			this.modifyDate = modifyDate;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, prdState);
			out.writeLong(openDate);
			out.writeLong(uninstallDate);
			out.writeLong(modifyDate);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			prdState = Text.readString(in);
			openDate = in.readLong();
			uninstallDate = in.readLong();
			modifyDate = in.readLong();
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(prdState).append("\t");
			sb.append(openDate).append("\t");
			sb.append(uninstallDate).append("\t");
			sb.append(modifyDate);
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
	 * Pre-process ods history data
	 */
	public static class OdsMapper extends Mapper<Object, Text, LongWritable, Record> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] items = value.toString().split("\t", -1);
			Record record = new Record();
			try {
				String nbrStr = Decryptor.getDecryptedString(DE_KEY, items[0]);
				Long nbr = Long.parseLong(nbrStr);
				if (nbr > 13000000000L && nbr < 19000000000L) {
					record.setPrdState(items[1]);
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
					context.write(new LongWritable(nbr), record);
				}
			} catch (NumberFormatException e) {
				// if nbr is not a mobile phone number, skip this record
			}
		}
	}

	/**
	 * Implement the join process between vsop and ods
	 */
	public static class OdsReducer extends Reducer<LongWritable, Record, Result, NullWritable> {
		private static final NullWritable n = NullWritable.get();

		private Record initRecord(Record r, String s) {
			r.setPrdState(s);
			r.setOpenDate(-1L);
			r.setUninstallDate(-1L);
			r.setModifyDate(-1L);
			return r;
		}

		private long getReasonableTime(long time) {
			if (time < MAX_TIME) {
				return time;
			} else {
				return 0L;
			}
		}

		@Override
		public  void reduce(LongWritable key, Iterable<Record> values, Context context) throws IOException, InterruptedException {
			/* The latest ods record */
			Record latest = new Record();
			/* The latest in used ods record */
			Record latestInused = new Record();
			/* The latest unused ods record */
			Record latestUnused = new Record();
			/* The latest unknown ods record */
			Record latestUnknown = new Record();

			/* Initialize records above */
			initRecord(latest, "-1");
			initRecord(latestInused, "-1");
			initRecord(latestUnused, "-1");
			initRecord(latestUnknown, "-1");

			for (Iterator<Record> it = values.iterator(); it.hasNext(); ) {
				Record value = it.next();
				String state = value.getPrdState();
				long oDate = getReasonableTime(value.getOpenDate());
				long uDate = getReasonableTime(value.getUninstallDate());
				long mDate = getReasonableTime(value.getModifyDate());
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
				value = null;
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
			Result result = new Result();
			result.setAccsNbr(MobileNbrHash.encryptLong(key.get()));
			result.setPrdState(getUserState(latest.getPrdState()));
			result.setOpenDate(latest.getOpenDate());
			result.setUninstallDate(latest.getUninstallDate());
			context.write(result, n);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SelectOds <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "select ods");
		job.setJarByClass(SelectOds.class);
		job.setMapperClass(OdsMapper.class);
		job.setReducerClass(OdsReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Record.class);
		job.setOutputKeyClass(Result.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
