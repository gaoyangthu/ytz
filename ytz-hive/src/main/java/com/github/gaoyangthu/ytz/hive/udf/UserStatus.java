package com.github.gaoyangthu.ytz.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/20 0020
 */
@Description(name = "UserStatus",
	value = "_FUNC_(status) - from the original status, " +
		"returns final status for users",
	extended = "Example:\n:" +
		" > SELECT _FUNC_(status) FROM table_name;")
public class UserStatus extends UDF {
	public String evaluate(String status) {
		/**
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
		 */
		if (status == null || status.length() == 0) {
			return "-1";
		} else {
			if (status.equals("100000") || status.equals("120000") || status.equals("130000")
				|| status.equals("140000") || status.equals("1001") || status.equals("1101")
				|| status.equals("1201") || status.equals("1203") || status.equals("1204")) {
				return "1";
			} else if (status.equals("110000") || status.equals("119999") || status.equals("1102")) {
				return "0";
			} else {
				return "-1";
			}
		}
	}
}
