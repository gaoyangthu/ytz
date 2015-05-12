package com.github.gaoyangthu.ytz.hive.udf;

import com.github.gaoyangthu.ytz.security.tripledes.Encryptor;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/5 0005
 */
@Description(name = "TripleDesEncrypt",
	value = "_FUNC_(phone) - from the input string, " +
		"returns its encrypted phone",
	extended = "Example:\n:" +
		" > SELECT _FUNC_(phone) FROM table_name;")
public class TripleDesEncrypt extends UDF {
	//public static final String KEY = "asiainfo3Des";
	public static final String KEY = "lixiang_3DES";

	public String evaluate(String phone) {
		if (phone == null) {
			return null;
		} else {
			return Encryptor.getEncryptedString(KEY, phone);
		}
	}
}
