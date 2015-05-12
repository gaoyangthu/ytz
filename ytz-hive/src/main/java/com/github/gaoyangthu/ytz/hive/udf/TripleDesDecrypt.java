package com.github.gaoyangthu.ytz.hive.udf;

import com.github.gaoyangthu.ytz.security.tripledes.Decryptor;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/5 0005
 */
@Description(name = "TripleDesDecrypt",
	value = "_FUNC_(code) - from the input string, " +
		"returns its decrypted code",
	extended = "Example:\n:" +
		" > SELECT _FUNC_(code) FROM table_name;")
public class TripleDesDecrypt extends UDF {
	public static final String KEY = "asiainfo3Des";

	public String evaluate(String code) {
		if (code == null) {
			return null;
		} else {
			return Decryptor.getDecryptedString(KEY, code);
		}
	}
}
