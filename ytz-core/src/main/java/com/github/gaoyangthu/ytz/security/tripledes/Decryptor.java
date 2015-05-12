package com.github.gaoyangthu.ytz.security.tripledes;

import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/5 0005
 */
public class Decryptor {
	/** Cipher algorithm */
	public static final String CIPHER_ALGORITHM = "DESede/ECB/PKCS5Padding";

	/**
	 * Get decrypted string of specified <tt>text</tt>.
	 *
	 * @param key serect key given by others
	 * @param text text to be decrypted
	 * @return decrypted string
	 */
	public static String getDecryptedString(String key, String text) {
		String result = null;
		try {
			Cipher cipher =  Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.DECRYPT_MODE, KeyGen.genKey(key));
			BASE64Decoder decoder = new BASE64Decoder();
			byte[] raw = decoder.decodeBuffer(text);
			byte[] stringBytes = cipher.doFinal(raw);
			result = new String(stringBytes, "UTF-8");
		} catch (Exception e) {
			e.getMessage();
		}
		return result;
	}
}
