package com.github.gaoyangthu.ytz.security.tripledes;

import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/5 0005
 */
public class Encryptor {
	/** Cipher algorithm */
	public static final String CIPHER_ALGORITHM = "DESede/ECB/PKCS5Padding";

	/**
	 * Get encrypted string of specified <tt>text</tt>.
	 *
	 * @param key serect key given by others
	 * @param text text to be encrypted
	 * @return encrypted string
	 */
	public static String getEncryptedString(String key, String text) {
		String base64 = "";
		try {
			Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.ENCRYPT_MODE, KeyGen.genKey(key));
			byte[] inputBytes = text.getBytes("UTF-8");
			byte[] outputBytes = cipher.doFinal(inputBytes);
			BASE64Encoder encoder = new BASE64Encoder();
			base64 = encoder.encode(outputBytes);
		} catch (Exception e) {
			base64 = e.getMessage();
		}
		return base64;
	}
}
