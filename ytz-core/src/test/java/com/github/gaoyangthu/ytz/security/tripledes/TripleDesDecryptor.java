package com.github.gaoyangthu.ytz.security.tripledes;

import sun.misc.BASE64Decoder;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class TripleDesDecryptor {
	/** Cipher algorithm */
	public static final String CIPHER_ALGORITHM = "DESede/ECB/PKCS5Padding";

	/** Encryption algorithm */
	public static final String KEY_ALGORITHM="DESede";

	/** Random algorithm */
	public static final String RANDOM_ALGORITHM="SHA1PRNG";

	/**
	 * Generate secret key of specified <tt>text</tt>.
	 *
	 * @param text text to be generated
	 * @return the secret key
	 * @throws Exception
	 */
	public static SecretKey genKey(String text) throws Exception {
		KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGORITHM);
		SecureRandom sr = SecureRandom.getInstance(RANDOM_ALGORITHM);
		sr.setSeed(text.getBytes());
		generator.init(sr);
		SecretKey sk = generator.generateKey();
		return sk;
	}

	/**
	 * Get decrypted string olsf specified <tt>text</tt>.
	 *
	 * @param key serect key given by others
	 * @param text text to be decrypted
	 * @return decrypted string
	 */
	public static String getDecryptedString(String key, String text) {
		String result = null;
		try {
			Cipher cipher =  Cipher.getInstance(CIPHER_ALGORITHM);
			cipher.init(Cipher.DECRYPT_MODE, TripleDesDecryptor.genKey(key));
			BASE64Decoder decoder = new BASE64Decoder();
			byte[] raw = decoder.decodeBuffer(text);
			byte[] stringBytes = cipher.doFinal(raw);
			result = new String(stringBytes, "UTF-8");
		} catch (Exception e) {
			e.getMessage();
		}
		return result;
	}

	public static void main(String[] args) {
		/** The serect key given*/
		String KEY = "lixiang_3DES";

		String src = "+++7rAfAP4M+8XMHZlJGIg==";
		System.out.println(TripleDesDecryptor.getDecryptedString(KEY, src));
	}
}
