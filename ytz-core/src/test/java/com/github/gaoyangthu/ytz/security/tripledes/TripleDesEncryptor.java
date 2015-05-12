package com.github.gaoyangthu.ytz.security.tripledes;

import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class TripleDesEncryptor {
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
			cipher.init(Cipher.ENCRYPT_MODE, TripleDesEncryptor.genKey(key));
			byte[] inputBytes = text.getBytes("UTF-8");
			byte[] outputBytes = cipher.doFinal(inputBytes);
			BASE64Encoder encoder = new BASE64Encoder();
			base64 = encoder.encode(outputBytes);
		} catch (Exception e) {
			base64 = e.getMessage();
		}
		return base64;
	}

	public static void main(String[] args) {
		/** The serect key given*/
		String KEY = "lixiang_3DES";

		String src = "18910987970";
		System.out.println(TripleDesEncryptor.getEncryptedString(KEY, src));
	}
}
