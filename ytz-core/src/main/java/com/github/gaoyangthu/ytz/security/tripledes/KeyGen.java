package com.github.gaoyangthu.ytz.security.tripledes;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.SecureRandom;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/5 0005
 */
public class KeyGen {
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
}
