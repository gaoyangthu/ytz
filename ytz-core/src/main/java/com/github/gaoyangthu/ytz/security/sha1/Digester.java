package com.github.gaoyangthu.ytz.security.sha1;

import java.security.MessageDigest;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class Digester {
	/**
	 * Digest a text
	 *
	 * @param content content to be digested
	 * @return digested bytes
	 */
	public static byte[] digest(String content) {
		try {
			MessageDigest digester = MessageDigest.getInstance("SHA-1");
			byte[] digest = digester.digest(content.getBytes("UTF-8"));

			return digest;
		} catch (Exception omit) {
			return null;
		}
	}

	/**
	 * Digest a text
	 *
	 * @param content content to be digested
	 * @return digested string
	 */
	public static String digestToString(String content) {
		if (null == content) {
			return null;
		}
		byte[] bytes = digest(content);
		if(null == bytes) {
			return null;
		}

		return BytesUtils.bytesToHex(bytes);
	}

	/**
	 * Check whether the digested context equals original hexadecimals
	 *
	 * @param content content to be checked
	 * @param hex hexadecimals to be checked
	 * @return sucess or false
	 */
	public static boolean checkDigest(String content, String hex) {
		byte[] digests = digest(content);

		byte[] fromDigests = BytesUtils.hexToBytes(hex);

		if (null == digests && null == fromDigests) {
			return true;
		}

		if (null == digests) {
			return false;
		}

		if (null == fromDigests) {
			return false;
		}

		if (digests.length != fromDigests.length) {
			return false;
		}

		for (int i = 0, size = digests.length; i < size; i++) {
			if (digests[i] != fromDigests[i]) {
				return false;
			}
		}

		return true;
	}
}
