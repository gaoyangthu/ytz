package com.github.gaoyangthu.ytz.security.sha1;

import java.security.MessageDigest;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class DigesterTest {
	/** Hexadecimal digitals */
	private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5',
		'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	/**
	 * Bytes to hexadecimals
	 *
	 * @param bytes bytes to be converted
	 * @return hexadecimals
	 */
	public static String bytesToHex(byte[] bytes) {
		StringBuilder buf = new StringBuilder(bytes.length * 2);
		for (int i = 0, len = bytes.length; i < len; i++) {
			buf.append(HEX_DIGITS[(bytes[i] >> 4) & 0x0f]);
			buf.append(HEX_DIGITS[bytes[i] & 0x0f]);
		}
		return buf.toString();
	}

	/**
	 * Hexadecimals to bytes
	 *
	 * @param hexString hexadecimals to be converted
	 * @return bytes
	 */
	public static byte[] hexToBytes(String hexString) {
		if (null == hexString) {
			return null;
		} else if (hexString.length() < 2) {
			return null;
		} else {
			int len = hexString.length() / 2;
			byte[] buffer = new byte[len];
			for (int i = 0; i < len; i++) {
				buffer[i] = (byte) Integer.parseInt(hexString.substring(i * 2, i * 2 + 2), 16);
			}
			return buffer;
		}
	}
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

		return bytesToHex(bytes);
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

		byte[] fromDigests = hexToBytes(hex);

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

	public static void main(String[] args) {
		String content = "datacycle=20141115&digest=abc&mobileNo=13366363712&ref=alipay";
		String digests = "10d1552ebdde76b7c439b32c8887f9d195f20744";
		System.out.println("摘要结果："+ DigesterTest.digestToString(content));
		System.out.println("比对结果："+ DigesterTest.checkDigest(content, digests));
	}
}
