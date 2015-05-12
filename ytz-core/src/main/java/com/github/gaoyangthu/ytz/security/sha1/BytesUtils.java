package com.github.gaoyangthu.ytz.security.sha1;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/10/30 0030
 */
public class BytesUtils {
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
}
