package com.github.gaoyangthu.ytz.security.tripledes;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 * Author: GaoYang
 * Date: 2014/8/6 0006
 */
public class EncryptorTest {
	/** The serect key given by AsiaInfo */
	public static final String KEY = "asiainfo3Des";

	public EncryptorTest() {
		System.out.println("A new EncryptorTest instance.");
	}

	@Before
	public void setUp() throws Exception {
		System.out.println("Call @Before before a test mechod");
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Call @After after a test mechod");
	}

	@Test
	public void doEncrypt() {
		/* Encrypt and check a cipher with its plain text */
		String src = "18903193260";
		String des = "9oytDznWiJfLkOQspiKRtQ==";
		Assert.assertEquals(Encryptor.getEncryptedString(KEY, src), des);

		/* Encrypt some plain texts */
		List<String> list = new ArrayList<String>();
		list.add("18963144219");
		list.add("13331673185");
		list.add("18914027730");
		list.add("13353260117");
		list.add("13370052053");
		list.add("18192080531");
		list.add("18066874640");
		list.add("15357963496");
		list.add("13337179174");
		for (String str : list) {
			System.out.println(Encryptor.getEncryptedString(KEY, str));
		}
	}
}
