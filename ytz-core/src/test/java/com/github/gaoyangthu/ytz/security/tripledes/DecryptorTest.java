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
public class DecryptorTest {
	/** The serect key given by AsiaInfo */
	public static final String KEY = "asiainfo3Des";

	public DecryptorTest() {
		System.out.println("A new DecryptorTest instance.");
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
		/* Decrypt and check a plain text with its cipher */
		String src = "18903193260";
		String des = "9oytDznWiJfLkOQspiKRtQ==";
		Assert.assertEquals(Decryptor.getDecryptedString(KEY, des), src);

		/* Decrypt some cipher texts */
		List<String> list = new ArrayList<String>();
		list.add("8itpbLBIiUrLkOQspiKRtQ==");
		list.add("8itpbLBIiUrrztAYfOKTnA==");
		list.add("8itpbLBIiUqjPbhrpOfPXA==");
		list.add("8itpbLBIiUroEwyaz25dTQ==");
		list.add("8itpbLBIiUq6egPqgrFoqQ==");
		list.add("8itpbLBIiUqnzUPq6ZJoSA==");
		list.add("8itpbLBIiUrkiIsSUso/6w==");
		list.add("8itpbLBIiUqe0arxM81gWA==");
		list.add("8itpbLBIiUq79LotbUch0w==");
		list.add("8itpbLBIiUpbeVFIJNoylQ==");
		list.add("E3uiwJmkoBpF/Uy78GzRhg==");
		list.add("E3uiwJmkoBrDlw+u62/0BA==");
		list.add("E3uiwJmkoBo7ejHjpz9a8w==");
		for (String str : list) {
			System.out.println(Decryptor.getDecryptedString(KEY, str));
		}
	}
}
