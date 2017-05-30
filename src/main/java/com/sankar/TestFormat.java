package com.sankar;

import java.text.DecimalFormat;
import java.util.Locale;

public class TestFormat {

	public static void main(String[] args) {
		DecimalFormat df = new DecimalFormat("#,#,###.00");
		String result = df.format(99999999999.99);
		System.out.println(result);
		
		System.out.println(String.format(Locale.ENGLISH, "%1$,.2f", 99999999999.99));
	}
}
