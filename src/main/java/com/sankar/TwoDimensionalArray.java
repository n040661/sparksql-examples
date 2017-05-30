package com.sankar;

class TwoDimensionalArray {
	public static void main(String[] args) {
		String[][] salutation = { { "Mr. ", "Mrs. ", "Ms. " }, { "Kumar" } };

		int[][] multi1 = new int[5][];
		multi1[0] = new int[10];
		multi1[1] = new int[10];
		multi1[2] = new int[10];
		multi1[3] = new int[10];
		multi1[4] = new int[10];

		int[][] multi2 = new int[5][10];

		int[][] multi = new int[][] { { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
				{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
				{ 0, 0, 0, 0, 0, 1, 1, 1, 1, 0 },
				{ 0, 0, 0, 0, 0, 1, 1, 1, 1, 0 },
				{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
		
		System.out.println("MULTI LENGTH:"+multi[0].length);
		
		// one missing number 
		
		int missingNumber = printMissingNumber(new int[]{1, 2, 3, 4, 6});
		System.out.println("MISSING NUMBER IS:"+missingNumber);
		
		System.out.println("THE SUM IS:"+5*(5+1)/2);

		
		for (int i = 0; i < multi.length; i++) { // 1 2 3 4 5
			for (int j = 0; j < multi[0].length; j++) { // 1 2 3 4 5
				//System.out.println(multi[i][j]);
				if(multi[i][j] == 1) {
					System.out.println("["+i+"]["+j+"]");
				}
			}// end of for J
			System.out.println();
		}// end of for i

		int[][] twoDim = new int[5][6];

		int a = (twoDim.length);// 5
		int b = (twoDim[0].length);// 5

		System.out.println(a);
		System.out.println(b);

		for (int i = 0; i < a; i++) { // 1 2 3 4 5
			for (int j = 0; j < b; j++) { // 1 2 3 4 5
				int x = (i + 1) * (j + 1);
				twoDim[i][j] = x;
				if (x < 10) {
					System.out.print(" " + x + " ");
				} else {
					System.out.print(x + " ");
				}
			}// end of for J
			System.out.println();
		}// end of for i

		// Mr. Kumar
		System.out.println(salutation[0][0] + salutation[1][0]);
		// Mrs. Kumar
		System.out.println(salutation[0][1] + salutation[1][0]);
	}
	
	private static int printMissingNumber(int[] numbers) {
		int sum = 0;
		for (int i : numbers) {
			sum = sum+i;
		}
		return sum - numbers.length;
	}
}