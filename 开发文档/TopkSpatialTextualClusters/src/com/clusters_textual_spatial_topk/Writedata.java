package com.clusters_textual_spatial_topk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Writedata {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader("/Users/ptt/Desktop/yelp_academic_dataset_business.json"));
			String str = null;
			// int[] num = { 10000, 30000, 50000, 80000, 100000, 150000 };
			BufferedWriter bw = null;
			int k = 0, j = 150000;
			try {
				File file = new File("/Users/ptt/Desktop/yelp_academic_dataset_business_" + j + ".json");
				FileWriter fw = new FileWriter(file);
				bw = new BufferedWriter(fw);
				while ((str = br.readLine()) != null && k < j) {			
					if (!file.exists()) {
						file.createNewFile();
					}
					bw.write(str+"\n");
					k++;
					System.out.println(k);
				}
				//bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
