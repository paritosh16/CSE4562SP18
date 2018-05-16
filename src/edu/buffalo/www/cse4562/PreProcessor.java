package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class PreProcessor {

	public void preprocess(HashMap<String, TableSchema> schemaMap) {
		for(TableSchema schema : schemaMap.values()) {
			findCardinality(schema);
		}
	}

	void findCardinality(TableSchema schema) {
		String path = "./data/" + schema.getTableName() + ".dat";
		String line = "";
		long count = 0L;
		int numBytesLineEnding = System.getProperty("line.separator").getBytes().length;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(path));
			while ((line = reader.readLine()) != null) {
				count += 1;

				int numBytesRead = line.getBytes().length;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		schema.setCardinality(count);
	}

	public static void main(String[] args) {

		try {
			String tableName = "LINEITEM";
			String path = "./data/" + tableName + ".dat";
			System.out.println(new String(readFromFile(path, 89, 140)));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static byte[] readFromFile(String filePath, int position, int size)
			throws IOException {

		RandomAccessFile file = new RandomAccessFile(filePath, "r");
		file.seek(position);
		byte[] bytes = new byte[size];
		file.read(bytes);
		file.close();
		return bytes;

	}
}
