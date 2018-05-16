package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class PreProcessor {

	public void preprocess(HashMap<String, TableSchema> schemaMap) {
		for(TableSchema schema : schemaMap.values()) {
			if (schema.getTableName().equals("ORDERS") || schema.getTableName().equals("SUPPLIER"))
				processTable(schema);
		}
	}

	void processTable(TableSchema schema) {
		String path = "./data/" + schema.getTableName() + ".dat";
		String line = "";

		HashMap<Object, RecordLocation> primaryKeyIndex = new HashMap<Object, RecordLocation>();
		int pkRecordIndex = schema.getPKRecordIndex();
		ColumnDefinition pkColumn = schema.getTabColumns().get(pkRecordIndex);
		String pkcolumnType = pkColumn.getColDataType().toString().split(" ")[0];

		HashMap<Object, List<RecordLocation>> foreignKeyIndex = new HashMap<Object, List<RecordLocation>>();
		int fkRecordIndex = schema.getFKRecordIndex();
		ColumnDefinition fkColumn = schema.getTabColumns().get(fkRecordIndex);
		String fkcolumnType = fkColumn.getColDataType().toString().split(" ")[0];

		long count = 0L;
		long numBytesLineEnding = System.getProperty("line.separator").getBytes().length;
		long offset = 0L;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(path));
			while ((line = reader.readLine()) != null) {
				count += 1;

				long numBytesRead = line.getBytes().length + numBytesLineEnding;
				String[] record = line.split("\\|");

				// primary index
				String pkCell = record[pkRecordIndex];
				primaryKeyIndex.put(readRecord(pkcolumnType, pkCell), new RecordLocation(offset, numBytesRead));

				// foreign key index
				String fkCell = record[fkRecordIndex];
				Object fkParsedcell = readRecord(fkcolumnType, fkCell);
				List<RecordLocation> locs = foreignKeyIndex.get(fkParsedcell);
				if (locs == null) {
					locs = new LinkedList<RecordLocation>();
					locs.add(new RecordLocation(offset, numBytesRead));
					foreignKeyIndex.put(fkParsedcell, locs);
				} else {
					locs.add(new RecordLocation(offset, numBytesRead));
					foreignKeyIndex.put(fkParsedcell, locs);
				}
				offset += numBytesRead;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		schema.setCardinality(count);
		schema.setFkIndexMap(foreignKeyIndex);
		schema.setPkIndexMap(primaryKeyIndex);
	}

	private Object readRecord(String columnType, String readCell) {
		Object cell = null;
		if (columnType.toLowerCase().equals("int")) {
			cell = new LongValue(readCell);
		} else if(columnType.toLowerCase().equals("integer")) {
			cell = new LongValue(readCell);
		} else if(columnType.toLowerCase().equals("double")) {
			cell = new DoubleValue(readCell);
		}else if (columnType.toLowerCase().equals("char")) {
			cell = new StringValue(readCell);
		} else if (columnType.toLowerCase().equals("varchar")) {
			cell = new StringValue(readCell);
		} else if (columnType.toLowerCase().equals("string")) {
			cell = new StringValue(readCell);
		} else if (columnType.toLowerCase().equals("decimal")) {
			cell = new DoubleValue(readCell);
		} else if (columnType.toLowerCase().equals("date")) {
			cell = new DateValue(readCell);
		} else {
			System.out.println("Unsupported Data type");
		}
		return cell;
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
