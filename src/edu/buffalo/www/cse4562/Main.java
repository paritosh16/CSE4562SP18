package edu.buffalo.www.cse4562;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;

import edu.buffalo.www.cse4562.operator.BaseOperator;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class Main {
	static String prompt = "$> "; // expected prompt
	public static void main(String[] main) throws Exception {
		System.out.println(prompt);
		System.out.flush();

		int num_create_q = 8;
		int query_counter = 0;

		Reader in = new InputStreamReader(System.in);
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement s;

		SimpleQueryProcessor queryProcessor = new SimpleQueryProcessor();
		while((s = parser.Statement()) != null){
			query_counter += 1;
			try {
				boolean success = queryProcessor.processOne(s);
				if (success) {
					BaseOperator resultIterator = queryProcessor.getRootOperator();
					String result;
					// resultIterator is null when there are no result rows to consume - likely a Create statement
					if (resultIterator != null) {
						//						if( skipQueryCount < 3) {
						//							System.out.println(prompt);
						//							skipQueryCount++;
						//							continue;
						//						}
						prettyTree(resultIterator);
						while(resultIterator.hasNext()) {
							Object[] row = resultIterator.next();
							result = "";
							for(int i = 0; i < row.length; i++) {
								if (i == row.length -1) {
									// last row
									result += row[i].toString();
								} else {
									result += (row[i].toString() + "|");
								}
							}
							System.out.println(result);
							System.out.flush();
						}
					}
				} else {
					// TODO error message handling goes here
					System.out.println("Error: query couldnt be processed");
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString() + s.toString());
				System.err.println(e.toString() + s.toString());
			}
			// 	print prompt for reading next query
			if (query_counter == num_create_q) {
				// trigger 5 minute pre-processing here
				PreProcessor pre = new PreProcessor();
				pre.preprocess(queryProcessor.getSchemaRegister());

				// print out stats and indices
				System.err.println("Cardinalities: ");
				for(TableSchema schema : queryProcessor.getSchemaRegister().values()) {
					System.err.println(schema.getTableName() + " : " + schema.getCardinality());
					//					System.err.println(schema.getPKIndexMap());
					//					System.err.println(schema.getFKIndexMap());

					if (schema.getTableName().equals("ORDERS")) {
						// Read record at PK value 1
						RecordLocation loc = schema.getPKIndexMap().get(new LongValue("1"));
						Object[] row = readFromFile(schema, loc.offset);
						String result = "";
						for(int i = 0; i < row.length; i++) {
							if (i == row.length -1) {
								// last row
								result += row[i].toString();
							} else {
								result += (row[i].toString() + "|");
							}
						}
						System.err.println(result);

						// Read record at PK value 2
						loc = schema.getPKIndexMap().get(new LongValue("2"));
						row = readFromFile(schema, loc.offset);
						result = "";
						for(int i = 0; i < row.length; i++) {
							if (i == row.length -1) {
								// last row
								result += row[i].toString();
							} else {
								result += (row[i].toString() + "|");
							}
						}
						System.err.println(result);

					}

				}
				System.out.println(prompt);
				System.out.flush();
			} else {
				System.out.println(prompt);
				System.out.flush();
			}

		}
	}

	public static void prettyTree(BaseOperator rootOperator) {
		System.out.flush();
		prettyTreeLevel(rootOperator, 0);
		System.err.flush();
	}

	private static void prettyTreeLevel(BaseOperator operatorNode, int level) {
		for (int i = 0; i < level; i++) {
			System.err.print("    ");
		}
		System.err.println(operatorNode.toString());
		if(operatorNode.getChildOperator() != null) {
			prettyTreeLevel(operatorNode.getChildOperator(), level+1);
		}
		if(operatorNode.getSecondChildOperator() != null) {
			prettyTreeLevel(operatorNode.getSecondChildOperator(), level+1);
		}
	}

	public static Object[] readFromFile(TableSchema schemaObj, long position) {
		String path = "./data/" + schemaObj.getTableName() + ".dat";
		RandomAccessFile file = null;

		try {
			file = new RandomAccessFile(path, "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		Object[] record = new Object[schemaObj.getTabColumns().size()];
		String line = null;

		try {
			file.seek(position);
			line = file.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (line != null) {
			String[] tempRecord = line.split("\\|");
			for (int i = 0; i < schemaObj.getTabColumns().size(); i++) {
				ColumnDefinition tempColumn = schemaObj.getTabColumns().get(i);

				String columnType = tempColumn.getColDataType().toString().split(" ")[0];
				if (columnType.toLowerCase().equals("int")) {
					record[i] = new LongValue(tempRecord[i]);
				} else if(columnType.toLowerCase().equals("integer")) {
					record[i] = new LongValue(tempRecord[i]);
				} else if(columnType.toLowerCase().equals("double")) {
					record[i] = new DoubleValue(tempRecord[i]);
				}else if (columnType.toLowerCase().equals("char")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("varchar")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("string")) {
					record[i] = new StringValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("decimal")) {
					record[i] = new DoubleValue(tempRecord[i]);
				} else if (columnType.toLowerCase().equals("date")) {
					record[i] = new DateValue(tempRecord[i]);
				} else {
					System.out.println("Unsupported Data type");
				}
			}
		}
		return record;
	}
}
