package edu.buffalo.www.cse4562;

import java.io.InputStreamReader;
import java.io.Reader;

import edu.buffalo.www.cse4562.operator.BaseOperator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

public class Main {
	static String prompt = "$> "; // expected prompt
	public static void main(String[] main) throws Exception {
		// ready to read stdin, print out prompt
		System.out.println(prompt);
		System.out.flush();

		Reader in = new InputStreamReader(System.in);
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement s;
		// project here
		SimpleQueryProcessor queryProcessor = new SimpleQueryProcessor();
		while((s = parser.Statement()) != null){
			// System.out.println("Query Result");

			boolean success = queryProcessor.processOne(s);
			if (success) {
				BaseOperator resultIterator = queryProcessor.getRootOperator();
				// resultIterator is null when there are no result rows to consume - likely a Create statement
				if (resultIterator != null) {
					while(resultIterator.hasNext()) {
						Object[] row = resultIterator.next();

						for(int i = 0; i < row.length; i++) {
							if (i == row.length -1) {
								// last row
								System.out.print(row[i]);
							} else {
								System.out.print(row[i] + "|");
							}
						}
						System.out.println();
						System.out.flush();
					}
				}
			} else {
				// TODO error message handling goes here
				System.out.println("Error: query couldnt be processed");
			}

			// 	read for next query
			System.out.println(prompt);
			System.out.flush();
		}
	}
}

/*

CREATE TABLE MyData (age int, name varchar, date date);
SELECT * from (SELECT * FROM MyData)A;
SELECT age, name from (SELECT * FROM MyData WHERE age <= 24)A WHERE name IS 'Ankit';

 */