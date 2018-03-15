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
			try {
				boolean success = queryProcessor.processOne(s);
				if (success) {
					BaseOperator resultIterator = queryProcessor.getRootOperator();
					String result;
					// resultIterator is null when there are no result rows to consume - likely a Create statement
					if (resultIterator != null) {
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
			// 	read for next query
			System.out.println(prompt);
			System.out.flush();
		}
	}
}

/*
 *
 * CREATE TABLE MyData (age int, name varchar, date date); SELECT * from (SELECT
 * * FROM MyData)A; SELECT age, name from (SELECT * FROM MyData WHERE age <= 24)
 * WHERE name IS 'Ankit';
 *
 *
 *
 * $> CREATE TABLE MyData (age int, name varchar, date date); CREATE TABLE
 * MyData (AGE INT, NAME VARCHAR, DATE DATE); CREATE TABLE R (A INT, B INT); $>
 * SELECT age, name from (SELECT * FROM MyData WHERE age <= 24) WHERE name IS
 * 'Ankit'; 22|'Ankit' $> SELECT A.age, name from (SELECT * FROM MyData WHERE
 * age <= 24) A WHERE name IS 'Ankit'; 22|'Ankit' $> SELECT age, name from
 * (SELECT * FROM MyData WHERE age <= 24) A WHERE name LIKE '%a%'; 24|'Aditya'
 * 24|'Paritosh'
 *
 * SELECT A.age, A.name from (SELECT * FROM MyData WHERE age <= 24) A WHERE name
 * LIKE '%a%'; SELECT A.AGE, A.NAME from (SELECT * FROM MyData WHERE AGE <= 24)
 * A WHERE NAME LIKE '%a%';
 *
 */