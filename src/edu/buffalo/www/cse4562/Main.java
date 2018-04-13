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
		int skipQueryCount = 0;
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
						//						if( skipQueryCount < 3) {
						//							System.out.println(prompt);
						//							skipQueryCount++;
						//							continue;
						//						}
						//prettyTree(resultIterator);
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
}

/*
 * CREATE TABLE MyData (age int, name varchar, date date);
 * CREATE TABLE MyData2 (age int, name varchar, date date);
 * CREATE TABLE MyData3 (age int, name varchar, date date);
 * SELECT C.*, N.name, R.name FROM MyData AS C, MyData AS R, MyData as N WHERE N.age = C.age AND N.age > 20;
 */

/*
 *
 * CREATE TABLE MyData (age double, name string, date date);
 * SELECT * from (SELECT
 * * FROM MyData)A; SELECT age, name from (SELECT * FROM MyData WHERE age <= 24)
 * WHERE name IS 'Ankit';
 *
 * Select * from MyData A, MyData B where A.age > 20 and B.age > 22;
 * select * from (select A.age as a1, A.name as a2, B.age as b1, B.name as b2 from MyData A, MyData B where A.date <> B.date and A.age > 23 and B.age > 23);
 *select Q.a1, Q.a2, Q.b2 from (select A.age as a1, A.name as a2, B.age as b1, B.name as b2 from MyData A, MyData B where A.date <> B.date and A.age > 23 and B.age > 23) Q;
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
 * select * from (select age as ad, name as nd from MyData) B, MyData A where A.name = B.nd;
 *
 */