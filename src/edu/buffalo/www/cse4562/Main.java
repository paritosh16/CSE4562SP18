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
			System.out.println("Query Result");
			boolean success = queryProcessor.processOne(s);
			if (success) {
				BaseOperator resultIterator = queryProcessor.getRootOperator();
				while (resultIterator.hasNext()) {
					System.out.println(resultIterator.next());
				}
				System.out.flush();
			} else {
				// TODO error message handling goes here
			}

			// 	read for next query
			System.out.println(prompt);
			System.out.flush();
		}
	}
}
