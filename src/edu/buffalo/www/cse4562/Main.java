package edu.buffalo.www.cse4562;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

public class Main {
	/* Hash Map containing the schema */
	public static HashMap<String,TableSchema> dataObjects = new HashMap<>();
	static String prompt = "$> "; // expected prompt
	public static void main(String[] main) throws Exception {
		// ready to read stdin, print out prompt
		System.out.println(prompt);
		System.out.flush();

		Reader in = new InputStreamReader(System.in);
		CCJSqlParser parser = new CCJSqlParser(in);
		Statement s;
		// project here
		while((s = parser.Statement()) != null){
			System.out.println("Query Result");
			// 	read for next query
			System.out.println(prompt);
			System.out.flush();
		}
	}
}
