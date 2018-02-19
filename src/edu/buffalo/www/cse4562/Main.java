package edu.buffalo.www.cse4562;

import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;

import edu.buffalo.www.cse4562.parser.SimpleParser;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

public class Main {
	public static void main(String[] main) throws ParseException, SQLException {
		SimpleParser parser = new SimpleParser();
		String query = "SELECT name, surname from employee where age > 35";
		Reader input = new StringReader(query);
		CCJSqlParser jSQLParser = new CCJSqlParser(input);
		Statement statement = jSQLParser.Statement();
		parser.parse(statement);
	}
}
