package edu.buffalo.www.cse4562;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.sun.org.apache.bcel.internal.generic.Select;



public class Main {
	// Declaring the sample data and datapath query
	public static final String dataPath = "./R.csv";
	public static final String dataQuery = "CREATE TABLE Red (A int, B int, C int);";

	// Variables for binding the objects
	public static HashMap<String, TableSchema> dataObjectsMap = new HashMap<>();
	public static ArrayList<TableSchema> dataObjects = new ArrayList<TableSchema>();

	public static void main(String[] main) throws IOException {
		System.out.println("Hello, World");
		//System.out.println(System.getProperty("user.dir"));
		//dataConfig(dataPath);

		// Flow for the query statements
		Reader in = new StringReader(dataQuery);
		// Giving the input into a JSQLParser
		CCJSqlParser parser = new CCJSqlParser(in);
		try{
			Statement statement = parser.Statement();
			//System.out.println(statement);
			if (statement instanceof Select)
			{
				System.out.println("Select Logic to handle string");
			}
			else if (statement instanceof CreateTable)
			{
				System.out.println("Create Table Logic");
				CreateTable createStatement = (CreateTable)statement;
				System.out.println(createStatement);
				boolean result = dataObjects(createStatement);
				System.out.println(result);
			}
			else
			{
				System.out.println("Not a valid statement");
			}
		}
		catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/* Function to play around with parsing the CSV
	 * Note : The CSV file comes without the header and is pipe separated
	 * */
	public static void dataConfig(String dataPath) throws IOException
	{
		try(
				Reader pathReader = Files.newBufferedReader(Paths.get(dataPath));
				/* Reading a file when there is no header specified */
				CSVParser csvParser = new CSVParser(pathReader, CSVFormat.newFormat('|'));
				)
				{
			/* Assigning the value to the Iterator and printing records*/
			Iterable<CSVRecord> records = csvParser.getRecords();
			for(CSVRecord record : records)
			{
				String val1 = record.get(0);
				String val2 = record.get(1);
				System.out.println("The value 1 is :" + val1 + "Value 2 is :" +val2 );
			}
				}
	}

	/* Function to perform action on create table statement */
	public static boolean dataObjects(CreateTable createStatement)
	{
		Table tableName = createStatement.getTable();
		List<ColumnDefinition> columns =  createStatement.getColumnDefinitions();
		System.out.println(tableName);
		System.out.println(columns);
		for (ColumnDefinition column : columns )
		{
			System.out.println(column.getColumnName() + " with data type : " + column.getColDataType());

		}
		/* Adding the table object to the Array of Tables*/
		TableSchema temp = new TableSchema();
		temp.tableName = "TestName";
		dataObjects.add(temp);
		dataObjectsMap.put("TestName", dataObjects.get(dataObjects.size() - 1));
		return true;
	}

}
