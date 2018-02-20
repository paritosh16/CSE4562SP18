package edu.buffalo.www.cse4562;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import com.sun.org.apache.bcel.internal.generic.Select;



public class Main {
	// Declaring the sample data and datapath query
	public static final String dataPath = "R";
	public static final String dataQuery = "CREATE TABLE R (A int, B int, C int);";

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

		/* Calling the ScanOperator*/
		dataConfig(dataPath);
	}

	/* Function to play around with parsing the CSV
	 * Note : The CSV file comes without the header and is pipe separated
	 * */
	public static void dataConfig(String tabName) throws IOException
	{
		/* Getting the Schema*/
		TableSchema tabObj = dataObjectsMap.get(tabName);


		/* Making the path and reading from CSV*/
		String path = "./" + tabName + ".csv";
		String line = "";
		try(
				BufferedReader br = new BufferedReader(new FileReader(path));
				)
				{
			/* Assigning the value to the Iterator and printing records*/

			while((line = br.readLine())!= null)
			{
				String[] record = line.split("|");
				System.out.println(record[0] + "  " + record[1] + "  " + record[2]);
			}


				}

	}

	/* Function to perform action on create table statement */
	public static boolean dataObjects(CreateTable createStmnt)
	{
		String tabName = (createStmnt.getTable()).getName();
		List<ColumnDefinition> 	tabColumns = createStmnt.getColumnDefinitions();
		/* Adding the table object to the Array of Tables*/
		TableSchema tabObj = new TableSchema();

		tabObj.setTableName(tabName);
		tabObj.setTabColumns(tabColumns);
		if (Main.dataObjectsMap.containsKey(tabName))
		{
			return false;
		}
		/* Assigning the tab object value to the hash*/
		Main.dataObjectsMap.put(tabName, tabObj);
		return true;
	}

}
