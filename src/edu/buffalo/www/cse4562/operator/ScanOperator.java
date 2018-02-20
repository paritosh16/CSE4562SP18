package edu.buffalo.www.cse4562.operator;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import edu.buffalo.www.cse4562.TableSchema;

/* Imported Libraries specific to scan*/


public class ScanOperator {
	/* Overloading the constructor for the method*/
	TableSchema tabSchema;

	public TableSchema getTabSchema() {
		return tabSchema;
	}

	public void setTabSchema(TableSchema tabSchema) {
		this.tabSchema = tabSchema;
	}

	public ScanOperator()
	{
		/* Default constructor : just assigns the memory*/
	}

	/* Overloaded Constructor*/
	public ScanOperator(String dataPath) throws IOException
	{
		try(
				Reader pathReader = Files.newBufferedReader(Paths.get(dataPath));
				/* Reading a file when there is no header specified */
				CSVParser csvParser = new CSVParser(pathReader, CSVFormat.newFormat('|'));
				)
				{
			/* Assigning the value to the Iterator and  records*/
			Iterable<CSVRecord> records = csvParser.getRecords();

				}

	}

	/* getSchema and get Alias */
}
