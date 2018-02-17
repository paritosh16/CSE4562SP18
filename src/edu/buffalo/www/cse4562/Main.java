package edu.buffalo.www.cse4562;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;



public class Main {
	public static final String dataPath = "./R.csv";
	public static void main(String[] main) throws IOException {
		System.out.println("Hello, World");
		System.out.println(System.getProperty("user.dir"));
		dataConfig(dataPath);
	}

	/* Function to play around with parsing the CSV
	 * Note : The CSV file comes without the header and is pipe separated
	 * */
	public static void dataConfig(String dataPath) throws IOException
	{
		try(
				Reader pathReader = Files.newBufferedReader(Paths.get(dataPath));
				/* Reading a file when there is no header specified
				 * */
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
}
