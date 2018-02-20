package edu.buffalo.www.cse4562.evaluator;

import java.util.HashMap;
import java.util.Map;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

public class evalOperator extends Eval {

	@Override
	public PrimitiveValue eval(Column col) {
		// Dummy object to test the functionality of the eval function.
		Map<String, Integer> schema = new HashMap<String, Integer>();
		Map<String, Map<String, Integer>> tableDirectory = new HashMap<String, Map<String, Integer>>();

		// Create the dummy function.
		schema.put("id", 0);
		schema.put("name", 1);
		schema.put("surname", 2);
		schema.put("gender", 3);
		schema.put("age", 4);

		// Create the global table directory with a dummy table name.
		tableDirectory.put("employee", schema);

		// Define a fake record for the purpose of this implementation.
		Object[] fakeRow = new Object[5];

		net.sf.jsqlparser.expression.DoubleValue i = new net.sf.jsqlparser.expression.DoubleValue(1234);
		net.sf.jsqlparser.expression.DoubleValue j = new net.sf.jsqlparser.expression.DoubleValue(24);
		fakeRow[0] = i;
		fakeRow[1]= "paritosh";
		fakeRow[2] = "walvekar";
		fakeRow[3] = "male";
		fakeRow[4] = j;

		//Table table = col.getTable();
		//String tableName = table.getWholeTableName();
		Map<String, Integer> currentSchema = tableDirectory.get("employee");
		Integer columnIndex = currentSchema.get(col.getColumnName());
		DoubleValue value = (DoubleValue) fakeRow[columnIndex];
		return value;
	}
}