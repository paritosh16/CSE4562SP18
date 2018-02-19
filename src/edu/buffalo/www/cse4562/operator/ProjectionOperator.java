package edu.buffalo.www.cse4562.operator;

import java.sql.SQLException;

import edu.buffalo.www.cse4562.evaluator.evalOperator;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

/*
 * Projects the columns from the row data it receives from the select/from operators.
 */
public class ProjectionOperator {

	public static void main(String[] args) throws SQLException{
		evalOperator test = new evalOperator();
		PrimitiveValue result = test.eval(new Addition(new LongValue(1), new LongValue(3)));
		System.out.println(result);
	}
	
}
