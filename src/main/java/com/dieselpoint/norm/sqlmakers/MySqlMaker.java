package com.dieselpoint.norm.sqlmakers;

import com.dieselpoint.norm.Query;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class MySqlMaker extends StandardSqlMaker {

	@Override
	public String getUpsertSql(Query query, Object row) {
		StandardPojoInfo pojoInfo = getPojoInfo(row.getClass());
		return pojoInfo.upsertSql;
	}

	@Override
	public Object[] getUpsertArgs(Query query, Object row) {
		
		// same args as insert, but we need to duplicate the values
		Object [] args = super.getInsertArgs(query, row);
		
		int count = args.length;
		
		Object [] upsertArgs = new Object[count * 2];
		System.arraycopy(args, 0, upsertArgs, 0, count);
		System.arraycopy(args, 0, upsertArgs, count, count);
		
		return upsertArgs;
	}
	

	@Override
	public void makeUpsertSql(StandardPojoInfo pojoInfo) {

		// INSERT INTO table (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
		
		// mostly the same as the makeInsertSql code
		// it uses the same column names and argcount

		StringBuilder buf = new StringBuilder();
		buf.append(pojoInfo.insertSql);
		buf.append(" on duplicate key update ");
		
		boolean first = true;
		for (String colName: pojoInfo.insertColumnNames) {
			if (first) {
				first = false;
			} else {
				buf.append(',');
			}
			buf.append(colName);
			buf.append("=?");
		}
		
		pojoInfo.upsertSql = buf.toString();
	}

	@Override
	protected String getColType(Class<?> dataType, int length, int precision, int scale) {
		String colType;

		if (dataType.equals(Boolean.class) || dataType.equals(boolean.class)) {
			colType = "tinyint";
		} else if (dataType.equals(LocalDate.class)) {
			colType = "date";
		} else if (dataType.equals(LocalDateTime.class)) {
			colType = "datetime";
		} else {
			colType = super.getColType(dataType, length, precision, scale);
		}
		return colType;
	}

	@Override
	public Object convertValue(Object value, String columnTypeName) {
		if ("TINYINT".equalsIgnoreCase(columnTypeName)) {
			value = (int) value == 1;
		}

		if ("DATE".equalsIgnoreCase(columnTypeName)) {
			value = ((Date) value).toLocalDate();
		}

		if ("DATETIME".equalsIgnoreCase(columnTypeName)) {
			value = ((Timestamp) value).toLocalDateTime();
		}

		if ("DECIMAL".equalsIgnoreCase(columnTypeName)) {
			value = ((BigDecimal) value).doubleValue();
		}

		return value;
	}

}
