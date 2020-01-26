package com.dieselpoint.norm.sqlmakers;

import com.dieselpoint.norm.Query;
import com.dieselpoint.norm.Util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;

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
		ArrayList<String> cols = new ArrayList<>();
		for (Property prop: pojoInfo.propertyMap.values()) {
			if (prop.isGenerated && !prop.isPrimaryKey) {
				continue;
			}
			cols.add(prop.name);
		}

		pojoInfo.insertColumnNames = cols.toArray(new String [cols.size()]);
		pojoInfo.insertSqlArgCount = pojoInfo.insertColumnNames.length;

		StringBuilder buf = new StringBuilder();
		buf.append("insert into ");
		buf.append(pojoInfo.table);
		buf.append(" (");
		buf.append(Util.join(pojoInfo.insertColumnNames)); // comma sep list?
		buf.append(") values (");
		buf.append(Util.getQuestionMarks(pojoInfo.insertSqlArgCount));
		buf.append(")");
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
