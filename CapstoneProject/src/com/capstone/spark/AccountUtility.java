package com.capstone.spark;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AccountUtility {
	public static StructType accountSchema() {
        StructField accountNumber = DataTypes.createStructField("accountNumber", DataTypes.IntegerType, false);
        StructField customerId = DataTypes.createStructField("customerId", DataTypes.StringType, false);
        StructField accountType = DataTypes.createStructField("accountType", DataTypes.StringType, false);
        StructField branch = DataTypes.createStructField("branch", DataTypes.StringType, false);
        StructType accSchema=new StructType(new StructField[] {accountNumber,customerId,accountType,branch});
        return accSchema;
}

}
