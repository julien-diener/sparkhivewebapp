package org.proto.sparkhivewebapp;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import java.io.Serializable;
import java.text.ParseException;

public class MyData implements Serializable{
    String name;
    int value;

    public MyData(String name, int value){
        this.name = name;
        this.value = value;
    }

    public static Function<Row, MyData> parseRow = new Function<Row, MyData>() {
        public MyData call(Row row) throws ParseException {
            int value;
            String name;

            int i=0;
            try {
                name  = row.getString(i);
                i += 1;
                value = row.getInt(i);
            } catch (Exception e) {
                // what to do with parsing error?
                String message = e.getMessage() + " (at " + (i==0? "name" : "value") + ") - row=";
                for (int j = 0; j < row.length(); i++)
                    message += row.get(j).toString() + ",";
                throw new ParseException(message,i);
            }
            return new MyData(name,value);
        }
    };

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
