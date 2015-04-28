package org.proto.sparkhivewebapp.webapp;

import org.proto.sparkhivewebapp.shared.MyData;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;


/**
 * The (spring) web API
 */
@Controller
@RequestMapping
public class Service {

    @RequestMapping(method = RequestMethod.GET, value="/gen", produces = "application/json; charset=utf-8")
    public @ResponseBody String generate(@RequestParam(required = false) String master,
                                    @RequestParam(required = false) String namenode,
                                    @RequestParam(required = false, defaultValue = "mytable") String name,
                                    @RequestParam(required = false, defaultValue = "5") int n) {
        MyTable.initSpark(master, namenode);
        MyTable table = new MyTable(name);
        table.generateTableUsingRDD(n);

        return request2jsonString(table.request());
    }


    @RequestMapping(method = RequestMethod.GET, value="/request", produces = "application/json; charset=utf-8")
    public @ResponseBody String request(@RequestParam(required = false, defaultValue = "mytable") String name) {
        return request2jsonString(new MyTable(name).request());
    }

    private String request2jsonString(List<MyData> myData){
        String output = "[";
        for(MyData data : myData)
            output += "{\"name\": \"" + data.getName()+ "\", \"value\":"+data.getValue()+"},\n";
        output = output.substring(0,output.length()-2)+"]";

        return output;
    }

}