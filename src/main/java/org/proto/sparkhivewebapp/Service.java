package org.proto.sparkhivewebapp;

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

    @RequestMapping(method = RequestMethod.GET, value="/run", produces = "application/json; charset=utf-8")
    public @ResponseBody String run(@RequestParam(required = false) String master,
                                     @RequestParam(required = false) String namenode) {
        MyTable.initSpark(master, namenode);
        MyTable table = new MyTable();

        List<MyData> myData = table.request();

        String output = "[";
        for(MyData data : myData)
            output += "{\"name\": \"" + data.getName()+ "\", \"value\":"+data.getValue()+"},\n";
        output = output.substring(0,output.length()-2)+"]";

        return output;
    }
}