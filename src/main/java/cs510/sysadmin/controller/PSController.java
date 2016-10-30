package cs510.sysadmin.controller;

import java.sql.Connection;

import cs510.sysadmin.service.PSService;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PSController {
	
	PSService ps = new PSService();
	Connection conn;
	
	@RequestMapping(value = "/{company}/{service}", params="command,topic,key,index,pagesize,offset")
	public ResponseBody PSService(@RequestBody String payload, @PathVariable(value = "company") String company, @PathVariable(value="service") String service, String command, String topic, String key, String index, String pagesize, String offset){
		
		int Index = Integer.parseInt(index);
		int PageSize = Integer.parseInt(pagesize);
		int offSet = Integer.parseInt(offset);
		
		PSService psService = new PSService();
		ResponseBody body = psService.start(company,command,topic,key,Index,PageSize,offSet, payload);
		
		return body;
	}
	
	@RequestMapping(value = "/{company}/{service}", params={"command", "topic","index"})
	public void PSService(@RequestBody String payload, @PathVariable(value = "company") String company, @PathVariable(value="service") String service, @RequestParam(value="command") String command, @RequestParam("topic") String topic, int index){
		
		if(command.equals("DELETE")){
			conn = ps.getConnection(company);
			ps.deleteARecord(conn, company, topic, index);
		}
			
	}
	
	@RequestMapping(value = "/{company}/{service}", params={"command", "topic","key"}, method=RequestMethod.POST)
	public void PSServicePost(@RequestBody String payload, @PathVariable(value = "company") String company, @PathVariable(value="service") String service, @RequestParam(value="command") String command, @RequestParam("topic") String topic, @RequestParam("key") String key){
		conn = ps.getConnection(company);
		
		if(command.equals("DELETE")){	
			ps.deleteARecord(conn, company, topic, key);
		}
		
		if(command.equals("CREATE")){
			ps.insertARecord(conn, company, topic, key, payload);
		}
		
		if(command.equals("UPDATE")){
			ps.updateARecord(conn, company, topic, key, payload);
		}
	}
	
	@RequestMapping(value="/{company}/{service}", params={"command","topic"})
	public void PSService(@PathVariable(value="company") String company, @PathVariable(value="service") String service, @RequestParam(value="command") String command, @RequestParam(value="topic") String topic){
		
		if(command.equals("ADDTOPIC")){
			conn = ps.getConnection(company);
			ps.addTopic(conn,topic);
		}
	}
	
	@RequestMapping(value = "/{company}/{service}", params={"command", "topic","key"}, method=RequestMethod.GET)
	public void PSServiceGet(@PathVariable(value = "company") String company, @PathVariable(value="service") String service, @RequestParam(value="command") String command, @RequestParam("topic") String topic, @RequestParam("key") String key){
		
		conn = ps.getConnection(company);
		
		if(command.equals("READ")){
			ps.read(conn,company,topic,key);
		}
	}
	
	@RequestMapping(value = "/{company}/{service}", params={"command", "topic","index"}, method=RequestMethod.GET)
	public void PSServiceGet(@PathVariable(value = "company") String company, @PathVariable(value="service") String service, @RequestParam(value="command") String command, @RequestParam("topic") String topic, @RequestParam("index") int index){
		
		conn = ps.getConnection(company);
		
		if(command.equals("READ")){
			ps.read(conn,company,topic,index);
		}
	}
		
}
