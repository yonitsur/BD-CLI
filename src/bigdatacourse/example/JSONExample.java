package bigdatacourse.example;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

public class JSONExample {

	public static void main(String[] args) {
		// creating object examples
		JSONObject json		=	new JSONObject();								// initialize empty object
		json				=	new JSONObject("{'salesRank': {'Home &amp; Kitchen': 277634}, 'categories': [['Office Products','Office & School Supplies']],'related': {'also_bought': ['1594416249', '1600220991', '1600221149', 'B004XMNTX6', '0545140544', 'B00D5SWA0C', '0545301467', '1600220746', '0545017025', '0439654939', 'B00AKAZ9N8', 'B001CNPKJU', '1600221009', 'B00AKAZ7SU', '1600224598', '1594415196', 'B000NWF1EK', 'B001F2I4YQ', '0545436486', '160022458X', '0448448521', '0375872809', '1594416230', '0545469147', '0439492092', '0439549256', '0545140609'], 'also_viewed': ['1594416249', 'B004XMNTX6', '1609964241', '1600220746', '1600220991', 'B007JKP9GO', 'B00B77DDUO', 'B00D5SX8MQ', '1594418357', 'B00I5N1T72', 'B0017O3GHM', 'B0077AT8F4', 'B001TQ7TSA', 'B001BY964K', '0843172525', 'B00B7XISQ2', 'B00AKAZKKK', 'B001RS0NR4', 'B0033DAL9Q', '0545140544', 'B00164853I', '1620570475', '1594415196', 'B003F118UM', '0545301467', 'B00I3USL5U', 'B00IN5A252', '1936022052', '0439549256', 'B001CNPKJU', 'B00290FKCC', 'B00H0LBMW2', 'B003CHI370', '1604182857', 'B00164A7MA']}}");	
		
		Map<String, Long> salesRank = new HashMap<String, Long>();
		JSONObject json0	=	json.getJSONObject("salesRank");
		for (String key : json0.keySet()) {
			salesRank.put(key, json0.getLong(key));
		}
		System.out.println(" --- salesRank: " +salesRank);
		
		Map<String, List<String>> related = new HashMap<String, List<String>>();
		JSONObject json1	=	json.getJSONObject("related");
		JSONArray json2		=	json1.getJSONArray("also_bought");
		JSONArray json3		=	json1.getJSONArray("also_viewed");
		for (int i = 0; i < json2.length(); i++) {
			if (related.containsKey("also_bought")) {
				related.get("also_bought").add(json2.getString(i));
			}
			else {
				related.put("also_bought", new ArrayList<String>(Arrays.asList(json2.getString(i))));
			}
		}
		for (int i = 0; i < json3.length(); i++) {
			if (related.containsKey("also_viewed")) {
				related.get("also_viewed").add(json3.getString(i));
			}
			else {
				related.put("also_viewed", new ArrayList<String>(Arrays.asList(json3.getString(i))));
			}
		}
		System.out.println(" --- related: " +related);

		TreeSet<String> categories = new TreeSet<String>();
		JSONArray json4		=	json.getJSONArray("categories");
		for (int i = 0; i < json4.length(); i++) {
			JSONArray json5 = json4.getJSONArray(i);
			for (int j = 0; j < json5.length(); j++) {
				categories.add(json5.getString(j));
			}
		}
		System.out.println(" --- categories: " +categories);

		// you will find here a few examples to handle JSON.Org
		// System.out.println("you will find here a few examples to handle JSON.org");
		// TreeSet<String> tt= new TreeSet<String>(Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper"));
		// //print tt
		// System.out.println(tt);
		// List<String> aa = Arrays.asList("Notebooks & Writing Pads", "Office & School Supplies", "Office Products", "Paper");
		// //print aa
		// System.out.println(aa);
		// //print aa.get(0)
		// System.out.println(aa.get(0));
		
		// creating object examples
		// JSONObject json		=	new JSONObject();								// initialize empty object
		// json				=	new JSONObject("{\"phone\":\"05212345678\"}");	// initialize from string
		
		// // adding attributes
		// json.put("street", "Einstein");
		// json.put("number", 3);
		// json.put("city", "Tel Aviv");
		// System.out.println(json);					// prints single line
		// System.out.println(json.toString(4));		// prints "easy reading"	
		
		// // adding inner objects
		// JSONObject main = new JSONObject();
		// main.put("address", json);
		// main.put("name", "Rubi Boim");
		// System.out.println(main.toString(4));
		
		// // adding array (1)
		// JSONArray views = new JSONArray();
		// views.put(1);
		// views.put(2);
		// views.put(3);
		// main.put("views-simple", views);
		
		// // adding array (2)
		// JSONArray viewsExtend = new JSONArray();
		// viewsExtend.put(new JSONObject().put("movieName", "American Pie").put("viewPercentage", 72));
		// viewsExtend.put(new JSONObject().put("movieName", "Top Gun").put("viewPercentage", 100));
		// viewsExtend.put(new JSONObject().put("movieName", "Bad Boys").put("viewPercentage", 87));
		// main.put("views-extend", viewsExtend);
		
		// System.out.println(main);
		// System.out.println(main.toString(4));
		 
		
	}

}
