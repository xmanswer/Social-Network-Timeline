/* Servlet for getting followers for given id
 * two tables in hbase to hold follower and following information 
 * schema is simply key = userid, value = string of list of follower ids */
package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

public class FollowerServlet extends HttpServlet {
	private Configuration hBaseConfig;
	private Connection sqlConnection;
	
	private final static String hbaseMasterIP = "172.31.17.89";
	private final static String userFollwers = "userFollowers";
	private final static String userFollowing = "userFollowing";
	private HTable hBaseTableFollower;
	private HTable hBaseTableFollowing;
	
	//mysql database name
	private final static String users = "users";
	//mysql table names
	private final static String mysqlTableName = "userProfileTable";
	
    public FollowerServlet() {
    	mySQLConnection();
    	HBaseConnection();
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        String id = request.getParameter("id");

        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();
        
        //get follower id list based on id
        String ids = getIdList(id, true); 
        //get follower name and profile list based on id list
        ArrayList<NameProfile> nameProfiles = getNameProfiles(ids);
        
        //for each name and profile, put in the follower object
        for(NameProfile nameProfile : nameProfiles) {
        	JSONObject follower = new JSONObject();
            follower.put("name", nameProfile.name);
            follower.put("profile", nameProfile.profile);
            followers.put(follower);
        }
        result.put("followers", followers);

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
    
    /* build connection with remote hbase */
    private void HBaseConnection() {
    	hBaseConfig = HBaseConfiguration.create(); 
    	hBaseConfig.set("hbase.zookeeper.quorum", hbaseMasterIP);
    	hBaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			hBaseTableFollower = new HTable(hBaseConfig, userFollwers);
			hBaseTableFollowing = new HTable(hBaseConfig, userFollowing);
		} catch(IOException e) {
			e.printStackTrace();
		}
    }
    
    /* build connection with local mysql */
    private void mySQLConnection() {
		try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
		
		try {
			// establish local mysql connection
			sqlConnection = DriverManager.getConnection("jdbc:mysql://localhost/" + users, "root", "15319project");
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
    
    /* get a String followers or following people 
     * (string separated by tab) based on userid from HBase */
    private String getIdList(String id, boolean getFollowers) {
    	Get get = new Get(Bytes.toBytes(id));
		Result getResult = null;
		
		try { //use different tables to get follower/followee
			if(getFollowers)
				getResult = hBaseTableFollower.get(get);
			else
				getResult = hBaseTableFollowing.get(get);
		} catch(IOException e) {
			e.printStackTrace();
		}
		if(getResult.isEmpty()) {
			return null;
		}
		byte[] column = null;
		if(getFollowers)
			column = getResult.getValue(Bytes.toBytes("followers"), Bytes.toBytes("followerID"));
		else
			column = getResult.getValue(Bytes.toBytes("following"), Bytes.toBytes("followingID"));
    	return new String(column);
    }
    
    /* get a list of Name and Profile url based on a list of user ids from MySQL */
    private ArrayList<NameProfile> getNameProfiles(String ids) {
    	Statement statement = null;
    	ArrayList<NameProfile> resultList = new ArrayList<NameProfile>();
    	String[] idList = ids.split("\t");
    	try {
    		statement = sqlConnection.createStatement();
    		for(String id : idList) { //for each id in id list, fetch the name and profile
    			String sqlQuery = "SELECT * FROM " + mysqlTableName + " WHERE userid = " + id;
    			ResultSet resultSet = statement.executeQuery(sqlQuery);
    			if(resultSet.next()) {
    				String name = resultSet.getString("name");
    				String profile = resultSet.getString("profile");
    				NameProfile nameProfile = new NameProfile(id, name, profile);
    				resultList.add(nameProfile);
    			}
    		}
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    	
    	//sort the resulted list alphabetically in ascending order 
    	Collections.sort(resultList, new compareNameProfile());
    	return resultList;
    }
    
    /* helper private nested class for storing name and profile info */
    private class NameProfile {
    	private String userid;
    	private String name;
    	private String profile;
    	
    	private NameProfile(String userid, String name, String profile) {
    		this.userid = userid;
    		this.name = name;
    		this.profile = profile;
    	}
    }
    
    /* comparator for sorting NameProfile first with name then with profile */
	private class compareNameProfile implements Comparator<NameProfile> {
		@Override
		public int compare(NameProfile first, NameProfile second) {
			if (first.name.compareTo(second.name) > 0) {
				return 1;
			} else if (first.name.compareTo(second.name) < 0) {
				return -1;
			} else {
				return first.profile.compareTo(second.profile);
			}
		}
	}
}

