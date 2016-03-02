package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

public class TimelineServlet extends HttpServlet {
	private Configuration hBaseConfig;
	private Connection sqlConnection;
	private static AmazonDynamoDBClient client;
	private final static String dynamoDBTableName = "postTable";
	private final static String hbaseMasterIP = "172.31.17.89";
	private final static String userFollwers = "userFollowers";
	private final static String userFollowing = "userFollowing";
	private HTable hBaseTableFollower;
	private HTable hBaseTableFollowing;
	
	//mysql database name
	private final static String users = "users";
	//mysql table names
	private final static String mysqlTableName = "userProfileTable";
	private final static String userPasswordTable = "userPasswordTable";
	private final static String userProfileTable = "userProfileTable";
	
    public TimelineServlet() {
    	connectDynamoDB();
    	mySQLConnection();
    	HBaseConnection();
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();
        JSONArray postsObject = new JSONArray();
        
        String id = request.getParameter("id");

        /* process name and profile */
        NameProfile mynameProfile = getNameProfile(id);
    	result.put("name", mynameProfile.name);
    	result.put("profile", mynameProfile.profile);
    	
    	/* process followers information */
    	String followerIds = getIdList(id, true); 
    	ArrayList<NameProfile> nameProfiles = getNameProfiles(followerIds);
        //for each name and profile, put in the follower object
        for(NameProfile nameProfile : nameProfiles) {
        	JSONObject follower = new JSONObject();
            follower.put("name", nameProfile.name);
            follower.put("profile", nameProfile.profile);
            followers.put(follower);
        }
        result.put("followers", followers);
        
        /* process following posts */
        String followingIds = getIdList(id, false); 
        List<JSONObject> postsList = getMostRecentPosts(followingIds);
        for(JSONObject post : postsList) {
        	postsObject.put(post);
        }
        result.put("posts", postsObject);

        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
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
    
    /* connect to dynamoDB */
    private void connectDynamoDB() {
    	try {
        	client = new AmazonDynamoDBClient(new EnvironmentVariableCredentialsProvider());
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }
    
    /* identify if existing such userid or if the password is correct */
    private boolean correctPassword(String id, String pwd) {
    	Statement statement = null;
    	String sqlQuery = "SELECT * FROM " + userPasswordTable + " WHERE userid = " + id;
    	String truepwd = null;
    	try {
    		statement = sqlConnection.createStatement();
    		ResultSet resultSet = statement.executeQuery(sqlQuery);
    		if(resultSet.next()) {
    			truepwd = resultSet.getString("password");
    		}
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    	
    	if(truepwd == null)
    		return false;
    	if(!truepwd.equals(pwd))
    		return false;
    	return true;
    }
    
    /* get Name and Profile url based on user id */
    private NameProfile getNameProfile(String id) {
    	Statement statement = null;
    	String sqlQuery = "SELECT * FROM " + userProfileTable + " WHERE userid = " + id;
    	String name = null;
    	String profile = null;
    	try {
    		statement = sqlConnection.createStatement();
    		ResultSet resultSet = statement.executeQuery(sqlQuery);
    		if(resultSet.next()) {
    			name = resultSet.getString("name");
    			profile = resultSet.getString("profile");
    		}
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    	    	
    	NameProfile nameProfile = new NameProfile(id, name, profile);
    	return nameProfile;
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
    
    /* return a list of post in JSONObject based on id from DynamoDB*/
    private List<JSONObject> getPosts(String id) {
    	//use mapper to query
    	DynamoDBMapper mapper = new DynamoDBMapper(client);
    	Post postKey = new Post();
    	postKey.setUid(Integer.parseInt(id)); //set user id as primary hash key
    	
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String currentDate = dateFormat.format(new Date());
    	Condition rangeKeyCondition = new Condition()
        	.withComparisonOperator(ComparisonOperator.LE.toString())
        	.withAttributeValueList(new AttributeValue(currentDate));
    	
    	DynamoDBQueryExpression<Post> queryExpression  = new DynamoDBQueryExpression<Post>()
    			.withHashKeyValues(postKey)
    			.withRangeKeyCondition("Timestamp", rangeKeyCondition);
    	
    	//get a list of Post objects parsing by DynamoDB
    	List<Post> postList = mapper.query(Post.class, queryExpression);
    	
    	List<JSONObject> postJsonList = new ArrayList<JSONObject>();
    	for(Post postObject : postList) { //convert Post object to JSON object and add to list
    		String postString = postObject.getPost();
    		JSONObject jsonObject = new JSONObject(postString);
    		postJsonList.add(jsonObject);
    	}
    	return postJsonList;
    }
    
    /* get the most recent posts from followees in asc order of time then pid */
    private List<JSONObject> getMostRecentPosts(String ids) {
    	String[] idList = ids.split("\t");
    	List<JsonTimePID> allJsonTimePIDList = new ArrayList<JsonTimePID>();
    	for(int i = 0; i < idList.length; i++) {
    		List<JSONObject> postsList = getPosts(idList[i]);
    		for(JSONObject post : postsList) {
    			String timestamp = post.getString("timestamp");
    			Long pid = post.getLong("pid");
    			JsonTimePID jsonTimePID = new JsonTimePID(timestamp, pid, post);
    			allJsonTimePIDList.add(jsonTimePID);
    		}
    	}
        Collections.sort(allJsonTimePIDList, new compareTimePID());
        List<JSONObject> mostRecentPostsList = new ArrayList<JSONObject>(30);
        for(int i = allJsonTimePIDList.size() - 30; i < allJsonTimePIDList.size(); i++) {
        	mostRecentPostsList.add(allJsonTimePIDList.get(i).json);
        }
        return mostRecentPostsList;
    }
    
    /* nested class for DynamoDBTable mapper */
    @DynamoDBTable(tableName=dynamoDBTableName)
    public static class Post {
    	private int uid;
        private String timestamp;
        private String post;

        @DynamoDBHashKey(attributeName="UserID")
        public int getUid() { return uid; }
        public void setUid(int uid) { this.uid = uid; }
        
        @DynamoDBRangeKey(attributeName="Timestamp")
        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
 
        @DynamoDBAttribute(attributeName="Post")
        public String getPost() { return post; }
        public void setPost(String post) { this.post = post; }

        @Override
        public String toString() {
            return "uid: " + uid + ", timestamp: " + timestamp + ", post: " + post;            
        }
    }
    
    /* helper nested class for storing time and pid and json object */
    private class JsonTimePID {
    	private String timestamp;
    	private Long pid;
    	private JSONObject json;
    	
    	private JsonTimePID(String timestamp, Long pid,  JSONObject json) {
    		this.timestamp = timestamp;
    		this.pid = pid;
    		this.json = json;
    	}
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
	
	/* first compare timestamp, then pid */
	private class compareTimePID implements Comparator<JsonTimePID> {
		@Override
		public int compare(JsonTimePID first, JsonTimePID second) {
			try{
			    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			    Date firstDate = dateFormat.parse(first.timestamp);
			    Date secondDate = dateFormat.parse(second.timestamp);
				
				if (firstDate.after(secondDate)) {
					return 1;
				} else if (firstDate.before(secondDate)) {
					return -1;
				} else {
					if(first.pid > second.pid) {
						return 1;
					}
					else if(first.pid < second.pid) {
						return -1;
					}
					else {
						return 0;
					}
				}
			} catch(Exception e){
				e.printStackTrace();
				return 0;
			}
		}
	}
}
