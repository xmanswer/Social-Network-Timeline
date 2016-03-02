/* Servlet for identifying user login info and getting profile url
 * MySQL schema is the following
 * Table 1: userPasswordTable for storing userid and password
 * col1: userid (primary key)	col2: password 
 * Table 2: userProfileTable for storing userid, name and profile 
 * col1: userid (primary key)	col2: name	col3: profile */

package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {
	private Connection sqlConnection;
	//database name
	private final static String users = "users";
	//table names
	private final static String userPasswordTable = "userPasswordTable";
	private final static String userProfileTable = "userProfileTable";

    public ProfileServlet() {
    	/* build connection with local mysql */
    	mySQLConnection();
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");
        
        if(!correctPassword(id, pwd)) { //put unauthorized if pwd not matching
            result.put("name", "Unauthorized");
            result.put("profile", "#");
        }
        else { //get corresponed name and profile from userid
        	NameProfile nameProfile = getNameProfile(id);
        	result.put("name", nameProfile.name);
        	result.put("profile", nameProfile.profile);
        }

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
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
}
