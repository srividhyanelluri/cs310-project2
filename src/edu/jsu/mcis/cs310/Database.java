package edu.jsu.mcis.cs310;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {

    private final Connection connection;
    private PreparedStatement preStmt = null;
    private ResultSet resSet = null;

    private final int TERMID_SP22 = 1;

    /* CONSTRUCTOR */
    public Database(String username, String password, String address) {

        this.connection = openConnection(username, password, address);
    }

    /* PUBLIC METHODS */
    public String getSectionsAsJSON(int termid, String subjectid, String num) {

        String result = null;        
        try {
            String query = "SELECT s.termid, s.scheduletypeid, s.instructor, s.num, s.start, s.days, s.section, s.end, s.where, s.crn, s.subjectid FROM section AS s WHERE termid=? AND subjectid=? AND num=?";
            this.preStmt = this.connection.prepareStatement(query);
            this.preStmt.setInt(1, termid);
            this.preStmt.setString(2, subjectid);
            this.preStmt.setString(3, num);
            this.resSet = this.preStmt.executeQuery();
            result = getResultSetAsJSON(this.resSet);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;

    }

    public int register(int studentid, int termid, int crn) {
        int result = 0;

        try {
            this.preStmt = this.connection.prepareStatement("insert into registration values(?,?,?)");
            this.preStmt.setInt(1, studentid);
            this.preStmt.setInt(2, termid);
            this.preStmt.setInt(3, crn);
            result = this.preStmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(Database.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;

    }

    public int drop(int studentid, int termid, int crn) {

        int result = 0;
        try {
            this.preStmt = this.connection.prepareStatement("DELETE FROM registration WHERE studentid=? AND termid=? AND crn=?");
            this.preStmt.setInt(1, studentid);
            this.preStmt.setInt(2, termid);
            this.preStmt.setInt(3, crn);
            result = this.preStmt.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;

    }

    public int withdraw(int studentid, int termid) {

        int result = 0;
        try {
            this.preStmt = this.connection.prepareStatement("DELETE FROM registration WHERE studentid=? AND termid=?");
            this.preStmt.setInt(1, studentid);
            this.preStmt.setInt(2, termid);
            result = this.preStmt.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;

    }

    public String getScheduleAsJSON(int studentid, int termid) {

        String result = null;
        try {
            String query = "SELECT r.studentid, r.termid, s.scheduletypeid, s.instructor, s.num, s.start, s.days, s.section, s.end, s.where, s.crn, s.subjectid FROM registration AS r, section AS s, term AS t WHERE r.studentid=? AND r.termid=? AND r.crn=s.crn AND s.termid=t.id";
            this.preStmt = this.connection.prepareStatement(query);
            this.preStmt.setInt(1, studentid);
            this.preStmt.setInt(2, termid);
            this.resSet = this.preStmt.executeQuery();
            result = getResultSetAsJSON(this.resSet);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;

    }

    public int getStudentId(String username) {

        int id = 0;

        try {

            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);

            boolean hasresults = pstmt.execute();

            if (hasresults) {

                ResultSet resultset = pstmt.getResultSet();

                if (resultset.next()) {
                    id = resultset.getInt("id");
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return id;

    }

    public boolean isConnected() {

        boolean result = false;

        try {

            if (!(connection == null)) {
                result = !(connection.isClosed());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {

        String result;

        /* Create JSON Containers */
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();

        try {

            /* Get Metadata */
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();

            /* Get Keys */
            for (int i = 1; i <= columnCount; ++i) {

                keys.add(metadata.getColumnLabel(i));

            }

            /* Get ResultSet Data */
            while (resultset.next()) {

                /* Create JSON Container for New Row */
                JSONObject row = new JSONObject();

                /* Get Row Data */
                for (int i = 1; i <= columnCount; ++i) {

                    /* Get Value; Pair with Key */
                    Object value = resultset.getObject(i);
                    row.put(keys.get(i - 1), String.valueOf(value));

                }

                /* Add Row Data to Collection */
                json.add(row);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        /* Encode JSON Data and Return */
        result = JSONValue.toJSONString(json);
        return result;

    }

}
