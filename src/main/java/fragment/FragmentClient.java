package fragment;
import java.sql.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "postgres";
    private static final String DB_HOST = "localhost";
    private static final int DB_PORT = 5432;
    private static final String DB_NAME_PREFIX = "fragment";

    public FragmentClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

    public void setupConnections() {
        try {
            for (int i = 0; i < numFragments; i++) {
                String url = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME_PREFIX + i;
                Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASSWORD);
                connectionPool.put(i, conn);
            }
            System.out.println("All fragment connections established.");
        } catch (SQLException e) {
            log.log(Level.SEVERE, "Failed to establish fragment DB connections", e);
        }
    }

    public void insertStudent(String studentId, String name, int age, String email) {
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        String sql =
            "INSERT INTO Student(student_id, name, age, email) " +
            "VALUES (?, ?, ?, ?) " ;
        try {
            PreparedStatement ps = conn.prepareStatement(sql)
            ps.setString(1, studentId);
            ps.setString(2, name);
            ps.setInt(3, age);
            ps.setString(4, email);
            ps.executeUpdate();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertGrade(String studentId, String courseId, int score) {
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        String sql =
            "INSERT INTO Grade(student_id, course_id, score) " +
            "VALUES (?, ?, ?) " +
            "ON CONFLICT (student_id, course_id) DO NOTHING"; // idempotent
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, studentId);
            ps.setString(2, courseId);
            ps.setInt(3, score);
            ps.executeUpdate();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateGrade(String studentId, String courseId, int newScore) {
        int fragmentId = router.getFragmentId(studentId);
        Connection conn = connectionPool.get(fragmentId);
        String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, newScore);
            ps.setString(2, studentId);
            ps.setString(3, courseId);
            ps.executeUpdate();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
	// Your code here:
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Fetch the student's name and email.
     */
    public String getStudentProfile(String studentId) {
        try {
            // Your code here
            return null; 
            
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Calculate the average score per department.
     */
    public String getAvgScoreByDept() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Find all the students that have taken most number of courses
     */
    public String getAllStudentsWithMostCourses() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        for (Connection conn : connectionPool.values()) {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
    }
}
