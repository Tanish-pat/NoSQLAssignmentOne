import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import fragment.FragmentClient;

public class Driver {

    private static final int NUM_FRAGMENTS = 3;

    private static final String OUTPUT_DIR = "output";
    private static final String BASELINE_OUTPUT = OUTPUT_DIR + "/expected_output.txt";
    private static final String DISTRIBUTED_OUTPUT = OUTPUT_DIR + "/actual_output.txt";

    // DB config
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "postgres";
    private static final String DB_HOST = "localhost";
    private static final int DB_PORT = 5432;
    private static final String ADMIN_DB = "postgres"; // IMPORTANT

    private static final String SCHEMA_FILE = "src/main/resources/scripts.sql";

    public static void main(String[] args) {
        try {
            Files.createDirectories(Paths.get(OUTPUT_DIR));

            System.out.println("Dropping old fragments...");
            dropFragments();

            System.out.println("Creating fragments...");
            createFragments();

            System.out.println("=== BASELINE RUN (1 FRAGMENT) ===");
            long startTimeBaseline = System.currentTimeMillis();
            runWorkload(1, BASELINE_OUTPUT);
            long endTimeBaseline = System.currentTimeMillis();
            long durationBaseline = endTimeBaseline - startTimeBaseline;

            System.out.println("=== DISTRIBUTED RUN (" + NUM_FRAGMENTS + " FRAGMENTS) ===");
            long startTimeDist = System.currentTimeMillis();
            runWorkload(NUM_FRAGMENTS, DISTRIBUTED_OUTPUT);
            long endTimeDist = System.currentTimeMillis();
            long durationDist = endTimeDist - startTimeDist;

            System.out.println("\n\n========== PERFORMANCE SUMMARY ==========");
            System.out.printf("Baseline Execution Time    : %d ms\n", durationBaseline);
            System.out.printf("Distributed Execution Time : %d ms\n", durationDist);
            System.out.println();

            calculateAccuracy();

        } catch (Exception e) {
            System.out.println("Driver failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                System.out.println("Cleaning up fragments");
                dropFragments();
            } catch (Exception ignored) {
            }
        }
    }

    /* ================= DATABASE LIFECYCLE ================= */

    private static Connection adminConnection() throws SQLException {
        String url = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + ADMIN_DB;
        Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASS);
        conn.setAutoCommit(true);
        return conn;
    }

    private static void dropFragments() throws SQLException {
        try (Connection conn = adminConnection();
                Statement stmt = conn.createStatement()) {

            for (int i = 0; i < Driver.NUM_FRAGMENTS; i++) {
                String db = "fragment" + i;
                stmt.executeUpdate("DROP DATABASE IF EXISTS " + db);
            }
        }
    }

    private static void createFragments() throws Exception {
        for (int i = 0; i < Driver.NUM_FRAGMENTS; i++) {
            createFragment("fragment" + i);
        }
    }

    private static void createFragment(String dbName) throws Exception {

        // 1. Create DB
        try (Connection conn = adminConnection();
                Statement stmt = conn.createStatement()) {

            stmt.executeUpdate("CREATE DATABASE " + dbName);
        }

        // 2. Run schema
        String url = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + dbName;
        try (Connection conn = DriverManager.getConnection(url, DB_USER, DB_PASS)) {

            String sql = Files.readString(Paths.get(SCHEMA_FILE));
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }
        }
    }

    /* ================= WORKLOAD ================= */

    private static void runWorkload(int numFragments, String outputPath) throws Exception {

        FragmentClient client = new FragmentClient(numFragments);
        client.setupConnections();
        client.resetDatabase();

        InputStream in = Driver.class
                .getClassLoader()
                .getResourceAsStream("workload.txt");

        if (in == null) {
            throw new FileNotFoundException("workload.txt not found");
        }

        try (Scanner scanner = new Scanner(in);
                PrintWriter out = new PrintWriter(outputPath)) {

            while (scanner.hasNextLine()) {
                String[] p = scanner.nextLine().trim().split(",");
                if (p.length == 0)
                    continue;

                switch (p[0]) {
                    case "INSERT_STUDENT":
                        client.insertStudent(p[1], p[2],
                                Integer.parseInt(p[3]), p[4]);
                        break;

                    case "INSERT_GRADE":
                        client.insertGrade(p[1], p[2],
                                Integer.parseInt(p[3]));
                        break;

                    case "UPDATE_GRADE":
                        client.updateGrade(p[1], p[2],
                                Integer.parseInt(p[3]));
                        break;

                    case "DELETE_STUDENT_COURSE":
                        client.deleteStudentFromCourse(p[1], p[2]);
                        break;

                    case "READ_PROFILE":
                        out.println(client.getStudentProfile(p[1]));
                        break;

                    case "READ_SCORE":
                        out.println(client.getAvgScoreByDept());
                        break;

                    case "READ_ALL":
                        out.println(client.getAllStudentsWithMostCourses());
                        break;
                }
            }
        }

        client.closeConnections();
    }

    /* ================= METRICS ================= */

    private static void calculateAccuracy() throws IOException {

        List<String> actual = Files.readAllLines(Paths.get(Driver.DISTRIBUTED_OUTPUT));
        List<String> expected = Files.readAllLines(Paths.get(Driver.BASELINE_OUTPUT));

        int total = expected.size();
        int match = 0;

        for (int i = 0; i < Math.min(actual.size(), expected.size()); i++) {
            if (actual.get(i).trim().equals(expected.get(i).trim())) {
                match++;
            }
        }

        double acc = total == 0 ? 0 : (double) match / total * 100;
        System.out.println("\n--- FINAL METRICS ---");
        System.out.println("Total Lines : " + total);
        System.out.println("Matching   : " + match);
        System.out.println("Accuracy   : " + String.format("%.2f", acc) + "%");
        System.out.println("---------------------\n");
    }
}