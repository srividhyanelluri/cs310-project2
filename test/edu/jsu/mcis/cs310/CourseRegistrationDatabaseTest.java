package edu.jsu.mcis.cs310;

import org.junit.*;
import static org.junit.Assert.*;
import org.json.simple.*;

public class CourseRegistrationDatabaseTest {

    private final int TERMID_SP22 = 1;

    private Database db;

    private final String s1 = "[{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"310\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"13:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"22574\",\"subjectid\":\"CS\"}]";
    private final String s2 = "[{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"308\",\"start\":\"11:15:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"12:15:00\",\"where\":\"Ayers Hall 261\",\"crn\":\"21730\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"325\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"001\",\"end\":\"10:45:00\",\"where\":\"Ayers Hall 363\",\"crn\":\"21734\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"David C Thornton\",\"num\":\"350\",\"start\":\"13:45:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"14:45:00\",\"where\":\"Ayers Hall 355\",\"crn\":\"21737\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"David C Thornton\",\"num\":\"488\",\"start\":\"11:00:00\",\"days\":\"TR\",\"section\":\"001\",\"end\":\"12:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"21741\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"310\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"13:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"22574\",\"subjectid\":\"CS\"}]";
    private final String s3 = "[{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"308\",\"start\":\"11:15:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"12:15:00\",\"where\":\"Ayers Hall 261\",\"crn\":\"21730\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"325\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"001\",\"end\":\"10:45:00\",\"where\":\"Ayers Hall 363\",\"crn\":\"21734\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"David C Thornton\",\"num\":\"350\",\"start\":\"13:45:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"14:45:00\",\"where\":\"Ayers Hall 355\",\"crn\":\"21737\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"310\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"13:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"22574\",\"subjectid\":\"CS\"}]";
    private final String s4 = "[{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"325\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"001\",\"end\":\"10:45:00\",\"where\":\"Ayers Hall 363\",\"crn\":\"21734\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"David C Thornton\",\"num\":\"488\",\"start\":\"11:00:00\",\"days\":\"TR\",\"section\":\"001\",\"end\":\"12:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"21741\",\"subjectid\":\"CS\"},{\"studentid\":\"1\",\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jay W Snellen\",\"num\":\"310\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"13:30:00\",\"where\":\"Ayers Hall 259\",\"crn\":\"22574\",\"subjectid\":\"CS\"}]";
    private final String s5 = "[]";

    private final String s6 = "[{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Arup Kumar Ghosh\",\"num\":\"230\",\"start\":\"10:00:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"11:00:00\",\"where\":\"Ayers Hall 355\",\"crn\":\"21721\",\"subjectid\":\"CS\"},{\"termid\":\"1\",\"scheduletypeid\":\"ONL\",\"instructor\":\"Sajib Datta\",\"num\":\"230\",\"start\":\"00:00:00\",\"days\":\"\",\"section\":\"002\",\"end\":\"00:00:00\",\"where\":\"Online\",\"crn\":\"21722\",\"subjectid\":\"CS\"},{\"termid\":\"1\",\"scheduletypeid\":\"LEC\",\"instructor\":\"David C Thornton\",\"num\":\"230\",\"start\":\"07:30:00\",\"days\":\"TR\",\"section\":\"200\",\"end\":\"09:00:00\",\"where\":\"TBA\",\"crn\":\"22548\",\"subjectid\":\"CS\"}]";
    private final String s7 = "[{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Katherine Cain Johnson\",\"num\":\"201\",\"start\":\"08:45:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"09:45:00\",\"where\":\"TBA\",\"crn\":\"21701\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Don S Bennett\",\"num\":\"201\",\"start\":\"10:00:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"11:00:00\",\"where\":\"TBA\",\"crn\":\"21702\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Kimberly Ann Southwick-Thompson\",\"num\":\"201\",\"start\":\"11:15:00\",\"days\":\"MWF\",\"section\":\"003\",\"end\":\"12:15:00\",\"where\":\"TBA\",\"crn\":\"21703\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Kimberly Ann Southwick-Thompson\",\"num\":\"201\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"006\",\"end\":\"13:30:00\",\"where\":\"TBA\",\"crn\":\"21706\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"LEC\",\"instructor\":\"Joshua Blake Duckworth\",\"num\":\"201\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"007\",\"end\":\"10:45:00\",\"where\":\"TBA\",\"crn\":\"21707\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Susan Ashley Dean\",\"num\":\"201\",\"start\":\"11:00:00\",\"days\":\"TR\",\"section\":\"008\",\"end\":\"12:30:00\",\"where\":\"TBA\",\"crn\":\"21708\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"April Dawn Mattox\",\"num\":\"201\",\"start\":\"12:45:00\",\"days\":\"TR\",\"section\":\"009\",\"end\":\"14:15:00\",\"where\":\"TBA\",\"crn\":\"21709\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"ONL\",\"instructor\":\"Emrys Catkin Donaldson\",\"num\":\"201\",\"start\":\"00:00:00\",\"days\":\"\",\"section\":\"011\",\"end\":\"00:00:00\",\"where\":\"Online\",\"crn\":\"21711\",\"subjectid\":\"EH\"},{\"termid\":\"1\",\"scheduletypeid\":\"LEC\",\"instructor\":\"Matthew Ryan Hill\",\"num\":\"201\",\"start\":\"14:30:00\",\"days\":\"TR\",\"section\":\"010\",\"end\":\"16:00:00\",\"where\":\"TBA\",\"crn\":\"22857\",\"subjectid\":\"EH\"}]";
    private final String s8 = "[{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Heather Black McDivitt\",\"num\":\"112\",\"start\":\"08:45:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"09:45:00\",\"where\":\"Ayers Hall 118\",\"crn\":\"22199\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"LEC\",\"instructor\":\"Taffy J Cruse\",\"num\":\"112\",\"start\":\"00:00:00\",\"days\":\"TBA\",\"section\":\"220\",\"end\":\"00:00:00\",\"where\":\"Oxford High School\",\"crn\":\"23118\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Marcus L Shell\",\"num\":\"112\",\"start\":\"10:00:00\",\"days\":\"MWF\",\"section\":\"002\",\"end\":\"11:00:00\",\"where\":\"Ayers Hall 114\",\"crn\":\"24261\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Marcus L Shell\",\"num\":\"112\",\"start\":\"11:15:00\",\"days\":\"MWF\",\"section\":\"003\",\"end\":\"12:15:00\",\"where\":\"Ayers Hall 114\",\"crn\":\"24262\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jordan Fisher Kittrell\",\"num\":\"112\",\"start\":\"12:30:00\",\"days\":\"MWF\",\"section\":\"004\",\"end\":\"13:30:00\",\"where\":\"Ayers Hall 218\",\"crn\":\"24263\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Clint Adam Stanley\",\"num\":\"112\",\"start\":\"13:45:00\",\"days\":\"MW\",\"section\":\"005\",\"end\":\"15:15:00\",\"where\":\"Ayers Hall 214\",\"crn\":\"24264\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jordan Fisher Kittrell\",\"num\":\"112\",\"start\":\"16:30:00\",\"days\":\"MW\",\"section\":\"006\",\"end\":\"18:00:00\",\"where\":\"Ayers Hall 114\",\"crn\":\"24265\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jeffrey J Dodd\",\"num\":\"112\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"007\",\"end\":\"10:45:00\",\"where\":\"Ayers Hall 214\",\"crn\":\"24266\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jessica Warren Bentley\",\"num\":\"112\",\"start\":\"09:15:00\",\"days\":\"TR\",\"section\":\"008\",\"end\":\"10:45:00\",\"where\":\"Ayers Hall 218\",\"crn\":\"24267\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jordan Fisher Kittrell\",\"num\":\"112\",\"start\":\"11:00:00\",\"days\":\"TR\",\"section\":\"009\",\"end\":\"12:30:00\",\"where\":\"Ayers Hall 214\",\"crn\":\"24268\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Jordan Fisher Kittrell\",\"num\":\"112\",\"start\":\"12:45:00\",\"days\":\"TR\",\"section\":\"010\",\"end\":\"14:15:00\",\"where\":\"Ayers Hall 214\",\"crn\":\"24269\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Clint Adam Stanley\",\"num\":\"112\",\"start\":\"12:45:00\",\"days\":\"TR\",\"section\":\"011\",\"end\":\"14:15:00\",\"where\":\"Ayers Hall 114\",\"crn\":\"24270\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"BL1\",\"instructor\":\"Thomas E Leathrum\",\"num\":\"112\",\"start\":\"14:30:00\",\"days\":\"TR\",\"section\":\"012\",\"end\":\"16:00:00\",\"where\":\"Ayers Hall 118\",\"crn\":\"24271\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"ONL\",\"instructor\":\"Heather Black McDivitt\",\"num\":\"112\",\"start\":\"00:00:00\",\"days\":\"\",\"section\":\"013\",\"end\":\"00:00:00\",\"where\":\"Online\",\"crn\":\"24272\",\"subjectid\":\"MS\"},{\"termid\":\"1\",\"scheduletypeid\":\"ONL\",\"instructor\":\"Sharon Padgett\",\"num\":\"112\",\"start\":\"00:00:00\",\"days\":\"\",\"section\":\"014\",\"end\":\"00:00:00\",\"where\":\"Online\",\"crn\":\"24273\",\"subjectid\":\"MS\"}]";
    private final String s9 = "[{\"termid\":\"1\",\"scheduletypeid\":\"LEC\",\"instructor\":\"Laura M Weinkauf\",\"num\":\"212\",\"start\":\"13:45:00\",\"days\":\"MWF\",\"section\":\"001\",\"end\":\"15:15:00\",\"where\":\"Martin Hall 219\",\"crn\":\"21228\",\"subjectid\":\"PHS\"}]";
    private final String s10 = "[]";

    @Before
    public void setUp() {

        db = new Database("cs310_p2_user", "P2!user", "localhost");

    }

    @Test
    public void testRegisterSingle() {

        JSONArray r1 = (JSONArray) JSONValue.parse(s1);

        // clear schedule
        db.withdraw(1, TERMID_SP22);

        // register for one course
        int numRecords = db.register(1, 1, 22574);

        // compare number of updated records
        assertEquals(1, numRecords);

        // compare schedule
        assertEquals(r1, (JSONArray) JSONValue.parse(db.getScheduleAsJSON(1, TERMID_SP22)));

    }

    @Test
    public void testRegisterMultiple() {
        
        JSONArray r2 = (JSONArray)JSONValue.parse(s2);
        
        int[] crn = {22574, 21730, 21734, 21741, 21737};
        
        // clear schedule
        
        db.withdraw(1, TERMID_SP22);
        
        // register for multiple courses
        
        for (int i = 0; i < crn.length; ++i) {
            
            // add next course
            
            int numRecords = db.register(1, 1, crn[i]);
            
            // compare number of updated records
            
            assertEquals(1, numRecords);
            
        }
        
        // compare schedule
        
        JSONArray t1 = (JSONArray)JSONValue.parse(db.getScheduleAsJSON(1, TERMID_SP22));
        assertEquals(5, t1.size());
        assertEquals(r2, t1);
        
    }
    
    @Test
    public void testDropSingle() {
        
        JSONArray r3 = (JSONArray)JSONValue.parse(s3);
        
        int[] crn = {22574, 21730, 21734, 21741, 21737};
        
        // clear schedule
        
        db.withdraw(1, TERMID_SP22);
        
        // register for multiple courses
        
        for (int i = 0; i < crn.length; ++i) {
            
            // add next course
            
            int numRecords = db.register(1, 1, crn[i]);
            
            // compare number of updated records
            
            assertEquals(1, numRecords);
            
        }
        
        // drop a single course
        
        int numRecords = db.drop(1, 1, 21741);
        
        // check number of updated records
        
        assertEquals(1, numRecords);
        
        // compare schedule
        
        JSONArray t1 = (JSONArray)JSONValue.parse(db.getScheduleAsJSON(1, TERMID_SP22));
        assertEquals(4, t1.size());
        assertEquals(r3, t1);
        
    }
    
    @Test
    public void testDropMultiple() {
        
        JSONArray r4 = (JSONArray)JSONValue.parse(s4);
        
        int[] crn = {22574, 21730, 21734, 21741, 21737};
        
        // clear schedule
        
        db.withdraw(1, TERMID_SP22);
        
        // register for multiple courses
        
        for (int i = 0; i < crn.length; ++i) {
            
            // add next course
            
            int numRecords = db.register(1, 1, crn[i]);
            
            // compare number of updated records
            
            assertEquals(1, numRecords);
            
        }
        
        // drop the first course
        
        int numRecords = db.drop(1, 1, 21737);
        
        // check number of updated records
        
        assertEquals(1, numRecords);
        
        // drop the first course
        
        numRecords = db.drop(1, 1, 21730);
        
        // check number of updated records
        
        assertEquals(1, numRecords);
        
        // compare schedule
        
        JSONArray t1 = (JSONArray)JSONValue.parse(db.getScheduleAsJSON(1, TERMID_SP22));
        assertEquals(3, t1.size());
        assertEquals(r4, t1);
        
    }
    
    @Test
    public void testWithdraw() {
        
        JSONArray r5 = (JSONArray)JSONValue.parse(s5);
        
        int[] crn = {22574, 21730, 21734, 21741, 21737};
        
        // clear schedule
        
        db.withdraw(1, TERMID_SP22);
        
        // register for multiple courses
        
        for (int i = 0; i < crn.length; ++i) {
            
            // add next course
            
            int numRecords = db.register(1, 1, crn[i]);
            
            // compare number of updated records
            
            assertEquals(1, numRecords);
            
        }
        
        // withdraw from all courses
        
        int numRecords = db.withdraw(1, 1);
        
        // check number of updated records
        
        assertEquals(5, numRecords);
        
        // compare schedule
        
        assertEquals(r5, (JSONArray)JSONValue.parse(db.getScheduleAsJSON(1, TERMID_SP22)));
        
    }
    
    @Test
    public void testGetSections() {

        JSONArray r6 = (JSONArray) JSONValue.parse(s6);
        JSONArray r7 = (JSONArray) JSONValue.parse(s7);
        JSONArray r8 = (JSONArray) JSONValue.parse(s8);
        JSONArray r9 = (JSONArray) JSONValue.parse(s9);
        JSONArray r10 = (JSONArray) JSONValue.parse(s10);

        // get sections; compare number of registered sections and JSON data
        // CS 230
        JSONArray t1 = (JSONArray) JSONValue.parse(db.getSectionsAsJSON(TERMID_SP22, "CS", "230"));
        assertEquals(3, t1.size());
        assertEquals(r6, t1);

        // EH 201
        JSONArray t2 = (JSONArray) JSONValue.parse(db.getSectionsAsJSON(TERMID_SP22, "EH", "201"));
        assertEquals(9, t2.size());
        assertEquals(r7, t2);

        // MS 112
        JSONArray t3 = (JSONArray) JSONValue.parse(db.getSectionsAsJSON(TERMID_SP22, "MS", "112"));
        assertEquals(15, t3.size());
        assertEquals(r8, t3);

        // PHS 212
        JSONArray t4 = (JSONArray) JSONValue.parse(db.getSectionsAsJSON(TERMID_SP22, "PHS", "212"));
        assertEquals(1, t4.size());
        assertEquals(r9, t4);

        // PHS 211 (should be empty)
        JSONArray t5 = (JSONArray) JSONValue.parse(db.getSectionsAsJSON(TERMID_SP22, "PHS", "211"));
        assertEquals(0, t5.size());
        assertEquals(r10, t5);

    }
}
