package avro.repo;

import org.apache.avro.repo.InMemorySchemaEntryCache;
import org.apache.avro.repo.InMemorySubjectCache;
import org.apache.avro.repo.Repository;
import org.apache.avro.repo.RepositoryUtil;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.SubjectConfig;
import org.apache.avro.repo.ValidatorFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by vchekan on 7/10/2014.
 */
public class DbRepository implements Repository {
    String jdbc;
    private ValidatorFactory validators;
    private final InMemorySubjectCache subjects = new InMemorySubjectCache();

    @Inject
    public DbRepository(@Named("avro.repo.jdbc") String jdbc, ValidatorFactory validators) {
        this.jdbc = jdbc;
        this.validators = validators;

        // eagerly load up subjects
        loadSubjects();
    }

    @Override
    public Subject register(String subjectName, SubjectConfig config) {
        Subject subject = subjects.lookup(subjectName);
        if (null == subject) {
            subject = subjects.add(Subject.validatingSubject(new DbSubject(subjectName, config), validators));
        }
        return subject;
    }

    @Override
    public Subject lookup(String subjectName) {
        return subjects.lookup(subjectName);
    }

    @Override
    public Iterable<Subject> subjects() {
        return subjects.values();
    }

    static Connection connect(String jdbc) throws ClassNotFoundException, SQLException {
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        return DriverManager.getConnection(jdbc);
    }

    private void loadSubjects() {
        try {
            Connection conn = connect(jdbc);
            try {
                // No tuples in java: use 2 lists to keep Subject and its Id
                ArrayList<DbSubject> subjects = new ArrayList<DbSubject>();
                ArrayList<Integer> subjId = new ArrayList<Integer>();
                PreparedStatement ste = conn.prepareStatement(
                        "select Topic, Id from dbo.Topic");
                ResultSet res = ste.executeQuery();
                while (res.next()) {
                    String topic = res.getString(1);
                    int id = res.getInt(2);
                    DbSubject subj = new DbSubject(topic, null);
                    subjects.add(subj);
                    subjId.add(id);
                }

                for(int i=0; i<subjects.size(); i++) {
                    DbSubject subj = subjects.get(i);
                    Integer id = subjId.get(i);
                    subj.loadSubjects(conn, id);
                    this.subjects.add(subj);
                }

            } finally {
                conn.close();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    //
    // Inner classes
    //

    class DbSubject extends Subject {
        private SubjectConfig config;
        private final InMemorySchemaEntryCache schemas = new InMemorySchemaEntryCache();
        private SchemaEntry latest = null;

        protected DbSubject(String name, SubjectConfig config) {
            super(name);
            this.config = RepositoryUtil.safeConfig(config);
        }

        @Override
        public SubjectConfig getConfig() {
            return config;
        }

        @Override
        public boolean integralKeys() {
            return true;
        }

        @Override
        public SchemaEntry register(String schema) throws SchemaValidationException {
            RepositoryUtil.validateSchemaOrSubject(schema);
            SchemaEntry entry = loadOrCreate(schema);
            return entry;
        }

        @Override
        public SchemaEntry registerIfLatest(String schema, SchemaEntry latest) throws SchemaValidationException {
            if (latest == this.latest || (latest != null && latest.equals(this.latest)))
                return register(schema);
            else
                return null;
        }

        @Override
        public SchemaEntry lookupBySchema(String schema) {
            return schemas.lookupBySchema(schema);
        }

        @Override
        public SchemaEntry lookupById(String id) {
            return schemas.lookupById(id);
        }

        @Override
        public SchemaEntry latest() {
            return latest;
        }

        @Override
        public Iterable<SchemaEntry> allEntries() {
            return schemas.values();
        }

        //
        // Helper functions
        //

        SchemaEntry loadOrCreate(String schema) {
            int topicId;
            int schemaId;
            String subjectName = this.getName();
            Connection conn;

            //
            // get topic
            //
            try {
                conn = connect(jdbc);
                // topic is not cached
                PreparedStatement ste = conn.prepareStatement(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"+
                                "select Id from dbo.Topic where Topic=?");
                ste.setString(1, subjectName);
                ResultSet res = ste.executeQuery();
                if(res.next()) {
                    // topic is already registered, just cache it
                    int id = res.getInt(1);
                    topicId = id;
                } else {
                    // topic does not exist in db, create it
                    PreparedStatement ste2 = conn.prepareStatement(
                            "insert into dbo.Topic(Topic) values(?);\n"+
                                    "select Id from dbo.Topic where Topic=?");
                    ste2.setString(1, subjectName);
                    ste2.setString(2, subjectName);
                    ResultSet res2 = ste2.executeQuery();
                    res2.next();
                    int id = res2.getInt(1);
                    topicId = id;
                }

                //
                // get schema
                //
                String hash = makeHash(schema);
                conn = connect(jdbc);
                ste = conn.prepareStatement(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"+
                                "select Id, Hash from dbo.[Schema] where Hash=?");
                ste.setString(1, hash);
                res = ste.executeQuery();
                if(res.next()) {
                    // schema is already registered, just cache it
                    schemaId = res.getInt(1);
                    if(!hash.toLowerCase().equals(res.getString(2).toLowerCase()))
                        throw new RuntimeException("Corrupt schema: hash does not match to the one already exists");
                } else {
                    // schema does not exist in db, create it
                    PreparedStatement ste2 = conn.prepareStatement(
                            "insert into dbo.[Schema]([Schema], Hash) values(?,?);\n" +
                                    "select Id from dbo.[Schema] where Hash=?");
                    ste2.setString(1, schema.toString());
                    ste2.setString(2, hash);
                    ste2.setString(3, hash);
                    ResultSet res2 = ste2.executeQuery();
                    res2.next();
                    schemaId = res2.getInt(1);
                }

                //
                // Register this schema to this topic
                //
                ste = conn.prepareStatement(
                        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;\n"+
                                "if not exists(select * from dbo.TopicSchemaMap where TopicId=? and SchemaId=?)\n"+
                                "insert into dbo.TopicSchemaMap(TopicId, SchemaId) values(?, ?)");
                ste.setInt(1, topicId);
                ste.setInt(2, schemaId);
                ste.setInt(3, topicId);
                ste.setInt(4, schemaId);
                ste.execute();

                if(conn != null)
                    conn.close();

                if(schemaId != -1)
                    return new SchemaEntry(String.valueOf(schemaId), schema);
                else
                    throw new RuntimeException("Schema not found");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        void loadSubjects(Connection conn, Integer id) throws SQLException {
            // load schemas into subjects
            PreparedStatement ste = conn.prepareStatement("select s.[Schema], s.Hash\n" +
                    "from dbo.TopicSchemaMap m\n" +
                    "join dbo.[Schema] s on s.Id=m.SchemaId\n" +
                    "where m.TopicId=?\n" +
                    "order by s.Id");
            ste.setInt(1, id);
            ResultSet res = ste.executeQuery();
            while(res.next()) {
                String schema = res.getString(1);
                String hash = res.getString(2);
                SchemaEntry se = new SchemaEntry(hash, schema);
                this.schemas.add(se);
                latest = se;
            }
        }

        private String makeHash(String schema) throws NoSuchAlgorithmException {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buff = schema.getBytes(Charset.forName("US-ASCII"));
            byte[] hash = md.digest(buff);

            return UUID.nameUUIDFromBytes(hash).toString();
        }
    }
}
