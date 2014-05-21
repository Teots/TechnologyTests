package cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Created by teots on 3/14/14.
 */
public class CassandraTester {
    public static void main(String[] args) {
        String serverIP = "localhost";
        String keyspace = "myTest";

        // Connect to the the Cassandra cluster.
        Cluster cluster = Cluster.builder().addContactPoint(serverIP).build();
        Session session = cluster.connect();

        // CassandraQueryLanguage (cql) statement to create a namespace.
        String cqlStatement = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true";
        session.execute(cqlStatement);

        session = cluster.connect(keyspace);

        cqlStatement = "CREATE TABLE IF NOT EXISTS users (" +
                " username varchar PRIMARY KEY," +
                " password varchar," +
                " age int" +
                " );";
        session.execute(cqlStatement);

        // Insert data.
        cqlStatement = "INSERT INTO " + keyspace + ".users (username, password, age) " +
                "VALUES ('Serenity', 'fa3dfQefx', 21)";
        session.execute(cqlStatement);
        cqlStatement = "INSERT INTO " + keyspace + ".users (username, password, age) " +
                "VALUES ('Karl', 'ljar0983', 19)";
        session.execute(cqlStatement);
        cqlStatement = "INSERT INTO " + keyspace + ".users (username, password, age) " +
                "VALUES ('Otto', '0ca29jf74', 24)";
        session.execute(cqlStatement);

        // Update data.
        cqlStatement = "UPDATE " + keyspace + ".users" +
                " SET password = 'zzaEcvAf32hla', age = 22" +
                " WHERE username = 'Otto';";
        session.execute(cqlStatement);

        // Delete data.
        cqlStatement = "DELETE FROM " + keyspace + ".users" +
                " WHERE username = 'Karl';";
        session.execute(cqlStatement);

        // Read data.
        cqlStatement = "SELECT * FROM users";
        for (Row row : session.execute(cqlStatement)) {
            System.out.println(row.toString());
        }

        // Free resources.
        session.close();
        cluster.close();
    }
}
