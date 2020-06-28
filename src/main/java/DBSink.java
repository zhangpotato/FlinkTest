import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

public class DBSink extends RichSinkFunction<List<Person>> {
    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ConnectDB.getConnection();
        String sql = "insert into person(name, age, date) values (?, ?, ?);";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }

        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<Person> persons, Context context) throws Exception {
        for(Person person : persons) {
            ps.setString(1, person.getName());
            ps.setInt(2, person.getAge());
            ps.setTimestamp(3, new Timestamp(person.getdate().getTime()));
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("成功写入Mysql数量：" + count.length);
    }
}
