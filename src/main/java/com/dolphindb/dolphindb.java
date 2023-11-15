package com.dolphindb;



import com.dolphindb.jdbc.JDBCConnection;
import com.dolphindb.jdbc.JDBCStatement;
import com.xxdb.DBConnection;
import com.xxdb.data.BasicTable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class dolphindb {

    static int thread_id = 1;

    private static Connection getConnection(String url) throws SQLException {
        return getConnection(url, new Properties());
    }
    private static Connection getConnection(String url, Properties properties) throws SQLException {
        final Connection conn;

        conn = DriverManager.getConnection(url, properties);
        return conn;
    }

    public static BasicTable QueryResultSet(DBConnection connection,String line,String dbPath,String tbName) {
        String tradeDate = line.substring(36, 46);
        String securityID = line.substring(63, 69);
        BasicTable table = null;
        try {
            connection.run("dfsTable = loadTable('" + dbPath + "','" + tbName + "')");
            table = (BasicTable)connection.run(String.format("select * from dfsTable where tradedate = %s and SecurityID = '%s'",tradeDate,securityID));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static ResultSet QueryResultSetjdbc(Connection connection,String line,String dbPath,String tbName) {
        String tradeDate = line.substring(36, 46);
        String securityID = line.substring(63, 69);
        ResultSet rst = null;
        try {
            JDBCStatement stm = (JDBCStatement) connection.createStatement();
            stm.execute("dfsTable = loadTable('" + dbPath + "','" + tbName + "')");
            PreparedStatement stmt = connection.prepareStatement("select * from dfsTable where tradedate=? and SecurityID=?");
            stmt.setDate(1, Date.valueOf(tradeDate));
            stmt.setString(2,securityID);
            rst = stmt.executeQuery();
        }catch (Exception e){
            e.printStackTrace();
        }
        return rst;
    }
    private static ExecutorService exec = new ThreadPoolExecutor(80, 80, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100), r -> {
        Thread t = new Thread(r);
        t.setName(String.valueOf(thread_id));
        thread_id++;
        return t;
    });

    private static AtomicInteger at = new AtomicInteger(0);
    public static void main(String[] args) throws Exception {
        String dbPath = "dfs://SZ_TSDB_snaplevel";
        String tbName = "spots";
        Properties info = new Properties();
        info.put("user","admin");
        info.put("password","r01tme@abc456");
        info.put("enableEncryption","false");
        String url = "jdbc:dolphindb://10.0.0.5:8848";
        String sql = "select * from dfsTable where tradedate in (2023.01.09,2023.01.10,2023.01.11,2023.01.12,2023.01.13)";
        Class.forName("com.dolphindb.jdbc.Driver");
        int a = 1;
        /*DBConnection conn = new DBConnection();
        conn.connect("10.173.37.3",8848);
        conn.login("admin","123456",false);
        System.out.print("start connect");
        Connection conn = DriverManager.getConnection(url,info);
        System.out.print("connect success");*/
        DBConnection conn = new DBConnection();
        conn.connect("10.0.0.5",8848);
        conn.login("admin","r01tme@abc456",false);
        try {
            long time_star=System.currentTimeMillis();
            conn.run("dfsTable = loadTable('" + dbPath + "','" + tbName + "')");
            BasicTable table = (BasicTable)conn.run(sql);
            int count = table.rows();
            long time_end=System.currentTimeMillis();
            long sum_time=time_end-time_star;
            System.out.println(sql+"耗时"+sum_time+"毫秒");
            System.out.print(count);
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*while (a <= 80) {
            exec.submit(() -> {
                System.err.println("Worker" + at.getAndIncrement() + " start.");
                try  {
                    String filename = "/opt/dolphindb/selectTestData/" + Thread.currentThread().getName() + ".txt";
                    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filename))) {
                        String line;
                        int rowcount = 0;
                        long time_star=System.currentTimeMillis();
                        //Connection conn = DriverManager.getConnection(url,info);
                        //QueryResultSet(conn,line,dbPath,tbName);
                        DBConnection conn = new DBConnection();
                        conn.connect("10.0.0.5",8848);
                        conn.login("admin","r01tme@abc456",false);
                        while ((line = bufferedReader.readLine()) != null) {
                            BasicTable table = QueryResultSet(conn,line,dbPath,tbName);
                            rowcount = rowcount + table.rows();
                        }
                        long time_end=System.currentTimeMillis();
                        long sum_time=time_end-time_star;
                        System.out.println(Thread.currentThread().getName()+"任务耗时："+sum_time+"毫秒");
                        System.out.print(rowcount);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.err.println("Worker end.");
            });
            a++;
        }*/
    }
}
