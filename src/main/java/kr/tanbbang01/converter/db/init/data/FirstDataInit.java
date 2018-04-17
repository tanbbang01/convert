package kr.tanbbang01.converter.db.init.data;

import com.google.common.base.Stopwatch;
import kr.tanbbang01.converter.util.Resourcer;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Created by InSeong on 18. 4. 10..
 */
public class FirstDataInit {
  private static Logger logger = LoggerFactory.getLogger(FirstDataInit.class);

  private String dbLinkName = "";
  private Resourcer config = null;
  private Connection sourceConn = null;
  private Connection destConn = null;

  public FirstDataInit(String configFile) throws SQLException {
    this.config = Resourcer.getInstance(configFile);

    BasicDataSource sourceBds = new BasicDataSource();
    sourceBds.setDriverClassName(config.getString("SOURCE_JDBC"));
    sourceBds.setUrl(config.getString("SOURCE_CONNECTION_STRING"));
    sourceBds.setUsername(config.getString("SOURCE_USER"));
    sourceBds.setPassword(config.getString("SOURCE_PASSWORD"));
    sourceBds.setMaxActive(-1);
    sourceBds.setMaxIdle(-1);
    sourceBds.setDefaultAutoCommit(true);
    sourceConn = sourceBds.getConnection();

    BasicDataSource destBds = new BasicDataSource();
    destBds.setDriverClassName(config.getString("DEST_JDBC"));
    destBds.setUrl(config.getString("DEST_CONNECTION_STRING"));
    destBds.setUsername(config.getString("DEST_USER"));
    destBds.setPassword(config.getString("DEST_PASSWORD"));
    destBds.setMaxActive(-1);
    destBds.setMaxIdle(-1);
    destBds.setDefaultAutoCommit(true);
    destConn = destBds.getConnection();

    dbLinkName = config.getString("DB_LINK_NAME");
  }

  private void connClose() {
    try{
      sourceConn.close();
      destConn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void create(){
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    insertFirstData();
    connClose();

    stopwatch.stop(); // optional
    logger.info("실행 시간 : {} ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS)); //실행 시\
  }

  /**
   * 데이터가 존재하는가 ?
   * @param tableName
   * @return
   */
  private boolean isFirstDataExists (String tableName) {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = true;
    Integer cnt = 1;

    StringBuilder sb = new StringBuilder();
    sb.append(" SELECT count(1) ");
    sb.append("   FROM " + tableName);

    try {
      pstmt = destConn.prepareStatement(sb.toString());
      rs = pstmt.executeQuery();
      if (rs.next()) {
        cnt = rs.getInt(1);
      }

      if (cnt == 0) {
        result = false;
      }

    } catch (SQLException e) {
      logger.error("[INSERT CHECK] - Exception : {}", tableName);
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try {
          pstmt.close();
        } catch (SQLException e) {   }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {   }
      }
    }
    return result;
  }

  /**
   * Default Data Insert Sql
   * @return Insert Sql
   */
  private ArrayList<String> getInsertQuery() {
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    ArrayList<String> sqlArr = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append(" SELECT get_insert_table@" + dbLinkName.toUpperCase() + " (table_name) as insertSql , table_name");
    sb.append("  FROM all_tables@" + dbLinkName.toUpperCase() );
    sb.append(" WHERE owner = 'SOLARS8_CODE' ");
    sb.append("  AND table_name not like '%COV_%' ");
    sb.append(" ORDER BY table_name ");
    String tableName = "";
    try{

      pstmt = destConn.prepareStatement(sb.toString());
      rs = pstmt.executeQuery();
      while (rs.next()) {
        tableName = rs.getString("table_name");
        if (!isFirstDataExists(tableName)) {
          logger.info("[INSERT] {} - First Data is Not Exists. INSERT add", tableName);
          sqlArr.add(rs.getString("insertSql"));
        } else {
          logger.info("[INSERT] {} - First Data Exists. Passing ... ", tableName);
        }
      }
    } catch (SQLException e) {
      logger.error("[INSERT] EXCEPTION : {}", tableName);
      e.printStackTrace();
    } finally {
      if(pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) {  }
      }
      if (rs != null) {
        try{
          rs.close();
        } catch (SQLException e) {  }
      }
    }

    return sqlArr;
  }

  /**
   * Solars8 초기 데이터
   */
  private void insertFirstData() {
    PreparedStatement pstmt = null;

    String insertSql = "";
    try{
      logger.info("==== First Data Insert Start. ====");
      ArrayList<String> sqlArr = getInsertQuery();

      for (int i = 0; i < sqlArr.size() ; i++) {
        String query = sqlArr.get(i);
        insertSql = query;
        pstmt = destConn.prepareStatement(query);

        logger.debug("[INSERT] SQL : {}", query);
        pstmt.executeUpdate();
        pstmt.clearParameters();
        pstmt.close();
      }
      logger.info("==== First Data Insert End. ====");
    } catch (SQLException e) {
      logger.error("[INSERT] Exception : {} ", insertSql);
      e.printStackTrace();
    } finally {
      if(pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) {        }
      }
    }
  }


}
