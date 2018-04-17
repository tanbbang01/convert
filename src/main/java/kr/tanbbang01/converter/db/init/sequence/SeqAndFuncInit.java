package kr.tanbbang01.converter.db.init.sequence;

import com.google.common.base.Stopwatch;
import kr.tanbbang01.converter.util.Resourcer;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Sequence 및 Function 생성
 * Created by InSeong on 18. 4. 10..
 */
public class SeqAndFuncInit {
  private static Logger logger = LoggerFactory.getLogger(SeqAndFuncInit.class);

  private Resourcer config = null;
  private Connection destConn = null;
  private String owner = null;

  public SeqAndFuncInit(String configFile) throws SQLException {
    this.config = Resourcer.getInstance(configFile);

    BasicDataSource destBds = new BasicDataSource();
    destBds.setDriverClassName(config.getString("DEST_JDBC"));
    destBds.setUrl(config.getString("DEST_CONNECTION_STRING"));
    destBds.setUsername(config.getString("DEST_USER"));
    destBds.setPassword(config.getString("DEST_PASSWORD"));
    destBds.setMaxActive(-1);
    destBds.setMaxIdle(-1);
    destBds.setDefaultAutoCommit(true);
    destConn = destBds.getConnection();

    owner = config.getString("DEST_USER");
  }

  private void connClose() {
    try{
      destConn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }


  public void create(){
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    rebuildIndex();
    createFunction();
    createSequence();
    connClose();

    stopwatch.stop(); // optional
    logger.info("실행 시간 : {} ms", stopwatch.elapsedTime(TimeUnit.MILLISECONDS)); //실행 시\
  }

  private void rebuildIndex() {
    PreparedStatement pstmt = null;
    PreparedStatement resultPstmt = null;
    ResultSet rs = null;

    StringBuilder sb = new StringBuilder();
    //sb.append(" SELECT 'ALTER INDEX '||index_name||' REBUILD TABLESPACE solars8dbs_idx' FROM dba_indexes ") ;
    sb.append(" SELECT 'ALTER INDEX '||index_name||' REBUILD TABLESPACE USERS' FROM dba_indexes ") ;
    sb.append("  WHERE owner = ? ");
    sb.append("    AND index_type <> 'LOB' ");
    sb.append("   ORDER BY index_name ");

    String logSql = "";
    try{
      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, owner.toUpperCase());
      rs = pstmt.executeQuery();

      while (rs.next()) {
        String alterSql = rs.getString(1);
        logSql = alterSql;
        logger.info("[INDEX REBUILD] SQL : {} ", alterSql);
        resultPstmt = destConn.prepareStatement(alterSql);
        resultPstmt.executeUpdate();
        resultPstmt.close();
      }

    } catch (SQLException e) {
      logger.error("[INDEX REBUILD] Error : {}", logSql);
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try{
          pstmt.close();
        } catch (SQLException e) { }
      }
      if (resultPstmt != null) {
        try{
          resultPstmt.close();
        } catch (SQLException e) { }
      }
    }
  }

  /**
   * 변환 시 필요한 FUNCTION 이관
   */
  private void createFunction() {
    PreparedStatement pstmt = null;

    try{
      StringBuilder sb = new StringBuilder();
      /*======    FNC_LONG2CHAR   =====*/
      sb.append(" CREATE OR REPLACE FUNCTION fnc_long2char ( ");
      sb.append("   ori_rowid    ROWID, ");
      sb.append("   ori_column   VARCHAR2, ");
      sb.append("   ori_table    VARCHAR2 ");
      sb.append("   )  ");
      sb.append("   RETURN VARCHAR   ");
      sb.append("  AS ");
      sb.append("   longcont   VARCHAR2 (32767); ");
      sb.append("   sqlquery   VARCHAR2 (2000); ");
      sb.append("  BEGIN  ") ;
      sb.append("    sqlquery := 'SELECT ' || ori_column || ' FROM ' || ori_table || ' WHERE rowid = ' || CHR (39) || ori_rowid || CHR (39);");
      sb.append(" EXECUTE IMMEDIATE sqlquery INTO longcont; ");
      sb.append(" longcont := SUBSTR (longcont, 1, 1000); ");
      sb.append(" RETURN longcont; ");
      sb.append(" END fnc_long2char; ");

      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.executeUpdate();
      pstmt.clearParameters();

      StringBuilder isDateStr = new StringBuilder();
      isDateStr.append(" CREATE OR REPLACE FUNCTION is_date(v_str_date IN char, V_FORMAT IN VARCHAR2 DEFAULT 'YYYYMMDD' ) ");
      isDateStr.append(" RETURN NUMBER  ");
      isDateStr.append(" IS  ");
      isDateStr.append("     V_DATE DATE; ");
      isDateStr.append(" BEGIN ");
      isDateStr.append("   IF TRIM(v_str_date) IS NOT NULL OR LENGTH(v_str_date) > 0 THEN ");
      isDateStr.append("      V_DATE := TO_DATE(v_str_date); ");
      isDateStr.append("   ELSE ");
      isDateStr.append("      RETURN 0; ");
      isDateStr.append("   END IF; ");
      isDateStr.append(" RETURN 1; ");
      isDateStr.append(" EXCEPTION ");
      isDateStr.append("   WHEN OTHERS THEN RETURN 0; ");
      isDateStr.append(" END is_date; ");

      pstmt = destConn.prepareStatement(isDateStr.toString());
      pstmt.executeUpdate();
      pstmt.clearParameters();

      StringBuilder isNumStr = new StringBuilder();
      isNumStr.append(" CREATE OR REPLACE FUNCTION is_num (v_str_num IN VARCHAR2)  RETURN NUMBER ");
      isNumStr.append(" IS  v_num   NUMBER; ");
      isNumStr.append(" BEGIN   ");
      isNumStr.append("   IF TRIM(v_str_num) IS NOT NULL AND LENGTH(v_str_num) > 0 AND REGEXP_INSTR(v_str_num, '[^0-9]') = 0 THEN ");
      isNumStr.append("     v_num := v_str_num + 0; ");
      isNumStr.append("   ELSE ");
      isNumStr.append("     RETURN 0; ");
      isNumStr.append("   END IF; ");
      isNumStr.append(" RETURN 1;  ");
      isNumStr.append(" EXCEPTION WHEN OTHERS ");
      isNumStr.append(" THEN RETURN 0; ");
      isNumStr.append(" END  is_num; ");

      pstmt = destConn.prepareStatement(isNumStr.toString());
      pstmt.executeUpdate();
      pstmt.clearParameters();

      StringBuilder uuidStr = new StringBuilder();
      uuidStr.append(" CREATE OR REPLACE FUNCTION random_uuid return VARCHAR2 is    v_uuid VARCHAR2(40); ");
      uuidStr.append("  BEGIN  ");
      uuidStr.append("  SELECT lower(regexp_replace(rawtohex(sys_guid()), '([A-F0-9]{8})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{12})', '\\1-\\2-\\3-\\4-\\5'))");
      uuidStr.append("   INTO v_uuid    FROM dual; ");
      uuidStr.append("  RETURN v_uuid; ");
      uuidStr.append("  END random_uuid; ");

      pstmt = destConn.prepareStatement(uuidStr.toString());
      pstmt.executeUpdate();
      pstmt.clearParameters();

      StringBuilder columnStr = new StringBuilder();
      columnStr.append(" CREATE OR REPLACE FUNCTION  get_insert_table (   p_table_name    VARCHAR2 ) ");
      columnStr.append(" RETURN VARCHAR2 ");
      columnStr.append("  IS ");
      columnStr.append("    v_result   VARCHAR2 (32767); ");
      columnStr.append("    v_column_list   VARCHAR2 (32767); ");
      columnStr.append("    CURSOR curdata ");
      columnStr.append("    IS   ");
      columnStr.append("      SELECT column_name  ");
      columnStr.append("        FROM cols  ");
      columnStr.append("       WHERE table_name = p_table_name ");
      columnStr.append("       ORDER BY column_name; ");
      columnStr.append(" BEGIN  ");
      columnStr.append("      v_column_list := '';  ");
      columnStr.append("      OPEN curdata;  ");
      columnStr.append("      LOOP  ");
      columnStr.append("        FETCH curdata  ");
      columnStr.append("        INTO V_DATA ; ");
      columnStr.append("        EXIT WHEN curdata%NOTFOUND ; ");
      columnStr.append("        v_column_list := v_column_list || ',' || V_DATA; ");
      columnStr.append("      END LOOP;  ");
      columnStr.append("      CLOSE curdata; ");
      columnStr.append("  v_column_list := substr(v_column_list, 2, length(v_column_list)); ");
      columnStr.append("  v_result := 'INSERT INTO '||p_table_name|| '(' || v_column_list || ')' || ");
      columnStr.append("  ' SELECT '|| v_column_list || ' FROM ' || p_table_name ||'@inek'  ; ");
      columnStr.append("  return v_result; ");
      columnStr.append("END get_insert_table; ");

      pstmt = destConn.prepareStatement(uuidStr.toString());
      pstmt.executeUpdate();
      pstmt.close();

    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try{
          pstmt.close();
        }catch(SQLException e){        }

      }
    }
  }

  /**
   * 해당 Sequence 가 존재하는지 여부를 체크
   * @param sequenceName
   * @return
   */
  private boolean isExistsSequence(String sequenceName) {
    PreparedStatement pstmt = null;
    ResultSet rs = null;
    Boolean result = true;

    StringBuilder sb = new StringBuilder();
    sb.append("  SELECT count(1) ");
    sb.append("   FROM all_objects ");
    sb.append("   WHERE owner = ? ");
    sb.append("     AND object_type = 'SEQUENCE' ");
    sb.append("     AND object_name = ? ");

    try{
      int existsCnt = 1;

      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, owner.toUpperCase());
      pstmt.setString(2, sequenceName.toUpperCase());
      rs = pstmt.executeQuery();

      if (rs.next()) {
        existsCnt = rs.getInt(1);
      }

      if (existsCnt == 0) {
        result = false;
      }

    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try {
          pstmt.close();
        } catch(SQLException e) {  }

      }
      if (rs != null) {
        try {
          rs.close();
        } catch(SQLException e) {  }

      }
    }

    return result;
  }

  /**
   * 초기 Sequence 생성
   */
  private void createSequence() {
    PreparedStatement pstmt = null;
    ResultSet rs = null;

    StringBuilder sb = new StringBuilder();
    sb.append("  SELECT 'CREATE SEQUENCE SEQ_' ");
    sb.append("   || table_name || ' START WITH 1' || ' MAXVALUE 999999999999999999999999999 MINVALUE 1 NOCYCLE NOCACHE NOORDER '");
    sb.append("  FROM cols ");
    sb.append("  WHERE column_name = 'ID' ");
    sb.append("    AND table_name NOT IN ( SELECT table_name ");
    sb.append("                              FROM user_cons_columns ");
    sb.append("                             WHERE owner = ? ");
    sb.append("                               AND constraint_name IN (SELECT constraint_name ");
    sb.append("                                                         FROM dba_constraints ");
    sb.append("                                                         WHERE owner = ? ");
    sb.append("                                                           AND constraint_type = 'R' ) ");
    sb.append("                               AND column_name = 'ID') ");
    sb.append("   AND table_name NOT like 'COV%' ");
    sb.append(" GROUP BY table_name ");
    sb.append(" ORDER BY table_name ");

    String logSql = "";
    try {
      pstmt = destConn.prepareStatement(sb.toString());
      pstmt.setString(1, owner.toUpperCase());
      pstmt.setString(2, owner.toUpperCase());
      rs = pstmt.executeQuery();
      pstmt.clearParameters();

      while (rs.next()) {
        String seqSql = rs.getString(1);
        logSql = seqSql;
        String seqName = seqSql.split(" ")[2];
        boolean isExists = isExistsSequence(seqName);
        if (!isExists) {
          pstmt = destConn.prepareStatement(seqSql);
          pstmt.executeUpdate();
          pstmt.close();

          logger.debug("[SEQ] - [ {} ] ", seqSql);
        } else {
          logger.info("[SEQ] {} is Exists. Passing ... ", seqName);
        }
      }
    } catch (SQLException e) {
      logger.error("[SEQ] Error : [ {} ]", logSql);
      e.printStackTrace();
    } finally {
      if (pstmt != null) {
        try {
          pstmt.close();
        } catch (SQLException e) {  }
      }
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {  }
      }
    }
  }
}
