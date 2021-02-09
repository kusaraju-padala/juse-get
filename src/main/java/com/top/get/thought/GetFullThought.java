package com.top.get.thought;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.top.lib.dbconnection.DBConnectionUtil;

public class GetFullThought {

	public List<Map<String, Object>> getThought(Integer thoughtid) throws Exception {

		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(getThoughtQuery(thoughtid));
			List<Map<String, Object>> gf = extractData(stmt.getResultSet());
			return gf;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String getThoughtQuery(Integer thoughtid) {

		String query = "SELECT `thought`.`t_id` as thoughtid, `thought`.`t_user_id` as userid, `thought`.`t_full_content` as fullcontent,"
				+ " `thought`.`t_post_id` as postid, `thought`.`t_downvotes_count` as downvotescount, `thought`.`t_upvotes_count` as upvotescount, "
				+ "`thought`.`t_timestamp` as timestamp FROM `topdb`.`thought` where `t_id` = " + thoughtid;

		return query;
	}

	private List<Map<String, Object>> extractData(ResultSet rs) throws SQLException {
		List<Map<String, Object>> gf = new ArrayList<>();
		while (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Map<String, Object> mp = new HashMap<String, Object>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				mp.put(rsmd.getColumnLabel(i), rs.getObject(rsmd.getColumnLabel(i)));
			}
			gf.add(mp);
		}
		return gf;
	}

}
