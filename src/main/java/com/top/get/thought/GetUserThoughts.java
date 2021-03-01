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

import com.top.lib.db.connection.DBConnectionUtil;


public class GetUserThoughts {
	
	public static final Integer PAGE_SIZE = 10;
	
	public List<Map<String, Object>> getUserThoughts(Integer userid, Integer pagenumber) throws Exception {

		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(getThoughtsQuery(userid, pagenumber));
			List<Map<String, Object>> gf = extractData(stmt.getResultSet());
			return gf;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String getThoughtsQuery(Integer userid, Integer pagenumber) {
		
		Integer limit = PAGE_SIZE;
		Integer offset = (pagenumber - 1) * PAGE_SIZE;

		String query = "SELECT `post`.`p_id` as postid, `post`.`p_heading` as heading, `thought`.`t_id` as thoughtid, "
				+ " `thought`.`t_full_content` as fullcontent, `thought`.`t_source_format` as sourceformat, `thought`.`t_downvotes_count` as downvotescount, `thought`.`t_upvotes_count` as upvotescount"
				+ " FROM `post` left outer join `thought` on `t_post_id`=`p_id` WHERE `t_user_id` = " + userid;

		query += " LIMIT " + offset + ", " + limit;
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
