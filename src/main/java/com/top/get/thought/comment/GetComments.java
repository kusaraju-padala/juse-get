package com.top.get.thought.comment;

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

public class GetComments {
	private static final Integer PAGE_SIZE = 10;

	public List<Map<String,Object>> getComments(Integer pagenumber, Integer thoughtId) throws Exception {

		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(getFeedQuery(pagenumber, thoughtId));
			List<Map<String, Object>> gf = extractData(stmt.getResultSet());
			// asyncInvoke(gf);
			return gf;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private String getFeedQuery(Integer pagenumber, Integer thoughtId) {

		Integer limit = PAGE_SIZE;
		Integer offset = (pagenumber - 1) * PAGE_SIZE;

		String getFeed = "SELECT `comment`.`c_id` as commentid, `comment`.`c_content` as content, "
				+ "`comment`.`c_user_id` as userid, `comment`.`c_post_id` as postid, `comment`.`c_thought_id` as thoughtid,"
				+ " `comment`.`c_is_parent_comment` as isparentcomment, `user`.`u_name` as username, `user`.`u_uname` as uniquename,"
				+ " `user`.`u_profile_image_thumb_url` as profileurl FROM `topdb`.`comment` "
				+ "left join `topdb`.`user` on `c_id`=`u_id` where `c_thought_id`="
				+ thoughtId + " LIMIT " + offset + ", " + limit;

		return getFeed;
	}

	private  List<Map<String,Object>> extractData(ResultSet rs) throws SQLException{
		List<Map<String,Object>> gf = new ArrayList<>(); 
		while(rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Map<String, Object> mp = new HashMap<String, Object>();
			for(int i = 1; i<=rsmd.getColumnCount(); i++) {
				mp.put(rsmd.getColumnLabel(i), rs.getObject(rsmd.getColumnLabel(i)));
			}
			gf.add(mp);
		}
		return gf;
	}
	
}
