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

		String query = "SELECT `post`.`p_id` as postid, `post`.`p_heading` as heading, `thought`.`t_id` as thoughtid, `thought`.`t_user_id` as userid, `thought`.`t_full_content` as fullcontent, `thought`.`t_source_format` as sourceformat, "
				+ " `thought`.`t_post_id` as postid, `thought`.`t_downvotes_count` as downvotescount, `thought`.`t_upvotes_count` as upvotescount, "
				+ "`post`.`p_body` as body,`post`.`p_post_type` as posttype, `post`.`p_owner_id` as adminsource, `post`.`p_category` as postcategory, `post`.`p_timestamp` as timestamp, `post`.`p_countryid` as countryid, `post`.`p_news_source` as newssource,"  
				+ "`post`.`p_stateid` as stateid, `post`.`p_languageid` as languageid,`post`.`p_image_url` as thumbnailurl," 
				+ " `poststats`.`ps_view_count` viewcount, `poststats`.`ps_reactions_count` as reactioncount," 
				+ "`poststats`.`ps_thoughts_count` as thoughtscount,"
				+ "`thought`.`t_timestamp` as timestamp FROM `thought` left outer join `post` on `t_post_id`=`p_id` left outer join `poststats` on `p_id`=`ps_post_id` where `t_id` = " + thoughtid;

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
