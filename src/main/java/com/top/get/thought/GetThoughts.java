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
import java.util.concurrent.CompletableFuture;

import com.top.lib.dbconnection.DBConnectionUtil;

public class GetThoughts {
	private static final Integer PAGE_SIZE = 7;

	public List<Map<String, Object>> getThoughts(Integer postid, Integer existing, Integer required) throws Exception {

		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(getFeedQuery(postid, existing, required));
			List<Map<String, Object>> gf = extractData(stmt.getResultSet());
			asyncInvoke(gf);
			return gf;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void asyncInvoke(List<Map<String, Object>> gf) {

		CompletableFuture.runAsync(() -> addViewForSelectedPosts(gf));

	}

	private void addViewForSelectedPosts(List<Map<String, Object>> gf) {
		if (gf.size() <= 0) {
			return;
		}
		String query = getIncrementViewQuery(gf);
		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(query);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(query);
		}
	}

	private String getFeedQuery(Integer postid, Integer existing, Integer required) {
		Integer limit = PAGE_SIZE;
		if (null != required && required != 0) {
			limit = required;
		}
		Integer offset = 0;
		if (null != existing) {
			offset = existing;
		}

		String getFeed = "SELECT `thought`.`t_id` as thoughtid, `thought`.`t_user_id` as userid, `thought`.`t_full_content` as fullcontent, `thought`.`t_source_format` as sourceformat,"
				+ " `thought`.`t_post_id` as postid, `thought`.`t_downvotes_count` as downvotescount, `thought`.`t_upvotes_count` as upvotescount, "
				+ "`thought`.`t_timestamp` as timestamp FROM `thought` where `t_post_id` = "+ postid+" ORDER BY upvotescount"
				+ " LIMIT " + offset + ", " + limit;

		return getFeed;
	}

	private String getIncrementViewQuery(List<Map<String, Object>> gf) {
		String updateQuery = "UPDATE `poststats` SET  `ps_view_count` = `ps_view_count` + 1 WHERE `ps_post_id` IN (";
		for (Map<String, Object> row : gf) {
			updateQuery += row.get("pid") + ",";
		}
		return updateQuery.substring(0, updateQuery.length() - 1) + ")";

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

	public static void main(String[] args) throws Exception {
		// GetThoughts gt = new GetThoughts();
		// System.out.println(gt.getThoughts(1, 100001));
	}
}
