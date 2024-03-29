package com.top.get.post;

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

public class GetFeed {

	private static final Integer PAGE_SIZE = 16;
	private static final String DEFAULT_LOCALTION = "india";
	//private static final String DEFAULT_DYNAMIC = "fresh";

	public List<Map<String, Object>> getFeed(Integer pagenumber, String location, String dynamic, String topics)
			throws Exception {

		try (Connection conn = DBConnectionUtil.getConnection(); Statement stmt = conn.createStatement();) {
			stmt.execute(getFeedQuery(pagenumber, location, dynamic, topics));
			List<Map<String, Object>> gf = extractData(stmt.getResultSet());
			asyncInvoke(gf);
			return gf;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void asyncInvoke(List<Map<String, Object>> gf) {

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

	private String getFeedQuery(Integer pagenumber, String location, String dynamic, String topics) {

		Integer limit = PAGE_SIZE;
		Integer offset = (pagenumber - 1) * PAGE_SIZE;

		if (null == location || "".equalsIgnoreCase(location)) {
			location = DEFAULT_LOCALTION;
		}

		String getFeed = "SELECT `post`.`p_id` as pid, `post`.`p_heading` as heading,`post`.`p_body` as body,`post`.`p_post_type` as posttype, `post`.`p_owner_id` as adminsource, "
				+ "`post`.`p_category` as postcategory, `post`.`p_timestamp` as timestamp, `post`.`p_countryid` as countryid, "
				+ "`post`.`p_stateid` as stateid, `post`.`p_languageid` as languageid,`post`.`p_thumb_image_url` as thumbnailurl,"
				+ "`post`.`p_is_only_text` as isonlytext,  `poststats`.`ps_view_count` viewcount, `poststats`.`ps_reactions_count` as reactioncount, "
				+ "`poststats`.`ps_thoughts_count` as thoughtscount FROM `topdb`.`post` left join `topdb`.`postindex`"
				+ " on `post`.`p_id`= `postindex`.`pi_post_id`"
				+ " left outer join `topdb`.`poststats` on `poststats`.`ps_post_id`= `postindex`.`pi_post_id`"
				+ " WHERE `pi_location` IN (" + "'" + location.replace(",", "','") + "')";

		if (null != dynamic && !"".equalsIgnoreCase(dynamic)) {
			getFeed+=" and `pi_dynamic` IN (" + "'" + dynamic.replace(",", "','") + "')";
		}
		if (null != topics && !"".equalsIgnoreCase(topics)) {
			getFeed+=" and `pi_topic` IN (" + "'" + topics.replace(",", "','") + "')";
		} 
		
		getFeed+= " GROUP BY `postindex`.`pi_post_id`, `pi_timestamp` ORDER BY `pi_timestamp` DESC" + " LIMIT "
		+ offset + ", " + limit;

		return getFeed;
	}

	private String getIncrementViewQuery(List<Map<String, Object>> gf) {
		String updateQuery = "UPDATE `topdb`.`poststats` SET  `ps_view_count` = `ps_view_count` + 1 WHERE `ps_post_id` IN (";
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

	public static void main(String[] args) {
		GetFeed gf = new GetFeed();

		System.out.println(gf.getFeedQuery(1, null,null,"worldnews,entertainment,sports"));
	}

}
